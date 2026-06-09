"""
parquet_writer.py

Escribe pandas DataFrames (con precios int32 y timestamps int64) a archivos Parquet
de forma thread-safe. Un archivo Parquet por chunk descargado; sobreescritura atómica
via write-to-temp + rename si el archivo ya existe.

Schemas:

  Tick:
    timestamp : int64   (milisegundos desde Unix epoch UTC)
    ask       : int32   (precio × decimal_factor)
    bid       : int32   (precio × decimal_factor)
    ask_vol   : float32
    bid_vol   : float32

  OHLCV:
    timestamp : int64   (milisegundos desde Unix epoch UTC)
    open      : int32   (precio × decimal_factor)
    high      : int32   (precio × decimal_factor)
    low       : int32   (precio × decimal_factor)
    close     : int32   (precio × decimal_factor)
    volume    : float32

File-level Parquet metadata (obligatoria en cada archivo):
  decimal_factor : str  — e.g. "100000"
  symbol         : str  — e.g. "EURUSD"
  timeframe      : str  — e.g. "tick", "m15", "h1"
  source         : str  — "dukascopy"

Convención de rutas (espeja CsvWriter):
  OHLCV: {base}/{SYMBOL}/{tf_folder}/{SYMBOL}_{tf_folder}.parquet
  Tick:  {base}/{SYMBOL}/tick/{SYMBOL}_tick_{YYYY}_{MM}.parquet

Fase 2B: compresión ZSTD nivel 1 (hardcoded).
         La configurabilidad se añade en Fase 2C.

Interfaz pública idéntica a CsvWriter:
  ParquetWriter(base_path)
  .write(symbol, timeframe, df) → None
  .finalize(symbol, timeframe)  → None
  .parquet_path(symbol, timeframe, dt=None) → Path
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

# ── Timeframe → carpeta (mismo mapping que CsvWriter) ─────────────────────
_TF_FOLDER: dict[str, str] = {
    "tick": "tick",
    "m15":  "15min",
    "h1":   "1h",
    "h4":   "4h",
    "m1":   "1min",
}

# ── Timeframes que usan finalize() no-op ──────────────────────────────────
# tick y OHLCV diarios (m1, m15) tienen un archivo por chunk → no necesitan dedup.
# h1 y h4 acumulan múltiples chunks en el mismo archivo → necesitan sort+dedup.
_NOFINALIZE_TIMEFRAMES = {"tick", "m1", "m15"}

# ── Schemas pyarrow ───────────────────────────────────────────────────────
_TICK_SCHEMA = pa.schema([
    pa.field("timestamp", pa.int64()),
    pa.field("ask",       pa.int32()),
    pa.field("bid",       pa.int32()),
    pa.field("ask_vol",   pa.float32()),
    pa.field("bid_vol",   pa.float32()),
])

_OHLCV_SCHEMA = pa.schema([
    pa.field("timestamp", pa.int64()),
    pa.field("open",      pa.int32()),
    pa.field("high",      pa.int32()),
    pa.field("low",       pa.int32()),
    pa.field("close",     pa.int32()),
    pa.field("volume",    pa.float32()),
])

_COMPRESSION      = "zstd"
_COMPRESSION_LEVEL = 1


class ParquetWriter:
    """Writer Parquet thread-safe, un archivo por chunk."""

    def __init__(self, base_path: Path | str) -> None:
        self._base = Path(base_path)
        self._global_write_lock = threading.Lock()

    # ── Ruta canónica ──────────────────────────────────────────────────────

    def parquet_path(
        self,
        symbol: str,
        timeframe: str,
        dt: datetime | pd.Timestamp | None = None,
    ) -> Path:
        """Retorna la ruta Parquet canónica para un chunk dado."""
        folder = _TF_FOLDER.get(timeframe, timeframe)

        if timeframe == "tick" and dt is not None:
            ts = pd.Timestamp(dt)
            filename = f"{symbol}_{folder}_{ts.year:04d}_{ts.month:02d}.parquet"
        else:
            filename = f"{symbol}_{folder}.parquet"

        return self._base / symbol / folder / filename

    # ── Escritura atómica ─────────────────────────────────────────────────

    def write(
        self,
        symbol: str,
        timeframe: str,
        df: pd.DataFrame,
        decimal_factor: int = 1,
    ) -> None:
        """
        Escribe df como un archivo Parquet con schema y metadata correctos.

        Sobreescritura atómica via write-to-temp + rename si el archivo ya existe.
        Thread-safe.

        Parameters
        ----------
        symbol         : símbolo en mayúsculas, e.g. "EURUSD".
        timeframe      : código interno, e.g. "tick", "m15", "h1".
        df             : DataFrame con columnas int32 (precios) e int64 (timestamp).
                         Producido por bi5_decoder.decode(..., raw_prices=True).
        decimal_factor : factor de escala; se almacena en metadata del archivo.
        """
        if df.empty:
            return

        # Derivar la ruta a partir del primer timestamp del chunk
        first_ts = pd.Timestamp(df["timestamp"].iloc[0], unit="ms", tz="UTC")
        path = self.parquet_path(symbol, timeframe, first_ts)

        is_tick   = timeframe == "tick"
        schema    = _TICK_SCHEMA if is_tick else _OHLCV_SCHEMA
        file_meta = {
            b"decimal_factor": str(decimal_factor).encode(),
            b"symbol":         symbol.upper().encode(),
            b"timeframe":      timeframe.encode(),
            b"source":         b"dukascopy",
        }

        # Inyectar metadata en el schema
        schema_with_meta = schema.with_metadata(file_meta)

        table = pa.Table.from_pandas(df, schema=schema_with_meta, preserve_index=False)

        with self._global_write_lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f"{path.stem}_{uuid.uuid4().hex}.tmp.parquet")
            try:
                pq.write_table(
                    table,
                    tmp,
                    compression=_COMPRESSION,
                    compression_level=_COMPRESSION_LEVEL,
                )
                tmp.replace(path)
            except Exception:
                # Limpiar temp si la escritura falló
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
                raise

    # ── Finalización ──────────────────────────────────────────────────────

    def finalize(self, symbol: str, timeframe: str) -> None:
        """
        Para h1 y h4: lee el archivo acumulado, deduplica por timestamp,
        ordena y lo sobreescribe atómicamente con Polars.

        Para tick, m1 y m15: no-op (archivos atómicos por chunk).
        """
        if timeframe in _NOFINALIZE_TIMEFRAMES:
            return

        path = self.parquet_path(symbol, timeframe)
        if not path.exists():
            return

        with self._global_write_lock:
            try:
                tmp = path.with_name(f"{path.stem}_{uuid.uuid4().hex}.tmp.parquet")
                (
                    pl.scan_parquet(path)
                    .unique(subset=["timestamp"], keep="last")
                    .sort("timestamp")
                    .sink_parquet(
                        tmp,
                        compression=_COMPRESSION,
                        compression_level=_COMPRESSION_LEVEL,
                    )
                )
                tmp.replace(path)
            except Exception as exc:
                log.error("Error finalizando %s: %s", path, exc)
                if tmp.exists():
                    tmp.unlink(missing_ok=True)