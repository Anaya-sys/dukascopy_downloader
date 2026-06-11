"""
parquet_writer.py

Escribe pandas DataFrames (con precios int32 y timestamps int64) a archivos Parquet
de forma thread-safe.

Modelo de escritura: **un archivo por chunk + merge en finalize()**.

  write()    → escribe SIEMPRE un archivo nuevo y único por chunk dentro de un
               subdirectorio `.chunks/`. Nunca sobreescribe. Sin lock global
               (cada chunk es un fichero distinto), por lo que múltiples workers
               escriben en paralelo sin contención.

  finalize() → concatena + deduplica + ordena todos los chunks
               (más el archivo canónico previo, si existe) y produce el/los
               archivo(s) canónico(s). Borra los chunks ya fusionados.
               Implementación en PyArrow puro (sin Polars) para máxima
               compatibilidad y fiabilidad.

Esto evita el bug previo en el que parquet_path() devolvía el mismo nombre para
cada chunk de m1/m15 (y por hora en tick) y write() hacía `tmp.replace(path)`
(sobreescritura atómica), perdiendo todos los chunks salvo el último.

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

Convención de rutas canónicas (espeja CsvWriter):
  OHLCV: {base}/{SYMBOL}/{tf_folder}/{SYMBOL}_{tf_folder}.parquet
  Tick:  {base}/{SYMBOL}/tick/{SYMBOL}_tick_{YYYY}_{MM}.parquet

Chunks (intermedios, borrados por finalize()):
  {base}/{SYMBOL}/{tf_folder}/.chunks/{SYMBOL}_{tf_folder}[_{YYYY}_{MM}]__{uuid}.parquet

Fase 2B: compresión ZSTD nivel 1.
Fase 2C: nivel configurable vía config.PARQUET_COMPRESSION_LEVEL.

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
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
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

# Subdirectorio donde viven los chunks individuales antes del merge.
_CHUNK_SUBDIR = ".chunks"

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
    """Writer Parquet thread-safe: un archivo por chunk, merge en finalize()."""

    def __init__(self, base_path: Path | str) -> None:
        self._base = Path(base_path)
        # Solo se usa en finalize() (write() ya no comparte fichero).
        self._global_write_lock = threading.Lock()

    # ── Helpers de schema ──────────────────────────────────────────────────

    @staticmethod
    def _schema_for(timeframe: str) -> pa.Schema:
        return _TICK_SCHEMA if timeframe == "tick" else _OHLCV_SCHEMA

    # ── Rutas ──────────────────────────────────────────────────────────────

    def parquet_path(
        self,
        symbol: str,
        timeframe: str,
        dt: datetime | pd.Timestamp | None = None,
    ) -> Path:
        """Ruta Parquet **canónica** (la que produce finalize()).

        Tick está particionado por mes; el resto es un único archivo.
        """
        folder = _TF_FOLDER.get(timeframe, timeframe)

        if timeframe == "tick" and dt is not None:
            ts = pd.Timestamp(dt)
            filename = f"{symbol}_{folder}_{ts.year:04d}_{ts.month:02d}.parquet"
        else:
            filename = f"{symbol}_{folder}.parquet"

        return self._base / symbol / folder / filename

    def _chunk_dir(self, symbol: str, timeframe: str) -> Path:
        folder = _TF_FOLDER.get(timeframe, timeframe)
        return self._base / symbol / folder / _CHUNK_SUBDIR

    def _chunk_path(
        self,
        symbol: str,
        timeframe: str,
        dt: pd.Timestamp,
    ) -> Path:
        """Ruta de un chunk individual, única e irrepetible.

        Para tick se incrusta el token ``YYYY_MM`` en el nombre para que
        finalize() pueda agrupar por mes sin abrir cada archivo.
        """
        folder = _TF_FOLDER.get(timeframe, timeframe)
        token = uuid.uuid4().hex

        if timeframe == "tick":
            stem = f"{symbol}_{folder}_{dt.year:04d}_{dt.month:02d}__{token}"
        else:
            stem = f"{symbol}_{folder}__{token}"

        return self._chunk_dir(symbol, timeframe) / f"{stem}.parquet"

    # ── Escritura (un archivo por chunk) ───────────────────────────────────

    def write(
        self,
        symbol: str,
        timeframe: str,
        df: pd.DataFrame,
        decimal_factor: int = 1,
    ) -> None:
        """Escribe el chunk como un archivo Parquet nuevo y único.

        Nunca sobreescribe: cada llamada crea un fichero distinto en `.chunks/`.
        Escritura atómica vía `*.tmp.parquet` + rename para que finalize() nunca
        vea un archivo a medio escribir. Thread-safe sin lock global.
        """
        if df.empty:
            return

        first_ts = pd.Timestamp(df["timestamp"].iloc[0], unit="ms", tz="UTC")
        chunk_path = self._chunk_path(symbol, timeframe, first_ts)

        schema = self._schema_for(timeframe)
        file_meta = {
            b"decimal_factor": str(decimal_factor).encode(),
            b"symbol":         symbol.upper().encode(),
            b"timeframe":      timeframe.encode(),
            b"source":         b"dukascopy",
        }
        schema_with_meta = schema.with_metadata(file_meta)

        # Conversión CPU-bound fuera de cualquier lock.
        table = pa.Table.from_pandas(df, schema=schema_with_meta, preserve_index=False)

        chunk_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = chunk_path.with_name(f"{chunk_path.stem}.tmp.parquet")
        try:
            pq.write_table(
                table,
                tmp,
                compression=_COMPRESSION,
                compression_level=_COMPRESSION_LEVEL,
            )
            tmp.replace(chunk_path)
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            raise

    # ── Finalización (merge en streaming) ──────────────────────────────────

    def finalize(self, symbol: str, timeframe: str) -> None:
        """Fusiona los chunks en el/los archivo(s) canónico(s).

        - tick:           agrupa los chunks por mes (YYYY_MM) y produce un
                          parquet canónico por mes, fusionando con el canónico
                          previo si ya existía.
        - m1/m15/h1/h4:   fusiona todos los chunks (+ canónico previo) en el
                          único archivo canónico.

        Deduplica por ``timestamp`` (keep="last"), ordena ascendentemente y
        borra los chunks ya consumidos. Idempotente entre runs incrementales.
        """
        chunk_dir = self._chunk_dir(symbol, timeframe)
        folder = _TF_FOLDER.get(timeframe, timeframe)

        with self._global_write_lock:
            chunks = sorted(chunk_dir.glob(f"{symbol}_{folder}_*.parquet")) \
                if chunk_dir.exists() else []
            # Ignorar restos temporales por si un write quedó a medias.
            chunks = [c for c in chunks if not c.name.endswith(".tmp.parquet")]

            if timeframe == "tick":
                self._finalize_tick(symbol, timeframe, chunks)
            else:
                self._finalize_single(symbol, timeframe, chunks)

            # Limpiar el subdirectorio de chunks si quedó vacío.
            if chunk_dir.exists():
                try:
                    next(chunk_dir.iterdir())
                except StopIteration:
                    chunk_dir.rmdir()

    def _finalize_single(self, symbol: str, timeframe: str, chunks: list[Path]) -> None:
        """Caso m1/m15/h1/h4: todo a un único archivo canónico."""
        canonical = self.parquet_path(symbol, timeframe)
        sources = list(chunks)
        if canonical.exists():
            sources.append(canonical)

        if not sources:
            return

        try:
            self._merge_into(sources, canonical, timeframe)
        except Exception as exc:
            log.error("Error finalizando %s/%s: %s", symbol, timeframe, exc)
            return  # No borrar chunks si el merge falló: se reintenta luego.

        for c in chunks:
            c.unlink(missing_ok=True)

    def _finalize_tick(self, symbol: str, timeframe: str, chunks: list[Path]) -> None:
        """Caso tick: un archivo canónico por mes, agrupando chunks por YYYY_MM."""
        folder = _TF_FOLDER["tick"]

        # Agrupar por el token YYYY_MM incrustado en el nombre del chunk.
        # Nombre: {symbol}_{folder}_{YYYY}_{MM}__{uuid}.parquet
        by_month: dict[str, list[Path]] = defaultdict(list)
        for c in chunks:
            month_key = self._tick_month_key(c, symbol, folder)
            if month_key is None:
                log.warning("Chunk tick con nombre inesperado, se omite: %s", c)
                continue
            by_month[month_key].append(c)

        for month_key, month_chunks in by_month.items():
            year, month = month_key.split("_")
            canonical = (
                self._base / symbol / folder
                / f"{symbol}_{folder}_{year}_{month}.parquet"
            )
            sources = list(month_chunks)
            if canonical.exists():
                sources.append(canonical)

            try:
                self._merge_into(sources, canonical, timeframe)
            except Exception as exc:
                log.error(
                    "Error finalizando %s/tick %s: %s", symbol, month_key, exc
                )
                continue  # Conserva estos chunks para reintento.

            for c in month_chunks:
                c.unlink(missing_ok=True)

    @staticmethod
    def _tick_month_key(chunk: Path, symbol: str, folder: str) -> str | None:
        """Extrae 'YYYY_MM' del nombre de un chunk de tick."""
        prefix = f"{symbol}_{folder}_"
        stem = chunk.stem  # sin .parquet
        if not stem.startswith(prefix):
            return None
        rest = stem[len(prefix):]            # "YYYY_MM__uuid"
        rest = rest.split("__", 1)[0]        # "YYYY_MM"
        parts = rest.split("_")
        if len(parts) != 2 or not (parts[0].isdigit() and parts[1].isdigit()):
            return None
        return f"{parts[0]}_{parts[1]}"

    # ── Merge + reinyección de metadata (PyArrow puro) ─────────────────────

    def _merge_into(self, sources: list[Path], dest: Path, timeframe: str) -> None:
        """Concatena + dedup + sort de `sources` → `dest` usando sólo PyArrow.

        Reemplaza la implementación anterior basada en Polars streaming, que
        fallaba silenciosamente por incompatibilidades de la API sink_parquet
        (argumento compression_level no disponible en algunas versiones de
        Polars), dejando todos los chunks sin limpiar.

        Pasos:
          1. Lee cada fuente con pq.read_table y concatena en memoria.
          2. Deduplica por timestamp (keep last via pandas drop_duplicates).
          3. Ordena por timestamp ascendente.
          4. Escribe el resultado a un tmp atómico con metadata completa.
          5. Rename atómico tmp → dest.

        Nota sobre memoria: para tick con 23 × ~5 000 filas/hora el total es
        ~115 000 filas × 5 columnas ≈ 3 MB en RAM, perfectamente asumible.
        Para OHLCV mensual (≤ 744 filas) es trivial.
        """
        if not sources:
            return

        dest.parent.mkdir(parents=True, exist_ok=True)

        meta = self._collect_metadata(sources, timeframe)
        base_schema = self._schema_for(timeframe)
        schema_with_meta = base_schema.with_metadata(meta)

        tmp_final = dest.with_name(f"{dest.stem}_{uuid.uuid4().hex}.tmp.parquet")

        try:
            # Paso 1 — leer y concatenar todas las fuentes.
            tables = []
            for src in sources:
                try:
                    t = pq.read_table(src, schema=base_schema)
                except Exception:
                    # Si el schema difiere (e.g. archivo canónico previo tiene
                    # metadata extra), leer sin schema forzado y castear.
                    t = pq.read_table(src)
                    t = t.select(base_schema.names).cast(base_schema)
                tables.append(t)

            combined = pa.concat_tables(tables)
            del tables  # liberar referencias intermedias

            # Paso 2 — deduplicar y ordenar via pandas (conciso y correcto).
            df = combined.to_pandas()
            del combined
            df = (
                df.sort_values("timestamp")
                  .drop_duplicates(subset=["timestamp"], keep="last")
                  .reset_index(drop=True)
            )

            # Paso 3 — convertir de vuelta a Arrow con schema + metadata.
            result = pa.Table.from_pandas(
                df, schema=schema_with_meta, preserve_index=False
            )
            del df

            # Paso 4 — escribir tmp y hacer replace atómico.
            pq.write_table(
                result,
                tmp_final,
                compression=_COMPRESSION,
                compression_level=_COMPRESSION_LEVEL,
            )
            tmp_final.replace(dest)

        finally:
            if tmp_final.exists():
                tmp_final.unlink(missing_ok=True)

    def _collect_metadata(self, sources: list[Path], timeframe: str) -> dict[bytes, bytes]:
        """Recupera la file-level metadata de la primera fuente que la tenga.

        Prefiere fuentes con metadata completa; si ninguna la trae, sintetiza un
        mínimo razonable (sin decimal_factor) para no escribir un footer vacío.
        """
        for path in sources:
            try:
                md = pq.read_schema(path).metadata
            except Exception:
                md = None
            if md and b"decimal_factor" in md:
                return dict(md)

        # Fallback: metadata mínima (no debería ocurrir; los chunks la incluyen).
        log.warning(
            "Sin decimal_factor en chunks de %s; escribiendo metadata mínima.",
            timeframe,
        )
        return {
            b"timeframe": timeframe.encode(),
            b"source":    b"dukascopy",
        }