"""
csv_writer.py

Escribe (o hace append) pandas DataFrames a archivos CSV de forma thread-safe.

Convención de rutas:
  OHLCV: {base}/{SYMBOL}/{tf_folder}/{SYMBOL}_{tf_folder}.csv
  Ticks: {base}/{SYMBOL}/tick/{SYMBOL}_tick_{YYYY}_{MM}.csv

Mapeo timeframe → carpeta:
  tick → tick  |  m15 → 15min  |  h1 → 1h  |  h4 → 4h  |  m1 → 1min
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
import gc
import pandas as pd
import polars as pl

_TF_FOLDER: dict[str, str] = {
    "tick": "tick",
    "m15":  "15min",
    "h1":   "1h",
    "h4":   "4h",
    "m1":   "1min",
}


class CsvWriter:
    """Escritor CSV thread-safe optimizado para grandes volúmenes de datos."""

    def __init__(self, base_path: Path | str) -> None:
        self._base = Path(base_path)
        self._global_write_lock = threading.Lock()

    def csv_path(self, symbol: str, timeframe: str, dt: pd.Timestamp | None = None) -> Path:
        """Retorna la ruta CSV canónica."""
        folder = _TF_FOLDER.get(timeframe, timeframe)
        
        # Particionamiento de archivos SOLO para ticks
        if timeframe == "tick" and dt is not None:
            filename = f"{symbol}_{folder}_{dt.year:04d}_{dt.month:02d}.csv"
        else:
            filename = f"{symbol}_{folder}.csv"
            
        return self._base / symbol / folder / filename

    def write(self, symbol: str, timeframe: str, df: pd.DataFrame,
              decimal_factor: int = 1) -> None:
        """Escribe el DataFrame en disco usando Append-Only ultrarrápido para todos."""
        if df.empty:
            return

        first_ts = pd.to_datetime(df["timestamp"].iloc[0])
        path = self.csv_path(symbol, timeframe, first_ts)

        with self._global_write_lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            write_header = not path.exists()
            # Append ciego y veloz. La limpieza se hará al final.
            df.to_csv(path, mode="a", header=write_header, index=False)

    def finalize(self, symbol: str, timeframe: str) -> None:
        """Lee el archivo completo de OHLCV, lo deduplica, ordena y lo guarda limpio."""
        # Los ticks ya están limpios por su naturaleza y particionados, no necesitan esto.
        if timeframe == "tick":
            return
            
        path = self.csv_path(symbol, timeframe)
        if not path.exists():
            return
            
        with self._global_write_lock:
             try:
                 tmp = path.with_suffix(".tmp.csv")
                 (
                    pl.scan_csv(path)
                    .unique(subset=["timestamp"], keep="last")
                    .sort("timestamp")
                    .sink_csv(tmp)      
                 )
                 tmp.replace(path)
             except Exception as e:
                logging.getLogger(__name__).error("Error finalizando %s: %s", path, e)