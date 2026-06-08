"""
bi5_decoder.py

Decodes Dukascopy .bi5 binary files (LZMA-compressed) into pandas DataFrames.

Schemas (big-endian):
  Tick:  int32 ms_offset | int32 ask | int32 bid | float32 ask_vol | float32 bid_vol  → 20 bytes
  OHLCV: int32 ms_offset | int32 open | int32 high | int32 low | int32 close | float32 volume → 24 bytes
"""

from __future__ import annotations

import lzma
import struct
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# ── Public constants (imported by tests) ───────────────────────────────────
TICK_STRUCT_FMT  = ">iiiff"
TICK_RECORD_SIZE = struct.calcsize(TICK_STRUCT_FMT)   # 20 bytes

OHLCV_STRUCT_FMT  = ">iiiiif"
OHLCV_RECORD_SIZE = struct.calcsize(OHLCV_STRUCT_FMT)  # 24 bytes

_TICK_TIMEFRAMES  = {"tick"}
_OHLCV_TIMEFRAMES = {"m1", "m15", "h1", "h4"}

_TICK_DTYPE = np.dtype([
    ("ms_offset", ">i4"),
    ("ask",       ">i4"),
    ("bid",       ">i4"),
    ("ask_vol",   ">f4"),
    ("bid_vol",   ">f4"),
])

_OHLCV_DTYPE = np.dtype([
    ("ms_offset", ">i4"),
    ("open",      ">i4"),
    ("high",      ">i4"),
    ("low",       ">i4"),
    ("close",     ">i4"),
    ("volume",    ">f4"),
])



class Bi5Decoder:
    """Decodes raw .bi5 bytes into a pandas DataFrame."""

    def decode(
        self,
        raw_bytes: bytes,
        timeframe: str,
        decimal_factor: int,
        base_dt: datetime,
    ) -> pd.DataFrame:
        """
        Parameters
        ----------
        raw_bytes      : compressed (or empty) bytes from a .bi5 file
        timeframe      : "tick" | "m1" | "m15" | "h1" | "h4"
        decimal_factor : integer divisor (e.g. 100_000 for EURUSD)
        base_dt        : timezone-aware datetime for the start of the period
                         (start of hour for tick, start of day/month for OHLCV)

        Returns
        -------
        pd.DataFrame with appropriate columns
        """
        # ── Validation ────────────────────────────────────────────────────
        if base_dt.tzinfo is None:
            raise ValueError("base_dt must be timezone-aware (pass a UTC datetime)")

        # Bug D fix: bloque único de asignación; eliminadas las asignaciones
        # duplicadas de is_tick/cols/fmt/size del if/elif anterior (código muerto).
        if timeframe not in _TICK_TIMEFRAMES and timeframe not in _OHLCV_TIMEFRAMES:
            raise ValueError(f"Unknown timeframe: {timeframe!r}. Valid: tick, m1, m15, h1, h4")

        is_tick = timeframe in _TICK_TIMEFRAMES
        dtype   = _TICK_DTYPE if is_tick else _OHLCV_DTYPE
        cols    = ["timestamp","ask","bid","ask_volume","bid_volume"] if is_tick \
              else ["timestamp","open","high","low","close","volume"]
        
        if not raw_bytes:
            return pd.DataFrame(columns=cols)

        # ── Decompress ───────────────────────────────────────────────────
        decompressed = lzma.decompress(raw_bytes)  # raises LZMAError on corruption

        arr = np.frombuffer(decompressed, dtype=dtype)
        if len(arr) == 0:
           return pd.DataFrame(columns=cols)

    # Timestamps vectorizados
        if is_tick:
           offsets = arr["ms_offset"].astype("timedelta64[ms]")
           ts_arr  = np.datetime64(base_dt.replace(tzinfo=None)) + offsets
           ts_strs = [str(t)[:23].replace("T", " ") + "+00:00" for t in ts_arr]
           df = pd.DataFrame({
                "timestamp":  ts_strs,
                "ask":        arr["ask"].astype(np.float32) / decimal_factor,
                "bid":        arr["bid"].astype(np.float32) / decimal_factor,
                "ask_volume": arr["ask_vol"].astype(np.float32),
                "bid_volume": arr["bid_vol"].astype(np.float32),
        })
        else:
           offsets = arr["ms_offset"].astype("timedelta64[s]")
           ts_arr  = np.datetime64(base_dt.replace(tzinfo=None)) + offsets
           ts_strs = [str(t)[:19].replace("T", " ") + "+00:00" for t in ts_arr]
           df = pd.DataFrame({
                "timestamp": ts_strs,
                "open":      arr["open"].astype(np.float32)  / decimal_factor,
                "high":      arr["high"].astype(np.float32)  / decimal_factor,
                "low":       arr["low"].astype(np.float32)   / decimal_factor,
                "close":     arr["close"].astype(np.float32) / decimal_factor,
                "volume":    arr["volume"].astype(np.float32),
        })
           
        return df