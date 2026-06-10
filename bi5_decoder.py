"""
bi5_decoder.py

Decodes Dukascopy .bi5 binary files (LZMA-compressed) into pandas DataFrames.

Schemas (big-endian):
  Tick:  int32 ms_offset | int32 ask | int32 bid | float32 ask_vol | float32 bid_vol  → 20 bytes
  OHLCV: int32 ms_offset | int32 open | int32 high | int32 low | int32 close | float32 volume → 24 bytes

Phase 2A additions
------------------
  Added ``raw_prices: bool = False`` parameter to ``decode()``.

  raw_prices=True
    - Price columns (ask, bid, open, high, low, close) → int32, unscaled (no division
      by decimal_factor). The original int32 from the binary is returned verbatim.
    - Timestamp column → int64, milliseconds since Unix epoch UTC (instead of strings).
    - Volume columns → float32, unchanged (they are float in the binary).

  raw_prices=False (default)
    - Identical to pre-2A behavior. All existing callers work without modification.

  Offset interpretation (preserved from pre-2A for consistency):
    - Tick    : ms_offset is milliseconds from the start of the hour.
    - OHLCV   : ms_offset field is treated as **seconds** from the start of the period.
      (The raw field name is a misnomer in Dukascopy's format; the existing string-based
      path already used timedelta64[s], and raw_prices=True mirrors that convention.)
"""

from __future__ import annotations

import lzma
import struct
from datetime import datetime
from typing import Union

import numpy as np
import pandas as pd

# ── Public constants (imported by tests) ───────────────────────────────────
TICK_STRUCT_FMT  = ">iiiff"
TICK_RECORD_SIZE = struct.calcsize(TICK_STRUCT_FMT)    # 20 bytes

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

# Column name lists (public for tests)
# BUG #1 FIX: usar "ask_vol"/"bid_vol" — nombre canónico que coincide con
# _TICK_DTYPE, _TICK_SCHEMA de parquet_writer y los campos del binario .bi5.
# El nombre anterior "ask_volume"/"bid_volume" provocaba ArrowInvalid en
# pa.Table.from_pandas() al intentar escribir ticks en modo raw_prices=True.
TICK_COLS  = ["timestamp", "ask", "bid", "ask_vol", "bid_vol"]
OHLCV_COLS = ["timestamp", "open", "high", "low", "close", "volume"]


class Bi5Decoder:
    """Decodes raw .bi5 bytes into a pandas DataFrame."""

    def decode(
        self,
        raw_bytes: bytes,
        timeframe: str,
        decimal_factor: int,
        base_dt: datetime,
        raw_prices: bool = False,
    ) -> pd.DataFrame:
        """
        Decode a .bi5 binary blob into a DataFrame.

        Parameters
        ----------
        raw_bytes      : LZMA-compressed bytes from a .bi5 file (may be empty).
        timeframe      : "tick" | "m1" | "m15" | "h1" | "h4"
        decimal_factor : integer divisor used to convert int32 prices to floats,
                         e.g. 100_000 for EURUSD.
        base_dt        : timezone-aware UTC datetime marking the start of the period
                         (start of hour for tick; start of day/month for OHLCV).
        raw_prices     : [Phase 2A]
                         False (default) — backward-compatible output:
                           * price columns as float32, divided by decimal_factor.
                           * timestamp column as ISO-8601 string with "+00:00" suffix.
                         True — raw output for the Parquet writer:
                           * price columns as int32, NOT divided by decimal_factor.
                             decimal_factor is intentionally ignored in this mode;
                             ParquetWriter stores it as file-level Parquet metadata.
                           * timestamp column as int64 milliseconds since Unix epoch UTC.
                           * volume columns always float32 (unchanged in both modes).

        Returns
        -------
        pd.DataFrame
            Empty DataFrame (with correct columns) if raw_bytes is falsy or the
            decompressed buffer contains zero records.

        Raises
        ------
        ValueError
            If base_dt is not timezone-aware, or if timeframe is unknown.
        lzma.LZMAError
            If the compressed data is corrupt.
        """
        # ── Validation ────────────────────────────────────────────────────
        if base_dt.tzinfo is None:
            raise ValueError(
                "base_dt must be timezone-aware (pass a UTC datetime)."
            )

        if timeframe not in _TICK_TIMEFRAMES and timeframe not in _OHLCV_TIMEFRAMES:
            raise ValueError(
                f"Unknown timeframe: {timeframe!r}. "
                "Valid values: tick, m1, m15, h1, h4."
            )

        is_tick = timeframe in _TICK_TIMEFRAMES
        dtype   = _TICK_DTYPE if is_tick else _OHLCV_DTYPE
        cols    = TICK_COLS if is_tick else OHLCV_COLS

        if not raw_bytes:
            return pd.DataFrame(columns=cols)

        # ── Decompress ───────────────────────────────────────────────────
        decompressed = lzma.decompress(raw_bytes)  # raises LZMAError on corruption

        arr = np.frombuffer(decompressed, dtype=dtype)
        if len(arr) == 0:
            return pd.DataFrame(columns=cols)

        # ── Build DataFrame ───────────────────────────────────────────────
        if raw_prices:
            return self._build_raw(arr, is_tick, base_dt)
        return self._build_float(arr, is_tick, base_dt, decimal_factor)

    # ── Private builders ──────────────────────────────────────────────────

    def _build_float(
        self,
        arr: np.ndarray,
        is_tick: bool,
        base_dt: datetime,
        decimal_factor: int,
    ) -> pd.DataFrame:
        """
        raw_prices=False — backward-compatible mode.

        Prices → float32 (divided by decimal_factor).
        Timestamps → ISO-8601 strings with "+00:00" suffix, no tz-info object.
        """
        base_naive = np.datetime64(base_dt.replace(tzinfo=None))

        if is_tick:
            # Tick: ms_offset is genuine milliseconds from the start of the hour.
            offsets = arr["ms_offset"].astype("timedelta64[ms]")
            ts_arr  = base_naive + offsets
            # Preserve millisecond precision in string (23 chars: "YYYY-MM-DD HH:MM:SS.mmm")
            ts_strs = [str(t)[:23].replace("T", " ") + "+00:00" for t in ts_arr]
            return pd.DataFrame({
                "timestamp":  ts_strs,
                "ask":        arr["ask"].astype(np.float32) / decimal_factor,
                "bid":        arr["bid"].astype(np.float32) / decimal_factor,
                "ask_vol":    arr["ask_vol"].astype(np.float32),
                "bid_vol":    arr["bid_vol"].astype(np.float32),
            })
        else:
            # OHLCV: ms_offset is treated as seconds (existing convention; see module docstring).
            offsets = arr["ms_offset"].astype("timedelta64[s]")
            ts_arr  = base_naive + offsets
            # Second precision in string (19 chars: "YYYY-MM-DD HH:MM:SS")
            ts_strs = [str(t)[:19].replace("T", " ") + "+00:00" for t in ts_arr]
            return pd.DataFrame({
                "timestamp": ts_strs,
                "open":      arr["open"].astype(np.float32)  / decimal_factor,
                "high":      arr["high"].astype(np.float32)  / decimal_factor,
                "low":       arr["low"].astype(np.float32)   / decimal_factor,
                "close":     arr["close"].astype(np.float32) / decimal_factor,
                "volume":    arr["volume"].astype(np.float32),
            })

    def _build_raw(
        self,
        arr: np.ndarray,
        is_tick: bool,
        base_dt: datetime,
    ) -> pd.DataFrame:
        """
        raw_prices=True — Parquet-ready mode (Phase 2A).

        Prices → int32, unscaled (identical to the original binary value).
        Timestamps → int64, milliseconds since Unix epoch UTC.
        Volumes → float32, same as float mode.

        Note: ``decimal_factor`` is intentionally NOT received here because
        in raw mode prices are returned unscaled. The caller (ParquetWriter)
        stores decimal_factor as file-level Parquet metadata instead.

        Timestamp derivation mirrors _build_float to guarantee parity:
          Tick    : ts_ms = base_epoch_ms + ms_offset          (ms_offset already in ms)
          OHLCV   : ts_ms = base_epoch_ms + ms_offset * 1000   (ms_offset treated as seconds)
        """
        base_epoch_ms = np.int64(int(base_dt.timestamp() * 1000))
        offsets_i64   = arr["ms_offset"].astype(np.int64)

        if is_tick:
            ts_ms = (base_epoch_ms + offsets_i64).astype(np.int64)
            return pd.DataFrame({
                "timestamp": ts_ms,
                "ask":       arr["ask"].astype(np.int32),
                "bid":       arr["bid"].astype(np.int32),
                "ask_vol":   arr["ask_vol"].astype(np.float32),   # BUG #1 FIX: ask_vol (no ask_volume)
                "bid_vol":   arr["bid_vol"].astype(np.float32),   # BUG #1 FIX: bid_vol (no bid_volume)
            })
        else:
            # Multiply by 1000 to convert seconds → milliseconds (mirrors timedelta64[s] path)
            ts_ms = (base_epoch_ms + offsets_i64 * 1000).astype(np.int64)
            return pd.DataFrame({
                "timestamp": ts_ms,
                "open":      arr["open"].astype(np.int32),
                "high":      arr["high"].astype(np.int32),
                "low":       arr["low"].astype(np.int32),
                "close":     arr["close"].astype(np.int32),
                "volume":    arr["volume"].astype(np.float32),
            })