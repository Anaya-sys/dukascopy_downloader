"""
failure_logger.py

Appends failed chunk information to failed.log in the output directory.

Log line format:
  YYYY-MM-DD HH:MM:SS | SYMBOL | TIMEFRAME | DATE | ERROR_MSG
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path


class FailureLogger:
    """Thread-safe writer for failed.log."""

    def __init__(self, base_path: Path) -> None:
        self._path = Path(base_path) / "failed.log"
        self._lock = threading.Lock()

    def log(self, symbol: str, timeframe: str, dt: object, error: str) -> None:
        """Append one failure record."""
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        line = f"{now} | {symbol} | {timeframe} | {dt} | {error}\n"
        with self._lock:
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(line)