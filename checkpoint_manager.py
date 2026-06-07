"""
checkpoint_manager.py

Persiste y recupera el progreso de descarga de forma thread-safe.

Esquema JSON en disco:
  { "EURUSD": { "tick": "2024-03-01", "h1": "2024-02-28" }, ... }
"""

from __future__ import annotations

import json
import threading
from datetime import date
from pathlib import Path


class CheckpointManager:
    """Lee/escribe progress.json con escrituras atómicas y threading.Lock."""

    def __init__(self, base_path: Path) -> None:
        self._path  = Path(base_path) / "progress.json"
        self._lock  = threading.Lock()
        self._data: dict[str, dict[str, str]] = self._load()

    # ── Private helpers ───────────────────────────────────────────────────

    def _load(self) -> dict:
        if self._path.exists():
            with self._path.open(encoding="utf-8") as fh:
                return json.load(fh)
        return {}

    def _save(self) -> None:
        """Escritura atómica: escribe a .tmp y luego hace rename."""
        tmp = self._path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2, ensure_ascii=False)
        tmp.replace(self._path)

    # ── Public API ────────────────────────────────────────────────────────

    def get_last_date(self, symbol: str, timeframe: str) -> date | None:
        """Retorna la última fecha descargada para (symbol, timeframe), o None.

        FIX BUG 3: la lectura también adquiere el lock para evitar leer
        _data mientras otro thread está en medio de un update().
        """
        with self._lock:                          # ← lock añadido
            raw = self._data.get(symbol, {}).get(timeframe)
        if raw is None:
            return None
        return date.fromisoformat(raw)

    def update(self, symbol: str, timeframe: str, d: date) -> None:
        """Registra que (symbol, timeframe) ha sido descargado hasta `d`."""
        with self._lock:
            if symbol not in self._data:
                self._data[symbol] = {}
            self._data[symbol][timeframe] = d.isoformat()
            self._save()