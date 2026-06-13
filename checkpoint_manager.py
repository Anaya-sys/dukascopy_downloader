"""
checkpoint_manager.py

Persiste y recupera el progreso de descarga de forma thread-safe.

Esquema JSON en disco:
  {
    "_session": {
      "date_from": "2020-01-01",   # fecha de inicio que configuró el usuario
      "date_to":   "2024-12-31",   # fecha de fin que configuró el usuario
      "paused":    true            # true si el usuario pausó (no interrumpió)
    },
    "EURUSD": { "tick": "2024-03-01", "h1": "2024-02-28" },
    ...
  }

La clave "_session" es reservada y nunca se interpreta como símbolo.
"""

from __future__ import annotations

import json
import threading
from datetime import date
from pathlib import Path


class CheckpointManager:
    """Lee/escribe progress.json con escrituras atómicas y threading.Lock."""

    _SESSION_KEY = "_session"

    def __init__(self, base_path: Path) -> None:
        self._path  = Path(base_path) / "progress.json"
        self._lock  = threading.RLock()
        self._data: dict = self._load()

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
        """Retorna la última fecha descargada para (symbol, timeframe), o None."""
        with self._lock:
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

    # ── Session metadata ─────────────────────────────────────────────────

    def save_session(self, date_from: date, date_to: date, paused: bool = False) -> None:
        """Guarda la configuración de la sesión (fechas + estado pausa)."""
        with self._lock:
            self._data[self._SESSION_KEY] = {
                "date_from": date_from.isoformat(),
                "date_to":   date_to.isoformat(),
                "paused":    paused,
            }
            self._save()

    def set_paused(self, paused: bool) -> None:
        """Actualiza solo el flag paused sin tocar las fechas."""
        with self._lock:
            sess = self._data.setdefault(self._SESSION_KEY, {})
            sess["paused"] = paused
            self._save()

    def get_session(self) -> dict | None:
        """Retorna el dict de sesión o None si no existe."""
        with self._lock:
            return self._data.get(self._SESSION_KEY)

    def clear(self) -> None:
        """Borra todo el progreso (nueva descarga desde cero)."""
        with self._lock:
            self._data = {}
            self._save()

    def has_progress_for(self, symbol: str) -> bool:
        """True si hay al menos un timeframe con checkpoint para este símbolo."""
        with self._lock:
            sym_data = self._data.get(symbol, {})
            return bool(sym_data)

    def get_earliest_year(self, symbol: str) -> int | None:
        """Retorna el año más temprano checkpointeado para el símbolo, o None."""
        with self._lock:
            sym_data = self._data.get(symbol, {})
            years = []
            for val in sym_data.values():
                try:
                    years.append(date.fromisoformat(val).year)
                except (ValueError, TypeError):
                    pass
            return min(years) if years else None