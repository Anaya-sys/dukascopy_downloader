"""
github_scraper.py

Descarga metadatos de instrumentos desde el repositorio GitHub de dukascopy-node
y retorna una lista de instancias de la dataclass Instrument.

JSON fuente: https://raw.githubusercontent.com/.../instrument-meta-data.json
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import List

import requests

log = logging.getLogger(__name__)

_META_URL = (
    "https://raw.githubusercontent.com/Leo4815162342/dukascopy-node"
    "/master/src/utils/instrument-meta-data/generated/instrument-meta-data.json"
)

# FIX: se eliminó "m1" (era código muerto; m1 no es un timeframe seleccionable
# y sólo se usa internamente como base de descarga para m15).
_TF_START_KEY: dict[str, str] = {
    "tick": "startHourForTicks",
    "m1":   "startDayForMinuteCandles",
    "h1":   "startMonthForHourlyCandles",
}


def _parse_iso(raw: str) -> date:
    """Parsea un string ISO-8601 a fecha UTC."""
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).date()


@dataclass
class Instrument:
    symbol: str          # mayúsculas, p.ej. "EURUSD"
    decimal_factor: int  # p.ej. 100_000
    start_dates: dict    # {"tick": date, "m15": date, "h1": date, "h4": date}


class GitHubScraper:
    """Obtiene la lista de instrumentos desde el JSON de metadatos de dukascopy-node."""

    def __init__(self, timeout: int = 30) -> None:
        self._timeout = timeout

    def scrape(self) -> List[Instrument]:
        """Retorna una lista de todos los Instrument disponibles."""
        log.info("Obteniendo metadatos de instrumentos desde GitHub…")
        resp = requests.get(_META_URL, timeout=self._timeout)
        resp.raise_for_status()
        raw: dict = resp.json()

        instruments: List[Instrument] = []
        for key, meta in raw.items():
            try:
                decimal_factor = int(meta.get("decimalFactor", 1))
                start_dates: dict[str, date] = {}

                for tf, json_key in _TF_START_KEY.items():
                    raw_date = meta.get(json_key)
                    if raw_date:
                        start_dates[tf] = _parse_iso(raw_date)

                if not start_dates:
                    continue

                instruments.append(
                    Instrument(
                        symbol=key.upper(),
                        decimal_factor=decimal_factor,
                        start_dates=start_dates,
                    )
                )
            except Exception as exc:
                log.warning("Saltando instrumento %r: %s", key, exc)

        log.info("Se encontraron %d instrumentos.", len(instruments))
        return instruments