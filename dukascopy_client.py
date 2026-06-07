"""
dukascopy_client.py

Descarga chunks individuales .bi5 desde los servidores de Dukascopy.

Formato de URL (el mes es 0-indexed: Ene=00 … Dic=11):
  Tick (por hora):   /SYMBOL/year/MM/DD/HHh_ticks.bi5
  m1 (por día):      /SYMBOL/year/MM/DD/BID_candles_min_1.bi5
  h1 (por mes):      /SYMBOL/year/MM/BID_candles_hour_1.bi5

NOTA: m15 y h4 son timeframes lógicos que NO tienen URL propia en Dukascopy.
  El orchestrator descarga m1 para m15 y h1 para h4, luego hace resample.
  Por eso _TF_CONFIG sólo mapea los timeframes primitivos: tick, m1, h1.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import date, datetime

import requests

log = logging.getLogger(__name__)

_ROOT = "https://datafeed.dukascopy.com/datafeed"

# Sólo timeframes primitivos que tienen URL real en Dukascopy.
# FIX BUG 4/5: se eliminaron "m15" y "h4" (eran alias ambiguos).
# El orchestrator ya traduce m15→m1 y h4→h1 antes de llamar al cliente.
_TF_CONFIG: dict[str, tuple[str, str]] = {
    "tick": ("hour",  "{hour:02d}h_ticks.bi5"),
    "m1":   ("day",   "BID_candles_min_1.bi5"),
    "h1":   ("month", "BID_candles_hour_1.bi5"),
}


class ChunkDownloadError(Exception):
    """Se lanza cuando un chunk falla permanentemente tras todos los reintentos."""


class DukascopyClient:
    """Descarga chunks .bi5 con reintento exponencial."""

    def __init__(
        self,
        max_retries: int = 3,
        base_backoff: float = 1.0,
        timeout: int = 30,
    ) -> None:
        self._max_retries   = max_retries
        self._base_backoff  = base_backoff
        self._timeout       = timeout
        self._session       = requests.Session()
        self._session.headers.update({"User-Agent": "dukascopy-downloader/1.0"})

    # ── URL builder ───────────────────────────────────────────────────────

    def _build_url(
        self,
        symbol: str,
        timeframe: str,
        dt: date | datetime,
    ) -> str:
        if timeframe not in _TF_CONFIG:
            raise ValueError(
                f"Timeframe desconocido: {timeframe!r}. "
                f"Válidos: {list(_TF_CONFIG)}. "
                "Recuerda que m15 → m1 y h4 → h1 (mapeo en orchestrator)."
            )

        granularity, suffix_tpl = _TF_CONFIG[timeframe]
        # Dukascopy usa meses 0-indexed
        month_0 = dt.month - 1
        base = f"{_ROOT}/{symbol.upper()}/{dt.year}/{month_0:02d}"

        if granularity == "month":
            return f"{base}/{suffix_tpl}"
        elif granularity == "day":
            return f"{base}/{dt.day:02d}/{suffix_tpl}"
        else:  # hour
            hour = dt.hour if isinstance(dt, datetime) else 0
            return f"{base}/{dt.day:02d}/{suffix_tpl.format(hour=hour)}"

    # ── Download ──────────────────────────────────────────────────────────

    def download_chunk(
        self,
        symbol: str,
        timeframe: str,
        dt: date | datetime,
    ) -> bytes | None:
        """
        Descarga un chunk .bi5.

        Returns
        -------
        bytes  : bytes comprimidos crudos (puede ser vacío si no hay datos)
        None   : HTTP 404 / no hay datos para este período
        Lanza ChunkDownloadError si se agotan los reintentos en errores no-404.
        """
        url = self._build_url(symbol, timeframe, dt)
        last_exc: Exception | None = None

        for attempt in range(self._max_retries):
            try:
                resp = self._session.get(url, timeout=self._timeout)

                if resp.status_code == 404:
                    return None

                resp.raise_for_status()
                return resp.content

            except requests.RequestException as exc:
                last_exc = exc
                wait = self._base_backoff * (2 ** attempt) + random.uniform(0, 0.5)
                log.warning(
                    "Intento %d/%d falló para %s %s %s: %s. Reintentando en %.1fs…",
                    attempt + 1, self._max_retries, symbol, timeframe, dt, exc, wait,
                )
                time.sleep(wait)

        raise ChunkDownloadError(
            f"Falló la descarga de {symbol}/{timeframe}/{dt} tras "
            f"{self._max_retries} intentos: {last_exc}"
        )