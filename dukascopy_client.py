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

Fase 1 — Migración HTTP (requests → httpx):
  - httpx.Client con http2=True: aprovecha HTTP/2 automáticamente si el
    servidor lo soporta; retrocede a HTTP/1.1 de forma transparente.
  - Pool de conexiones configurable vía HTTPX_MAX_CONNECTIONS y
    HTTPX_MAX_KEEPALIVE_CONNECTIONS en config.py.
  - Timeouts separados (connect vs read) vía HTTPX_CONNECT_TIMEOUT y
    HTTPX_READ_TIMEOUT en config.py.
  - httpx.Client es thread-safe: puede ser compartido entre todos los
    workers del ThreadPoolExecutor sin modificación al orchestrator.
  - Interfaz pública sin cambios: download_chunk(symbol, timeframe, dt)
    retorna bytes | None y lanza ChunkDownloadError en fallo permanente.

Verificación de HTTP/2 (antes de desplegar):
  curl -sI --http2 https://datafeed.dukascopy.com | grep -i "HTTP/"
  → HTTP/2 200   : servidor soporta HTTP/2, multiplexing activo.
  → HTTP/1.1 200 : servidor habla solo HTTP/1.1; httpx retrocede sin error.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import date, datetime

import httpx

import config as _cfg

log = logging.getLogger(__name__)

_ROOT = "https://datafeed.dukascopy.com/datafeed"

# Sólo timeframes primitivos que tienen URL real en Dukascopy.
# m15 → m1 y h4 → h1 (traducción en orchestrator antes de llamar aquí).
_TF_CONFIG: dict[str, tuple[str, str]] = {
    "tick": ("hour",  "{hour:02d}h_ticks.bi5"),
    "m1":   ("day",   "BID_candles_min_1.bi5"),
    "h1":   ("month", "BID_candles_hour_1.bi5"),
}


class ChunkDownloadError(Exception):
    """Se lanza cuando un chunk falla permanentemente tras todos los reintentos."""


class DukascopyClient:
    """Descarga chunks .bi5 con reintento exponencial.

    Usa httpx.Client con HTTP/2 y pool de conexiones configurable.
    El cliente es thread-safe y puede ser compartido entre workers.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_backoff: float = 1.0,
        timeout: int = 30,  # retenido para compatibilidad con llamadas existentes;
                            # los timeouts reales se leen desde config.HTTPX_*_TIMEOUT
        max_404_retries: int = _cfg.MAX_404_RETRIES,
    ) -> None:
        self._max_retries     = max_retries
        self._base_backoff    = base_backoff
        self._max_404_retries = max_404_retries

        # httpx.Timeout(default, connect=override): el primer argumento establece
        # el default para read/write/pool; connect se sobreescribe con el valor
        # de config para diferenciar el handshake TLS de la transferencia de datos.
        _timeout = httpx.Timeout(
            _cfg.HTTPX_READ_TIMEOUT,
            connect=_cfg.HTTPX_CONNECT_TIMEOUT,
        )

        # CONCLUSIÓN EXPERIMENTAL (Teoría 1): HTTP/2 negociado con Dukascopy
        # provoca httpx.ReadError bajo multiplexación.  http2 se controla vía
        # config.HTTP2_ENABLED (False por defecto → HTTP/1.1 keep-alive).
        self._client = httpx.Client(
            http2=_cfg.HTTP2_ENABLED,
            limits=httpx.Limits(
                max_connections=_cfg.HTTPX_MAX_CONNECTIONS,
                max_keepalive_connections=_cfg.HTTPX_MAX_KEEPALIVE_CONNECTIONS,
            ),
            timeout=_timeout,
            headers={"User-Agent": "dukascopy-downloader/1.0"},
        )

    def close(self) -> None:
        """Cierra el cliente y libera las conexiones del pool.

        Llamar explícitamente al finalizar el proceso o en contextos de
        testing para evitar ResourceWarning de httpx.
        """
        self._client.close()

    def __del__(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

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
        None   : HTTP 404 — no hay datos para este período
        Lanza ChunkDownloadError si se agotan los reintentos en errores de red
        o de status HTTP (4xx/5xx distintos de 404).

        Nota sobre excepciones capturadas en el retry loop:
          httpx.RequestError    — errores de red (timeout, conexión rechazada…)
          httpx.HTTPStatusError — raise_for_status() para 4xx/5xx distintos de 404
          Ambas se reintentan; esto preserva la semántica original de
          requests.RequestException que cubría ambas categorías.
        """
        url = self._build_url(symbol, timeframe, dt)
        last_exc: Exception | None = None
        seen_404 = 0

        for attempt in range(self._max_retries):
            try:
                resp = self._client.get(url)

                # DEBUG: registrar el protocolo negociado con el servidor.
                # En producción con HTTP/2: "HTTP/2 200 EURUSD …"
                # En fallback HTTP/1.1:    "HTTP/1.1 200 EURUSD …"
                log.debug(
                    "%s %d  %s/%s/%s",
                    resp.http_version, resp.status_code, symbol, timeframe, dt,
                )

                if resp.status_code == 404:
                    # FIX Teoría 2: un 404 puede ser un fallo transitorio del
                    # servidor, no ausencia real de datos.  Reintentamos antes
                    # de aceptarlo.  Solo un 404 PERSISTENTE se interpreta como
                    # "no hay datos" (None).  Un 404 intermitente que luego
                    # devuelve 200 deja de producir huecos silenciosos.
                    seen_404 += 1
                    if seen_404 > self._max_404_retries:
                        return None
                    wait = self._base_backoff * (2 ** attempt) + random.uniform(0, 0.5)
                    log.warning(
                        "404 (%d/%d) para %s %s %s. Reverificando en %.1fs…",
                        seen_404, self._max_404_retries, symbol, timeframe, dt, wait,
                    )
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.content

            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                # httpx.RequestError  → errores de transporte (timeout, reset…)
                # httpx.HTTPStatusError → raise_for_status() para 4xx/5xx
                # Ambos se reintentan para preservar el comportamiento previo de
                # requests.RequestException.  El PRD menciona solo httpx.RequestError;
                # se añade HTTPStatusError para mantener el retry-on-5xx.
                last_exc = exc
                wait = self._base_backoff * (2 ** attempt) + random.uniform(0, 0.5)
                log.warning(
                    "Intento %d/%d falló para %s %s %s: %s. Reintentando en %.1fs…",
                    attempt + 1, self._max_retries, symbol, timeframe, dt, exc, wait,
                )
                time.sleep(wait)

        # Si el loop se agotó habiendo visto SOLO 404s (sin error de red),
        # se interpreta como ausencia real de datos.
        if last_exc is None and seen_404 > 0:
            return None

        raise ChunkDownloadError(
            f"Falló la descarga de {symbol}/{timeframe}/{dt} tras "
            f"{self._max_retries} intentos: {last_exc}"
        )
