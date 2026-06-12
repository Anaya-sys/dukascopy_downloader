"""
config.py

Configuración global. Sobreescribe BASE_PATH con --ruta en el CLI o editando aquí.
"""

from __future__ import annotations

import pathlib

# ── Ruta de salida ────────────────────────────────────────────────────────
BASE_PATH: pathlib.Path = (
    pathlib.Path.home() / "Desktop" / "data" / "cdfs"
)

# ── Paralelismo ───────────────────────────────────────────────────────────
# CONCLUSIÓN EXPERIMENTAL (Teoría 1): el throughput está limitado por el
# servidor (throttle por IP), no por el cliente.  8 workers no superan a 4 en
# wall-time (~4.8s para 24 chunks en ambos casos).  Se fija en 4 para reducir
# presión sobre el servidor y la probabilidad de reset de conexión, sin perder
# velocidad.  El solapamiento de red+decode aún justifica >1 worker.
MAX_WORKERS: int = 4

# ── Política de reintentos ────────────────────────────────────────────────
MAX_RETRIES: int = 3

# Reintentos específicos para HTTP 404 (Teoría 2): un 404 puede ser un fallo
# transitorio del servidor, no una ausencia real de datos.  Antes de aceptar
# un 404 como "no hay datos" se reintenta este número de veces.  Solo si el
# 404 persiste se considera ausencia genuina.
MAX_404_RETRIES: int = 2

# ── Timeframes objetivo (códigos internos) ────────────────────────────────
# Usados cuando no se pasa --timeframes en el CLI.
TIMEFRAMES: list[str] = ["tick", "m15", "h1", "h4"]

# Nombres amigables para mostrar al usuario (coinciden con los alias del CLI)
TIMEFRAMES_DISPLAY: list[str] = ["tick", "m15", "1h", "4h"]

# ── Almacenamiento — Fase 2C ─────────────────────────────────────────────
# STORAGE_FORMAT controla qué writer instancia el orchestrator.
# Valores válidos: "parquet" | "csv".
# Sobreescribible en runtime vía --format en el CLI.
STORAGE_FORMAT: str = "parquet"

# Compresión Parquet (usado por ParquetWriter).  ZSTD nivel 1 maximiza
# la velocidad de escritura en streaming de tick data.
PARQUET_COMPRESSION: str = "zstd"
PARQUET_COMPRESSION_LEVEL: int = 1

# ── HTTP client (httpx) — Fase 1 ─────────────────────────────────────────
# CONCLUSIÓN EXPERIMENTAL (Teoría 1): el servidor datafeed.dukascopy.com
# negocia HTTP/2 pero la conexión multiplexada se cae a mitad de transferencia
# (httpx.ReadError).  HTTP/1.1 con keep-alive iguala el rendimiento sin ese
# fallo.  HTTP2_ENABLED=False fuerza HTTP/1.1 en el cliente.
HTTP2_ENABLED: bool = False

# Con 4 workers basta un pool pequeño.  Mantener keepalive >= workers para que
# cada worker reutilice su conexión TCP persistente (evita re-handshake TLS,
# que era la única fuente de lentitud en el modo secuencial).
HTTPX_MAX_CONNECTIONS: int = 8
HTTPX_MAX_KEEPALIVE_CONNECTIONS: int = 8

# Timeouts separados: connect cubre TCP + TLS handshake; read cubre la
# transferencia del .bi5.  El valor de read coincide con el default previo
# de requests para evitar regresiones de timeout en archivos grandes.
HTTPX_CONNECT_TIMEOUT: float = 10.0   # segundos
HTTPX_READ_TIMEOUT: float    = 30.0   # segundos
