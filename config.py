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
MAX_WORKERS: int = 8   # máx 16

# ── Política de reintentos ────────────────────────────────────────────────
MAX_RETRIES: int = 3

# ── Timeframes objetivo (códigos internos) ────────────────────────────────
# Usados cuando no se pasa --timeframes en el CLI.
TIMEFRAMES: list[str] = ["tick", "m15", "h1", "h4"]

# Nombres amigables para mostrar al usuario (coinciden con los alias del CLI)
TIMEFRAMES_DISPLAY: list[str] = ["tick", "m15", "1h", "4h"]

# ── HTTP client (httpx) — Fase 1 ─────────────────────────────────────────
# HTTPX_MAX_CONNECTIONS debe coincidir o superar MAX_WORKERS para que cada
# worker pueda tener su propia conexión TCP persistente sin contención.
# Con 8 workers (default) y 16 conexiones hay margen para picos de concurrencia.
HTTPX_MAX_CONNECTIONS: int = 16
HTTPX_MAX_KEEPALIVE_CONNECTIONS: int = 8

# Timeouts separados: connect cubre TCP + TLS handshake; read cubre la
# transferencia del .bi5.  El valor de read coincide con el default previo
# de requests para evitar regresiones de timeout en archivos grandes.
HTTPX_CONNECT_TIMEOUT: float = 10.0   # segundos
HTTPX_READ_TIMEOUT: float    = 30.0   # segundos