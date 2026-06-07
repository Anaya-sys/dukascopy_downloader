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