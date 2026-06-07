"""
main.py

Punto de entrada del Dukascopy Historical Data Downloader.

Uso
---
  python main.py                                        # ruta por defecto de config.py
  python main.py --ruta /ruta/a/datos                  # directorio de salida personalizado
  python main.py --ruta /ruta/a/datos --workers 12
  python main.py --ruta /ruta/a/datos --timeframes m15 1h
  python main.py --ruta /ruta/a/datos --timeframes tick m15 1h 4h

Nombres válidos de timeframes
------------------------------
  tick   → ticks (datos raw, por hora)
  m15    → velas de 15 minutos
  1h     → velas de 1 hora
  4h     → velas de 4 horas
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# ── Logging setup ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ── Mapeo de nombres de usuario → códigos internos ────────────────────────
# El usuario escribe "1h" o "4h"; internamente el código usa "h1" / "h4".
_TF_ALIAS: dict[str, str] = {
    "tick": "tick",
    "m15":  "m15",
    "1h":   "h1",
    "h1":   "h1",   # también se acepta la forma interna por compatibilidad
    "4h":   "h4",
    "h4":   "h4",
}

_VALID_CHOICES = list(_TF_ALIAS.keys())  # tick, m15, 1h, h1, 4h, h4


def _parse_timeframes(raw: list[str]) -> list[str]:
    """Convierte los nombres de usuario a códigos internos y elimina duplicados."""
    result: list[str] = []
    seen: set[str] = set()
    for item in raw:
        internal = _TF_ALIAS.get(item.lower())
        if internal is None:
            print(
                f"Error: timeframe desconocido '{item}'. "
                f"Válidos: {', '.join(_VALID_CHOICES)}",
                file=sys.stderr,
            )
            sys.exit(1)
        if internal not in seen:
            seen.add(internal)
            result.append(internal)
    return result


def main() -> None:
    import config

    parser = argparse.ArgumentParser(
        description="Dukascopy Historical Data Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--ruta", "-r",
        metavar="RUTA",
        default=None,
        help=f"Directorio de salida (default: {config.BASE_PATH})",
    )
    # Mantener --output como alias oculto para compatibilidad con scripts previos
    parser.add_argument(
        "--output", "-o",
        metavar="PATH",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=config.MAX_WORKERS,
        metavar="N",
        help=f"Workers de descarga paralela, máx 16 (default: {config.MAX_WORKERS})",
    )
    parser.add_argument(
        "--timeframes", "-t",
        nargs="+",
        default=None,
        metavar="TF",
        help=(
            "Timeframes a descargar. Valores válidos: tick  m15  1h  4h  "
            f"(default: {' '.join(config.TIMEFRAMES_DISPLAY)})"
        ),
    )
    args = parser.parse_args()

    # ── Resolver ruta de salida ───────────────────────────────────────────
    # --ruta tiene precedencia; --output queda como fallback de compatibilidad
    raw_path = args.ruta or args.output
    output_path = Path(raw_path) if raw_path else config.BASE_PATH
    workers = min(max(1, args.workers), 16)

    # ── Resolver timeframes ───────────────────────────────────────────────
    raw_tfs = args.timeframes if args.timeframes else config.TIMEFRAMES_DISPLAY
    timeframes = _parse_timeframes(raw_tfs)

    # ── Crear directorio de salida si no existe ───────────────────────────
    if output_path.exists():
        print(f"Directorio de salida: {output_path}  (ya existe)")
    else:
        output_path.mkdir(parents=True, exist_ok=True)
        print(f"Directorio de salida creado: {output_path}")

    print(f"Workers    : {workers}")
    print(f"Timeframes : {', '.join(raw_tfs)}")
    print()

    # ── Ejecutar ──────────────────────────────────────────────────────────
    from orchestrator import DownloadOrchestrator
    orchestrator = DownloadOrchestrator(
        output_path=output_path,
        max_workers=workers,
        max_retries=config.MAX_RETRIES,
    )
    orchestrator.run(timeframes=timeframes)


if __name__ == "__main__":
    main()