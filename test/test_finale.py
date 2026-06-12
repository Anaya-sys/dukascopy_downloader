"""
test_finale.py  ──  Test definitivo de descarga + integridad de checkpoints.

Vista-Controlador: ÚNICAMENTE usa clases ya existentes en el proyecto.
No duplica lógica propia del orchestrator; lo importa directamente.

Uso
---
  python test_finale.py --days 120 --timeframe tick
  python test_finale.py --days 120 --timeframe m15
  python test_finale.py --days 120 --timeframe h1
  python test_finale.py --days 120 --timeframe h4

Flags opcionales
----------------
  --symbol   EURUSD       Símbolo a descargar (default: EURUSD)
  --ruta     ./test_data  Directorio de salida (default: ./test_data)
  --decimal  100000       Factor decimal del símbolo (default: 100 000 para EURUSD)

Comportamiento de checkpoint
----------------------------
  • El checkpoint se guarda INMEDIATAMENTE tras cada día/mes exitoso.
    (No usa el buffer batch del orchestrator → cero pérdida en Ctrl+C.)
  • En un segundo run el script detecta los días ya guardados y los
    muestra como "[ya descargado]" sin volver a descargar.
  • Ctrl+C en mitad de un día: ese día no se checkpointea (no estaba
    completo); el siguiente run lo retoma desde ese día.

Rango fijo de fechas
--------------------
  END_DATE  = 2026-06-11
  START_DATE = END_DATE - --days   (default: 2026-02-11 para 120 días)
"""

from __future__ import annotations

import argparse
import signal
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ── Imports del proyecto (solo clases existentes) ─────────────────────────────
from bi5_decoder import Bi5Decoder
from checkpoint_manager import CheckpointManager
from dukascopy_client import ChunkDownloadError, DukascopyClient
from failure_logger import FailureLogger
from parquet_writer import ParquetWriter
from orchestrator import _DOWNLOAD_TF, _resample_ohlcv   # helpers internos del proyecto
import config as _cfg

UTC = timezone.utc

# ── Fecha final fija ──────────────────────────────────────────────────────────
END_DATE = date(2026, 6, 11)

# ── Alias de timeframe (interfaz de usuario → código interno) ─────────────────
_TF_ALIAS: dict[str, str] = {
    "tick": "tick",
    "m15":  "m15",
    "1h":   "h1",
    "h1":   "h1",
    "4h":   "h4",
    "h4":   "h4",
}

# Reglas de resample (mismas que en orchestrator)
_RESAMPLE_RULE: dict[str, str] = {"m15": "15min", "h4": "4h"}


# ── Helpers de fechas ─────────────────────────────────────────────────────────

def _last_day_of_month(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def _monthly_periods(start: date, end: date):
    """Genera (year, month) para cada mes que solapa con [start, end]."""
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1


# ── Runner principal ──────────────────────────────────────────────────────────

class TestRunner:
    """
    Descarga y verifica datos de un símbolo/timeframe concreto.

    Patrón Vista-Controlador:
      - Vista : la clase actual (bucles, prints, argparse)
      - Controlador: DukascopyClient, Bi5Decoder, ParquetWriter,
                     CheckpointManager, FailureLogger  (ya en el proyecto)
    """

    def __init__(
        self,
        symbol:         str,
        timeframe:      str,           # código interno: tick | m15 | h1 | h4
        days:           int,
        output_path:    Path,
        decimal_factor: int,
    ) -> None:
        self.symbol         = symbol.upper()
        self.timeframe      = timeframe
        self.days           = days
        self.output_path    = output_path
        self.decimal_factor = decimal_factor

        self.start_date = END_DATE - timedelta(days=days)
        self.end_date   = END_DATE

        # ── Componentes del proyecto ──────────────────────────────────────
        self.client     = DukascopyClient(max_retries=_cfg.MAX_RETRIES)
        self.decoder    = Bi5Decoder()
        self.writer     = ParquetWriter(output_path)
        self.checkpoint = CheckpointManager(output_path)
        self.failures   = FailureLogger(output_path)

        # ── Estadísticas de sesión ────────────────────────────────────────
        self._downloaded  = 0     # unidades descargadas (días o meses)
        self._skipped     = 0     # ya en checkpoint
        self._total_recs  = 0     # ticks o velas totales
        self._fail_days   = 0     # días/meses con error de red

        # ── Flag de interrupción ──────────────────────────────────────────
        self._interrupted = False
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame) -> None:  # noqa: ARG002
        self._interrupted = True
        print(
            f"\n\n⚠  Señal {signum} recibida. "
            "El checkpoint del último día COMPLETO ya está guardado.\n"
            "Ejecuta de nuevo para continuar desde donde se interrumpió.\n"
        )

    # ── Punto de entrada ──────────────────────────────────────────────────────

    def run(self) -> None:
        self._print_header()
        self._show_checkpoint_state()

        if   self.timeframe == "tick":
            self._run_tick()
        elif self.timeframe in ("m15",):
            self._run_daily_ohlcv()
        elif self.timeframe in ("h1", "h4"):
            self._run_monthly_ohlcv()
        else:
            print(f"ERROR: timeframe no reconocido: {self.timeframe!r}")
            sys.exit(1)

        if not self._interrupted:
            self._finalize()

        self._print_summary()

    # ── Sección TICK ──────────────────────────────────────────────────────────

    def _run_tick(self) -> None:
        d = self.start_date
        while d <= self.end_date and not self._interrupted:
            # ── Checkpoint: saltar si ya está completo ────────────────────
            last = self.checkpoint.get_last_date(self.symbol, "tick")
            if last and d <= last:
                print(f"  {d}  [ya descargado — saltado]")
                self._skipped += 1
                d += timedelta(days=1)
                continue

            # ── Descarga de las 24 horas del día ─────────────────────────
            day_ticks = self._download_tick_day(d)

            if self._interrupted:
                # El día no estaba completo → no checkpointear
                break

            # ── Guardar checkpoint INMEDIATAMENTE (sin buffer) ────────────
            # Solo para días pasados (hoy puede tener más ticks antes de medianoche)
            if d < date.today():
                self.checkpoint.update(self.symbol, "tick", d)
                cp_tag = "  ✓"
            else:
                cp_tag = "  (hoy — sin checkpoint)"

            # ── Salida por consola ────────────────────────────────────────
            if day_ticks > 0:
                print(f"  {d}  →  {day_ticks:>9,} ticks{cp_tag}")
            else:
                print(f"  {d}  →        sin datos (404 todo el día){cp_tag}")

            self._downloaded += 1
            self._total_recs += day_ticks
            d += timedelta(days=1)

    def _download_tick_day(self, dt: date) -> int:
        """
        Descarga las 24 horas de un día tick.
        Usa DukascopyClient + Bi5Decoder + ParquetWriter del proyecto.
        Retorna el número total de ticks del día.
        """
        day_ticks = 0
        for hour in range(24):
            if self._interrupted:
                break

            hour_dt = datetime(dt.year, dt.month, dt.day, hour, tzinfo=UTC)
            try:
                raw = self.client.download_chunk(self.symbol, "tick", hour_dt)
                if raw is None:
                    continue  # 404 confirmado → no hay datos esta hora

                df = self.decoder.decode(
                    raw, "tick", self.decimal_factor, hour_dt, raw_prices=True
                )
                del raw

                if not df.empty:
                    df.sort_values("timestamp", inplace=True)
                    self.writer.write(
                        self.symbol, "tick", df, decimal_factor=self.decimal_factor
                    )
                    day_ticks += len(df)
                    del df

            except ChunkDownloadError as exc:
                self.failures.log(self.symbol, "tick", hour_dt, str(exc))
                self._fail_days += 1
                print(f"    ✗ {dt} H{hour:02d}: {exc}")

        return day_ticks

    # ── Sección OHLCV DIARIO (m15) ────────────────────────────────────────────

    def _run_daily_ohlcv(self) -> None:
        d = self.start_date
        while d <= self.end_date and not self._interrupted:
            last = self.checkpoint.get_last_date(self.symbol, self.timeframe)
            if last and d <= last:
                print(f"  {d}  [ya descargado — saltado]")
                self._skipped += 1
                d += timedelta(days=1)
                continue

            n = self._download_ohlcv_day(d)

            if self._interrupted:
                break

            if d < date.today():
                self.checkpoint.update(self.symbol, self.timeframe, d)
                cp_tag = "  ✓"
            else:
                cp_tag = "  (hoy — sin checkpoint)"

            if n is None:
                print(f"  {d}  →  sin datos (404){cp_tag}")
            elif n == 0:
                print(f"  {d}  →  0 velas (archivo vacío){cp_tag}")
            else:
                print(f"  {d}  →  {n:>5,} velas{cp_tag}")

            self._downloaded += 1
            self._total_recs += n or 0
            d += timedelta(days=1)

    def _download_ohlcv_day(self, dt: date) -> int | None:
        """Descarga m1 y resamplea a m15. Retorna # velas o None en 404."""
        download_tf = _DOWNLOAD_TF[self.timeframe]          # m15 → "m1"
        base_dt     = datetime(dt.year, dt.month, dt.day, tzinfo=UTC)
        try:
            raw = self.client.download_chunk(self.symbol, download_tf, dt)
            if raw is None:
                return None

            df = self.decoder.decode(
                raw, download_tf, self.decimal_factor, base_dt, raw_prices=True
            )
            del raw
            if df.empty:
                return 0

            rule = _RESAMPLE_RULE.get(self.timeframe)
            if rule:
                df = _resample_ohlcv(df, rule)

            self.writer.write(
                self.symbol, self.timeframe, df, decimal_factor=self.decimal_factor
            )
            n = len(df)
            del df
            return n

        except ChunkDownloadError as exc:
            self.failures.log(self.symbol, self.timeframe, dt, str(exc))
            self._fail_days += 1
            print(f"    ✗ {dt}: {exc}")
            return None

    # ── Sección OHLCV MENSUAL (h1, h4) ────────────────────────────────────────

    def _run_monthly_ohlcv(self) -> None:
        for year, month in _monthly_periods(self.start_date, self.end_date):
            if self._interrupted:
                break

            last           = self.checkpoint.get_last_date(self.symbol, self.timeframe)
            last_of_month  = _last_day_of_month(year, month)

            # Saltar si el checkpoint ya cubre este mes completo
            if last and last >= last_of_month:
                print(f"  {year}-{month:02d}  [ya descargado — saltado]")
                self._skipped += 1
                continue

            n = self._download_ohlcv_month(year, month)

            if self._interrupted:
                break

            # Checkpoint solo si el mes está cerrado (no el mes en curso)
            if last_of_month < date.today():
                self.checkpoint.update(self.symbol, self.timeframe, last_of_month)
                cp_tag = "  ✓"
            else:
                cp_tag = "  (mes en curso — sin checkpoint)"

            if n is None:
                print(f"  {year}-{month:02d}  →  sin datos (404){cp_tag}")
            elif n == 0:
                print(f"  {year}-{month:02d}  →  0 velas (archivo vacío){cp_tag}")
            else:
                print(f"  {year}-{month:02d}  →  {n:>4,} velas{cp_tag}")

            self._downloaded += 1
            self._total_recs += n or 0

    def _download_ohlcv_month(self, year: int, month: int) -> int | None:
        """Descarga h1 y opcionalmente resamplea a h4. Retorna # velas o None."""
        download_tf = _DOWNLOAD_TF[self.timeframe]          # h4 → "h1"
        dt          = date(year, month, 1)
        base_dt     = datetime(year, month, 1, tzinfo=UTC)
        try:
            raw = self.client.download_chunk(self.symbol, download_tf, dt)
            if raw is None:
                return None

            df = self.decoder.decode(
                raw, download_tf, self.decimal_factor, base_dt, raw_prices=True
            )
            del raw
            if df.empty:
                return 0

            rule = _RESAMPLE_RULE.get(self.timeframe)
            if rule:
                df = _resample_ohlcv(df, rule)
            else:
                df.sort_values("timestamp", inplace=True)

            self.writer.write(
                self.symbol, self.timeframe, df, decimal_factor=self.decimal_factor
            )
            n = len(df)
            del df
            return n

        except ChunkDownloadError as exc:
            self.failures.log(self.symbol, self.timeframe, dt, str(exc))
            self._fail_days += 1
            print(f"    ✗ {year}-{month:02d}: {exc}")
            return None

    # ── Merge final de chunks ─────────────────────────────────────────────────

    def _finalize(self) -> None:
        """Llama a ParquetWriter.finalize() para mergear todos los chunks."""
        print(f"\nConsolidando chunks de {self.symbol}/{self.timeframe}…")
        self.writer.finalize(self.symbol, self.timeframe)
        print("  ✓ Consolidación completada.")

    # ── Salida en consola ─────────────────────────────────────────────────────

    def _print_header(self) -> None:
        line = "=" * 62
        unit = "ticks" if self.timeframe == "tick" else "velas"
        print(f"\n{line}")
        print(f"  TEST DE DESCARGA DEFINITIVO")
        print(f"  Símbolo    : {self.symbol}  (decimal_factor={self.decimal_factor:,})")
        print(f"  Timeframe  : {self.timeframe.upper()}")
        print(f"  Rango      : {self.start_date} → {self.end_date}  ({self.days} días)")
        print(f"  Salida     : {self.output_path.resolve()}")
        print(f"  Unidad     : {unit}")
        print(f"{line}\n")

    def _show_checkpoint_state(self) -> None:
        """Muestra el estado actual del checkpoint para este símbolo/timeframe."""
        last = self.checkpoint.get_last_date(self.symbol, self.timeframe)
        if last is None:
            print(f"  CHECKPOINT: ninguno guardado — descarga desde {self.start_date}\n")
        else:
            next_day = last + timedelta(days=1)
            if next_day > self.end_date:
                print(f"  CHECKPOINT: {last}  →  ya al día (todo descargado)\n")
            else:
                print(f"  CHECKPOINT: {last}  →  reanudando desde {next_day}\n")

    def _print_summary(self) -> None:
        line = "=" * 62
        unit = "ticks" if self.timeframe == "tick" else "velas"
        last = self.checkpoint.get_last_date(self.symbol, self.timeframe)

        print(f"\n{line}")
        print(f"  RESUMEN DE SESIÓN")
        print(f"{line}")
        print(f"  Descargados esta sesión : {self._downloaded}")
        print(f"  Saltados (checkpoint)   : {self._skipped}")
        print(f"  Errores de red          : {self._fail_days}")
        print(f"  Total {unit:<6} descargados: {self._total_recs:,}")
        print(f"\n  Checkpoint guardado en  : {last or 'ninguno'}")

        # Verificar archivo de fallos
        fail_path = self.output_path / "failed.log"
        if fail_path.exists() and fail_path.stat().st_size > 0:
            lines = fail_path.read_text(encoding="utf-8").strip().splitlines()
            print(f"\n  ⚠  {len(lines)} fallo(s) en {fail_path}")
        else:
            print(f"\n  ✓ Sin fallos registrados en failed.log")

        if self._interrupted:
            print(
                f"\n  ⚠  Interrumpido. Checkpoint guardado hasta el último día completo."
                f"\n     Ejecuta de nuevo: retomará desde ese punto sin re-descargar nada."
            )
        else:
            print(f"\n  ✓ Sesión completada correctamente.")
        print(f"{line}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test definitivo de descarga Dukascopy — checkpoint + integridad",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=120,
        metavar="N",
        help="Días a cubrir hacia atrás desde 2026-06-11 (default: 120)",
    )
    parser.add_argument(
        "--timeframe", "-t",
        default="tick",
        choices=sorted(_TF_ALIAS.keys()),
        metavar="TF",
        help="tick | m15 | h1 | 1h | h4 | 4h  (default: tick)",
    )
    parser.add_argument(
        "--symbol", "-s",
        default="EURUSD",
        metavar="SYM",
        help="Símbolo Dukascopy (default: EURUSD)",
    )
    parser.add_argument(
        "--ruta", "-r",
        default="./test_data",
        metavar="PATH",
        help="Directorio de salida (default: ./test_data)",
    )
    parser.add_argument(
        "--decimal", "-dec",
        type=int,
        default=100_000,
        metavar="N",
        dest="decimal_factor",
        help="Factor decimal del símbolo (default: 100000 para EURUSD)",
    )

    args = parser.parse_args()

    tf_internal = _TF_ALIAS[args.timeframe]
    output_path = Path(args.ruta)
    output_path.mkdir(parents=True, exist_ok=True)

    runner = TestRunner(
        symbol         = args.symbol,
        timeframe      = tf_internal,
        days           = args.days,
        output_path    = output_path,
        decimal_factor = args.decimal_factor,
    )
    runner.run()


if __name__ == "__main__":
    main()