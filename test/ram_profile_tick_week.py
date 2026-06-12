"""
ram_profile_tick_week.py

Script de profiling: descarga N días de tick data para 1 símbolo, los escribe
en Parquet usando ÚNICAMENTE los componentes ya existentes del pipeline
(DukascopyClient, Bi5Decoder, ParquetWriter, FailureLogger, config), y mide
el pico de RSS (memoria residente del proceso) día por día.

Objetivo
--------
Responder empíricamente a la duda sobre ParquetWriter.finalize() para "tick":
  ¿el costo (tiempo + memoria) de finalize() crece con el tamaño acumulado
  del canónico mensual (patrón ~O(d²) a lo largo de un mes), o se mantiene
  plano?

Para eso, en cada día del rango se llama a finalize() inmediatamente después
de escribir los chunks de ese día — simulando una corrida diaria real — y se
registra tiempo + delta de RSS de esa llamada. Si el último día cuesta
notablemente más que el primero, el patrón está confirmado.

Diseño Vista/Controlador
-------------------------
  _MemorySampler      → utilidad de bajo nivel: hilo en background que
                        muestrea RSS y expone el pico observado vía peek().
                        No imprime nada.

  TickWeekController  → CONTROLADOR. Orquesta descarga + decode + write +
                        finalize, reutilizando exactamente los mismos
                        componentes que orchestrator.py. No imprime nada;
                        produce una lista de DayResult.

  ConsoleView         → VISTA. Recibe los DayResult y los imprime en una
                        tabla, más un veredicto final sobre el patrón O(d²).

  main()              → cablea CLI → Controlador → Vista.

Uso
---
  python ram_profile_tick_week.py
  python ram_profile_tick_week.py --symbol GBPUSD --days 7
  python ram_profile_tick_week.py --start 2025-03-01 --ruta /tmp/ram_test
  python ram_profile_tick_week.py --decimal-factor 1000 --symbol USDJPY

Medición de RSS
---------------
  1. psutil.Process().memory_info().rss   (preciso, funciona en Windows)
  2. resource.getrusage(RUSAGE_SELF).ru_maxrss  (fallback POSIX)
  3. Si ninguno está disponible (típicamente Windows sin psutil), el script
     sigue funcionando y reporta tiempos, pero la memoria aparece como "N/D".
     Recomendado: pip install psutil
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dukascopy_client import ChunkDownloadError, DukascopyClient
from bi5_decoder import Bi5Decoder
from parquet_writer import ParquetWriter
from failure_logger import FailureLogger
import config as _cfg

UTC = timezone.utc


# ───────────────────────────────────────────────────────────────────────────
# Utilidad de bajo nivel: muestreo de RSS en background
# ───────────────────────────────────────────────────────────────────────────

def _make_reader():
    """Retorna una función () -> bytes de RSS actual, o None si no es posible.

    Orden de preferencia:
      1. psutil.Process().memory_info().rss
         RSS real instantáneo, multiplataforma (incluye Windows).
      2. resource.getrusage(RUSAGE_SELF).ru_maxrss
         POSIX. Ya es un PICO acumulado desde el arranque del proceso (KB en
         Linux, bytes en macOS). Nos sirve igual porque sólo nos interesan
         los DELTAS entre fases, y ru_maxrss es monótono no decreciente.
      3. None — típicamente Windows sin psutil instalado.
    """
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        return lambda: proc.memory_info().rss
    except ImportError:
        pass

    try:
        import resource
        divisor = 1 if sys.platform == "darwin" else 1024
        return lambda: resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * divisor
    except ImportError:
        return None


class _MemorySampler:
    """Hilo en background que muestrea RSS cada `interval` segundos y expone
    el pico observado vía peek().

    No se "resetea" entre fases: el código que lo usa llama a peek() en cada
    frontera de fase y calcula deltas (peek_después - peek_antes). Esto
    funciona tanto con RSS real (psutil, casi monótono para este tipo de
    carga) como con un pico acumulado (ru_maxrss, monótono por definición).
    """

    def __init__(self, interval: float = 0.05) -> None:
        self._interval = interval
        self._reader = _make_reader()
        self._peak = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def available(self) -> bool:
        return self._reader is not None

    def start(self) -> None:
        if self._reader is None:
            return
        self._peak = self._reader()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                v = self._reader()
                with self._lock:
                    if v > self._peak:
                        self._peak = v
            except Exception:
                pass
            time.sleep(self._interval)

    def peek(self) -> int:
        with self._lock:
            return self._peak

    def stop(self) -> int:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        return self.peek()


# ───────────────────────────────────────────────────────────────────────────
# Resultado de un día (estructura compartida Controlador → Vista)
# ───────────────────────────────────────────────────────────────────────────

@dataclass
class DayResult:
    day: date
    ticks_day: int
    ticks_cumulative: int
    download_seconds: float
    download_rss_delta: int
    finalize_seconds: float
    finalize_rss_delta: int
    canonical_bytes: int


# ───────────────────────────────────────────────────────────────────────────
# CONTROLADOR
# ───────────────────────────────────────────────────────────────────────────

class TickWeekController:
    """Descarga `days` días de tick data para `symbol` y los escribe en
    Parquet, llamando a finalize() AL FINAL DE CADA DÍA — simula una corrida
    diaria del orchestrator real.

    Reutiliza exactamente los mismos componentes que orchestrator.py:
      DukascopyClient.download_chunk()   → bytes .bi5 por hora
      Bi5Decoder.decode(raw_prices=True) → DataFrame int32/int64
      ParquetWriter.write() / .finalize() → chunk + merge mensual

    No imprime nada: produce una lista de DayResult que consume la Vista.
    """

    def __init__(
        self,
        symbol: str,
        decimal_factor: int,
        start: date,
        days: int,
        output_path: Path,
    ) -> None:
        self.symbol = symbol.upper()
        self.decimal_factor = decimal_factor
        self.start = start
        self.days = days
        self.output_path = Path(output_path)

        self._client = DukascopyClient(max_retries=_cfg.MAX_RETRIES)
        self._decoder = Bi5Decoder()
        self._writer = ParquetWriter(self.output_path)
        self._failures = FailureLogger(self.output_path)

    def close(self) -> None:
        self._client.close()

    # ── Un día: 24 descargas horarias → decode → write ─────────────────────

    def _download_day(self, day: date) -> int:
        total = 0
        for hour in range(24):
            hour_dt = datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
            try:
                raw = self._client.download_chunk(self.symbol, "tick", hour_dt)
                if raw is None:
                    continue

                df = self._decoder.decode(
                    raw, "tick", self.decimal_factor, hour_dt, raw_prices=True
                )
                del raw
                if df.empty:
                    continue

                df.sort_values("timestamp", inplace=True)
                self._writer.write(self.symbol, "tick", df, decimal_factor=self.decimal_factor)
                total += len(df)
                del df

            except ChunkDownloadError as exc:
                self._failures.log(self.symbol, "tick", hour_dt, str(exc))
            except Exception as exc:
                self._failures.log(self.symbol, "tick", hour_dt, f"UNEXPECTED: {exc}")

        return total

    def _canonical_size(self, day: date) -> int:
        ref = datetime(day.year, day.month, day.day, tzinfo=UTC)
        path = self._writer.parquet_path(self.symbol, "tick", ref)
        return path.stat().st_size if path.exists() else 0

    # ── Loop principal ──────────────────────────────────────────────────

    def run(self) -> list[DayResult]:
        sampler = _MemorySampler()
        sampler.start()

        results: list[DayResult] = []
        cumulative = 0
        prev_peak = sampler.peek()

        try:
            for offset in range(self.days):
                day = self.start + timedelta(days=offset)

                t0 = time.perf_counter()
                ticks_today = self._download_day(day)
                download_seconds = time.perf_counter() - t0
                peak_after_download = sampler.peek()
                download_delta = max(0, peak_after_download - prev_peak)
                prev_peak = peak_after_download

                cumulative += ticks_today

                t1 = time.perf_counter()
                self._writer.finalize(self.symbol, "tick")
                finalize_seconds = time.perf_counter() - t1
                peak_after_finalize = sampler.peek()
                finalize_delta = max(0, peak_after_finalize - prev_peak)
                prev_peak = peak_after_finalize

                results.append(DayResult(
                    day=day,
                    ticks_day=ticks_today,
                    ticks_cumulative=cumulative,
                    download_seconds=download_seconds,
                    download_rss_delta=download_delta,
                    finalize_seconds=finalize_seconds,
                    finalize_rss_delta=finalize_delta,
                    canonical_bytes=self._canonical_size(day),
                ))
        finally:
            sampler.stop()
            self.close()

        return results


# ───────────────────────────────────────────────────────────────────────────
# VISTA
# ───────────────────────────────────────────────────────────────────────────

def _fmt_bytes(n: float) -> str:
    if n <= 0:
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB")
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    return f"{n:,.0f} B" if i == 0 else f"{n:,.1f} {units[i]}"


class ConsoleView:
    """Imprime el progreso día a día y el veredicto final en consola."""

    def __init__(self, symbol: str, output_path: Path, sampler_available: bool) -> None:
        self.symbol = symbol
        self.output_path = output_path
        self.sampler_available = sampler_available

    def header(self, start: date, days: int) -> None:
        end = start + timedelta(days=days - 1)
        print(f"=== Profiling RAM -- tick {self.symbol}, {days} dias desde {start.isoformat()} ===")
        print(f"Salida: {self.output_path}")

        if end.year != start.year or end.month != start.month:
            print(
                f"AVISO: el rango {start.isoformat()}..{end.isoformat()} cruza un "
                f"limite de mes; el canonico de tick se partira en dos archivos "
                f"y el veredicto dia-a-dia no sera comparable de forma directa."
            )

        if not self.sampler_available:
            print(
                "AVISO: no se pudo medir RSS (instala psutil para medicion "
                "precisa, especialmente en Windows: pip install psutil). "
                "Se mostraran tiempos pero la memoria aparecera como N/D."
            )
        print()

    def _mem(self, n: int) -> str:
        return _fmt_bytes(n) if self.sampler_available else "N/D"

    def row(self, r: DayResult) -> None:
        print(
            f"{r.day.isoformat()}  "
            f"ticks_dia={r.ticks_day:>8,}  acum={r.ticks_cumulative:>9,}  "
            f"| descarga {r.download_seconds:6.2f}s  drss {self._mem(r.download_rss_delta):>10}  "
            f"| finalize {r.finalize_seconds:6.2f}s  drss {self._mem(r.finalize_rss_delta):>10}  "
            f"| canonico {_fmt_bytes(r.canonical_bytes):>10}"
        )

    def summary(self, results: list[DayResult]) -> None:
        if not results:
            print("\nSin resultados (0 dias).")
            return

        n = len(results)
        first, last = results[0], results[-1]

        print("\n=== Veredicto ===")

        if first.finalize_seconds > 1e-3:
            time_ratio: float | None = last.finalize_seconds / first.finalize_seconds
            ratio_str = f"  (x{time_ratio:.1f})"
        else:
            time_ratio = None
            ratio_str = ""

        print(
            f"finalize() tiempo -- dia 1: {first.finalize_seconds:.3f}s  ->  "
            f"dia {n}: {last.finalize_seconds:.3f}s{ratio_str}"
        )

        if self.sampler_available:
            print(
                f"finalize() drss   -- dia 1: {_fmt_bytes(first.finalize_rss_delta)}  ->  "
                f"dia {n}: {_fmt_bytes(last.finalize_rss_delta)}"
            )

        print(
            f"canonico (tamano) -- dia 1: {_fmt_bytes(first.canonical_bytes)}  ->  "
            f"dia {n}: {_fmt_bytes(last.canonical_bytes)}"
        )

        if time_ratio is not None and n > 1:
            expected_linear = n  # si el costo fuera ~O(d), ratio esperado ~= n
            if time_ratio >= expected_linear * 0.6:
                print(
                    f"\n-> El costo de finalize() crece aprox. proporcional al "
                    f"tamano acumulado del canonico (ratio observado x{time_ratio:.1f} "
                    f"sobre {n} dias, esperado ~x{n} si fuera lineal). El patron "
                    f"O(d^2) descrito es real: cada corrida diaria relee y "
                    f"reescribe TODO el canonico del mes."
                )
            else:
                print(
                    f"\n-> El costo de finalize() se mantiene relativamente plano "
                    f"(ratio x{time_ratio:.1f} sobre {n} dias, esperado ~x{n} si "
                    f"fuera lineal). En este rango de volumen no es un problema."
                )

        print(
            f"\nNota: este experimento cubre {n} dias. Para extrapolar al final "
            f"de un mes (~30 dias), escala el drss y el tiempo del dia {n} por "
            f"~{30 / n:.1f}x (extrapolacion lineal, aproximada)."
        )


# ───────────────────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────────────────

def _default_start() -> date:
    """1er dia del mes anterior.

    Garantiza que start..start+6 nunca cruce un limite de mes (todos los
    meses tienen >= 28 dias), evitando que el experimento se reparta entre
    dos archivos canonicos distintos por defecto.
    """
    first_of_this_month = date.today().replace(day=1)
    last_month_last_day = first_of_this_month - timedelta(days=1)
    return last_month_last_day.replace(day=1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Profiling de RAM/tiempo para descarga + finalize de tick data (Parquet).",
    )
    parser.add_argument("--symbol", "-s", default="EURUSD", metavar="SYMBOL")
    parser.add_argument("--decimal-factor", "-d", type=int, default=100_000, metavar="N")
    parser.add_argument(
        "--start", type=lambda s: date.fromisoformat(s), default=None, metavar="YYYY-MM-DD",
        help="Dia inicial (default: dia 1 del mes anterior, evita cruzar mes).",
    )
    parser.add_argument("--days", type=int, default=7, metavar="N")
    parser.add_argument(
        "--ruta", "-r", default=None, metavar="PATH",
        help="Directorio de salida (default: {BASE_PATH}/_ram_profile).",
    )
    args = parser.parse_args()

    start = args.start or _default_start()
    output_path = Path(args.ruta) if args.ruta else _cfg.BASE_PATH / "_ram_profile"
    output_path.mkdir(parents=True, exist_ok=True)

    sampler_available = _make_reader() is not None
    view = ConsoleView(
        symbol=args.symbol.upper(), output_path=output_path, sampler_available=sampler_available
    )
    view.header(start, args.days)

    controller = TickWeekController(
        symbol=args.symbol,
        decimal_factor=args.decimal_factor,
        start=start,
        days=args.days,
        output_path=output_path,
    )

    results = controller.run()
    for r in results:
        view.row(r)
    view.summary(results)


if __name__ == "__main__":
    main()
