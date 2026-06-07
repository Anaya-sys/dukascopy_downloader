"""
orchestrator.py

Central coordinator. Builds the full task list, executes downloads in parallel,
decodes bi5 data, resamples where needed, writes CSVs, and updates checkpoints.

Task granularity:
  tick  → (symbol, "tick", date)    → 24 hourly .bi5 files per day
  m15   → (symbol, "m15", date)     → 1 daily .bi5 file  (1-min data → resample 15 min)
  h1    → (symbol, "h1", date)      → 1 monthly .bi5 file (first day of month)
  h4    → (symbol, "h4", date)      → 1 monthly .bi5 file (first day of month → resample 4h)

# ── Mapping interno de descarga ───────────────────────────────────────────
  Los timeframes m15 y h4 descargan datos de resolución inferior y luego
  los remuestrean.  Para que la URL y el decoder sean coherentes se usa un
  mapa explícito:
      m15  descarga "m1"  → resample a 15 min
      h4   descarga "h1"  → resample a 4 h
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List
import threading
import pandas as pd
from tqdm import tqdm

from bi5_decoder import Bi5Decoder
from checkpoint_manager import CheckpointManager
from csv_writer import CsvWriter
from dukascopy_client import ChunkDownloadError, DukascopyClient
from failure_logger import FailureLogger
from github_scraper import GitHubScraper, Instrument
import gc

log = logging.getLogger(__name__)

UTC = timezone.utc

# ── Timeframe que se descarga realmente vs. el timeframe lógico ───────────
# m15 descarga datos m1 y luego hace resample; h4 descarga h1 y hace resample.
_DOWNLOAD_TF: dict[str, str] = {
    "tick": "tick",
    "m1":   "m1",
    "m15":  "m1",   # FIX BUG 4: descarga m1, no m15
    "h1":   "h1",
    "h4":   "h1",   # FIX BUG 5: descarga h1, no h4
}

# ── Resample helpers ──────────────────────────────────────────────────────

def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample a 1-min or 1-hour OHLCV DataFrame to a coarser timeframe.

    ``closed='left'`` y ``label='left'`` garantizan que cada bucket se
    etiquete con su tiempo de *apertura*, evitando look-ahead bias.
    """
    if df.empty:
        return df
    df2 = df.copy()
    df2["timestamp"] = pd.to_datetime(df2["timestamp"])
    df2 = df2.set_index("timestamp").sort_index()
    rs = df2.resample(rule, closed="left", label="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna(subset=["open"])

    rs = rs.dropna(subset=["open"]).reset_index()
    rs["timestamp"] = rs["timestamp"].dt.tz_convert(None).dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
    return rs[["timestamp", "open", "high", "low", "close", "volume"]]


_RESAMPLE_RULE: dict[str, str] = {"m15": "15min", "h4": "4h"}

# ── Date-range generators ─────────────────────────────────────────────────

def _daily_dates(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _monthly_first_days(start: date, end: date):
    """Yield the 1st of each month from start's month to end's month.

    FIX BUG 1 (parcial): si start ya es el primero del mes, empieza ahí.
    Si start es mitad de mes, avanzamos al primer día del *siguiente* mes
    para no generar una tarea duplicada del mes ya checkpointeado.
    """
    if start.day == 1:
        d = date(start.year, start.month, 1)
    else:
        # Avanzar al primer día del mes siguiente
        if start.month == 12:
            d = date(start.year + 1, 1, 1)
        else:
            d = date(start.year, start.month + 1, 1)

    while d <= end:
        yield d
        if d.month == 12:
            d = date(d.year + 1, 1, 1)
        else:
            d = date(d.year, d.month + 1, 1)


# ── Task dataclass ────────────────────────────────────────────────────────

@dataclass
class Task:
    symbol: str
    timeframe: str
    dt: date
    decimal_factor: int


# ── Orchestrator ─────────────────────────────────────────────────────────

class DownloadOrchestrator:
    def __init__(self, output_path: Path, max_workers: int = 8, max_retries: int = 3) -> None:
        self._output      = Path(output_path)
        self._workers     = max_workers
        self._client      = DukascopyClient(max_retries=max_retries)
        self._decoder     = Bi5Decoder()
        self._csv         = CsvWriter(self._output)
        self._checkpoint  = CheckpointManager(self._output)
        self._failures    = FailureLogger(self._output)
        self._scraper     = GitHubScraper()
        self._decode_sem = threading.BoundedSemaphore(3)
        self._checkpoint_buf: list[tuple] = []
        self._checkpoint_buf_lock = threading.Lock()
        self._CHECKPOINT_BATCH = 15

    # ── Task builder ──────────────────────────────────────────────────────

    def _build_tasks(self, instruments: List[Instrument], timeframes: list) -> List[Task]:
        today = datetime.now(timezone.utc).date()
        tasks: List[Task] = []

        for instr in instruments:
            for tf in timeframes:
                primitive_tf = _DOWNLOAD_TF.get(tf, tf)

                if primitive_tf not in instr.start_dates:
                    continue

                last = self._checkpoint.get_last_date(instr.symbol, tf)
                start = (last + timedelta(days=1)) if last else instr.start_dates[primitive_tf]

                if start > today:
                    continue

                if tf in ("h1", "h4"):
                    for d in _monthly_first_days(start, today):
                        tasks.append(Task(instr.symbol, tf, d, instr.decimal_factor))
                else:
                    for d in _daily_dates(start, today):
                        tasks.append(Task(instr.symbol, tf, d, instr.decimal_factor))

        return tasks
    
    def _flush_checkpoints(self, force=False):
       with self._checkpoint_buf_lock:
            if not self._checkpoint_buf:
               return
            if not force and len(self._checkpoint_buf) < self._CHECKPOINT_BATCH:
               return
            for sym, tf, d in self._checkpoint_buf:
               self._checkpoint.update(sym, tf, d)
               self._checkpoint_buf.clear()
    # ── Single task executor ──────────────────────────────────────────────

    def _execute_task(self, task: Task) -> None:
        symbol, tf, dt, factor = task.symbol, task.timeframe, task.dt, task.decimal_factor
        try:
            if tf == "tick":
                self._execute_tick_day(symbol, dt, factor)
            elif tf in ("m15", "m1"):
                self._execute_ohlcv_day(symbol, tf, dt, factor)
            elif tf in ("h1", "h4"):
                self._execute_ohlcv_month(symbol, tf, dt, factor)
            else:
                log.warning("Timeframe desconocido en _execute_task: %s", tf)
                return

            # ── Checkpoint ─────────────────────────────────────────────
            # FIX BUG 1: para meses completos (end_of_month < hoy)
            # guardamos el último día del mes.  Para el mes en curso NO
            # guardamos checkpoint, así la próxima ejecución vuelve a
            # descargar el mes parcial para obtener nuevas velas.
            if tf in ("h1", "h4"):
                if dt.month == 12:
                    last_of_month = date(dt.year + 1, 1, 1) - timedelta(days=1)
                else:
                    last_of_month = date(dt.year, dt.month + 1, 1) - timedelta(days=1)

                if last_of_month < date.today():
                    # Mes completamente cerrado: checkpoint definitivo
                    self._checkpoint.update(symbol, tf, last_of_month)
                # else: mes en curso → sin checkpoint; se re-intentará en
                # la próxima ejecución para obtener las velas más recientes
            else:
                if dt < date.today():
                   with self._checkpoint_buf_lock:
                      self._checkpoint_buf.append((symbol, tf, last_of_month))
                      self._flush_checkpoints()

        except ChunkDownloadError as exc:
            self._failures.log(symbol, tf, dt, str(exc))
        except Exception as exc:
            log.error("Error inesperado %s/%s/%s: %s", symbol, tf, dt, exc, exc_info=True)
            self._failures.log(symbol, tf, dt, f"UNEXPECTED: {exc}")

    def _execute_tick_day(self, symbol: str, dt: date, factor: int) -> None:
        """Descarga, decodifica y escribe ticks hora por hora para evitar picos de memoria."""
        for hour in range(24):
            hour_dt = datetime(dt.year, dt.month, dt.day, hour, tzinfo=UTC)
            raw = self._client.download_chunk(symbol, "tick", hour_dt)
            
            if raw is None:
                continue
                
            with self._decode_sem:
                 df = self._decoder.decode(raw, "tick", factor, hour_dt)
                 del raw
            if not df.empty:
                df.sort_values("timestamp", inplace=True)
                self._csv.write(symbol, "tick", df)
                del df

    def _execute_ohlcv_day(self, symbol: str, tf: str, dt: date, factor: int) -> None:
        """Descarga velas diarias (m1 o m15) → resamplea si aplica → escribe."""
        base_dt = datetime(dt.year, dt.month, dt.day, tzinfo=UTC)
        download_tf = _DOWNLOAD_TF[tf]
        raw = self._client.download_chunk(symbol, download_tf, dt)

        if raw is None:
            return
        
        with self._decode_sem:   # ← max 3 decodificaciones simultáneas
             df = self._decoder.decode(raw, download_tf, factor, base_dt)
             del raw
             if df.empty:
                return
        rule = _RESAMPLE_RULE.get(tf)
        if rule:
            df = _resample_ohlcv(df, rule)
        self._csv.write(symbol, tf, df)
        del df

    def _execute_ohlcv_month(self, symbol: str, tf: str, dt: date, factor: int) -> None:
        """Descarga velas mensuales (h1 o h4) → resamplea si aplica → escribe."""
        base_dt = datetime(dt.year, dt.month, 1, tzinfo=UTC)

        # FIX BUG 5: usar el timeframe de descarga real (h1 para h4)
        download_tf = _DOWNLOAD_TF[tf]
        raw = self._client.download_chunk(symbol, download_tf, dt)
        if raw is None:
            return
        df = self._decoder.decode(raw, download_tf, factor, base_dt)
        if df.empty:
            return

        rule = _RESAMPLE_RULE.get(tf)
        if rule:
            df = _resample_ohlcv(df, rule)
        else:
            df.sort_values("timestamp", inplace=True)

        self._csv.write(symbol, tf, df)
        del df
        del raw
        gc.collect()

    # ── Main entry point ──────────────────────────────────────────────────

    def run(self, timeframes: list | None = None) -> None:
        from config import TIMEFRAMES
        if timeframes is None:
            timeframes = TIMEFRAMES

        print("Obteniendo lista de instrumentos desde GitHub…")
        instruments = self._scraper.scrape()
        print(f"  → {len(instruments)} instrumentos encontrados.")

        print("Construyendo lista de tareas…")
        tasks = self._build_tasks(instruments, timeframes)
        print(f"  → {len(tasks):,} tareas en cola.")

        if not tasks:
            print("Sin datos que descargar. Todo está al día.")
            return

        print(f"Iniciando descarga con {self._workers} workers…\n")

        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = {pool.submit(self._execute_task, t): t for t in tasks}
            with tqdm(total=len(tasks), unit="task", dynamic_ncols=True) as pbar:
                for fut in as_completed(futures):
                    task = futures[fut]
                    try:
                        fut.result()
                    except Exception as exc:
                        log.error(
                            "Task %s/%s/%s lanzó: %s",
                            task.symbol, task.timeframe, task.dt, exc,
                        )
                    pbar.set_postfix_str(f"{task.symbol}/{task.timeframe}", refresh=False)
                    pbar.update(1)

        print("\nDescarga completada.")

        ohlcv_targets = {
            (t.symbol, t.timeframe) for t in tasks if t.timeframe != "tick"
        }
        
        if ohlcv_targets:
            print("Consolidando, ordenando y limpiando archivos OHLCV (Post-Deduplicación)...")
            for sym, tf in tqdm(ohlcv_targets, unit="file", dynamic_ncols=True):
                self._csv.finalize(sym, tf)
            print("Archivos OHLCV consolidados perfectamente.")

        fail_path = self._output / "failed.log"
        if fail_path.exists() and fail_path.stat().st_size > 0:
            print(f"  ⚠  Algunos chunks fallaron. Revisa: {fail_path}")
            self._flush_checkpoints(force=True)