"""
test_benchmark_pipeline.py
==========================
Benchmark honesto: pipeline propio (16 workers + Parquet ZSTD) vs.
descarga vanilla (secuencial + CSV plano).

El cuello de botella real es la RED, no el CPU.  Este test lo demuestra
midiendo ambas dimensiones por separado y de forma transparente:

  ┌─ Dimensión 1: Red ──────────────────────────────────────────────────┐
  │  Misma carga de trabajo, mismo número de bytes a descargar.         │
  │  Pipeline: 16 workers en paralelo (ThreadPoolExecutor).             │
  │  Vanilla:  peticiones secuenciales con requests (1 a la vez).       │
  │  Resultado esperado: speedup ≈ número efectivo de workers           │
  │  (limitado por ancho de banda y latencia del servidor).             │
  └─────────────────────────────────────────────────────────────────────┘

  ┌─ Dimensión 2: Procesamiento post-descarga ──────────────────────────┐
  │  Los bytes ya están en memoria (descargados previamente).           │
  │  Pipeline: Bi5Decoder(raw_prices=True) → ParquetWriter ZSTD lv1.   │
  │  Vanilla:  struct.unpack loop → pandas DataFrame → to_csv().        │
  │  Resultado: cuánto más rápido escribe y cuánto menos ocupa en disco.│
  └─────────────────────────────────────────────────────────────────────┘

Ventana de test
---------------
  Semana: 2024-01-08 → 2024-01-14  (lunes a domingo, datos cerrados).
  Símbolos: EURUSD, USDJPY.

  Fase A — OHLCV (m15, h1, h4):
    m15: 7 archivos diarios m1 × 2 símbolos  = 14 URLs
    h1:  1 archivo mensual  × 2 símbolos     =  2 URLs
    h4:  mismo archivo h1   × 2 símbolos     =  2 URLs  (resample)
    Total OHLCV: 18 URLs

  Fase B — Ticks:
    24 horas × 7 días × 2 símbolos           = 336 URLs
    Aquí es donde el paralelismo aplasta: 336 peticiones secuenciales
    vs. 16 en vuelo simultáneo.

Métricas capturadas
-------------------
  - Tiempo de DESCARGA puro (sin procesamiento)
  - Tiempo de PROCESAMIENTO puro (con bytes ya en RAM)
  - Tiempo TOTAL (descarga + procesamiento + escritura + finalize)
  - Pico de RSS en MB (hilo daemon, muestreo cada 10 ms)
  - Throughput: filas/s y MB/s
  - Speedup por dimensión y global
  - Tamaño en disco: CSV vs Parquet

Validaciones (mismas que test_live_pipeline)
--------------------------------------------
  ✓ Schema exacto de cada Parquet producido
  ✓ Metadata completa (decimal_factor, symbol, timeframe, source)
  ✓ Timestamps int64 ms-epoch UTC, ordenados, en rango 2024
  ✓ Precios int32, positivos, en rango de mercado
  ✓ Volúmenes float32 no negativos
  ✓ Sin residuos de chunks en .chunks/

Ejecución
---------
    python test_benchmark_pipeline.py

No requiere pytest.  Salida 0 si todo pasa, 1 si algo falla.
Requiere conexión a Internet (datos reales de Dukascopy).
"""

from __future__ import annotations

import csv
import datetime as dt
import gc
import io
import lzma
import struct
import sys
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import requests

sys.path.insert(0, str(Path(__file__).parent))

from bi5_decoder import Bi5Decoder
from dukascopy_client import DukascopyClient, ChunkDownloadError
from parquet_writer import ParquetWriter
from orchestrator import _resample_ohlcv

UTC  = dt.timezone.utc
PROC = psutil.Process()

# ── Ventana ────────────────────────────────────────────────────────────────
WEEK_START = dt.date(2023, 1, 1)
WEEK_END   = dt.date(2024, 1, 31)
WEEK_DAYS  = [WEEK_START + dt.timedelta(days=i) for i in range(31)]
TEST_MONTH    = dt.date(2024, 1, 1)
TEST_MONTH_DT = dt.datetime(2024, 1, 1, tzinfo=UTC)

SYMBOLS     = ["EURUSD", "USDJPY"]
FACTOR      = {"EURUSD": 100_000, "USDJPY": 1_000}
_PRICE_RANGES = {"EURUSD": (100_000, 115_000), "USDJPY": (100_000, 160_000)}

MAX_WORKERS = 16

# ── Schemas PyArrow ────────────────────────────────────────────────────────
TICK_SCHEMA = pa.schema([
    pa.field("timestamp", pa.int64()),
    pa.field("ask",       pa.int32()),
    pa.field("bid",       pa.int32()),
    pa.field("ask_vol",   pa.float32()),
    pa.field("bid_vol",   pa.float32()),
])
OHLCV_SCHEMA = pa.schema([
    pa.field("timestamp", pa.int64()),
    pa.field("open",      pa.int32()),
    pa.field("high",      pa.int32()),
    pa.field("low",       pa.int32()),
    pa.field("close",     pa.int32()),
    pa.field("volume",    pa.float32()),
])

# ── URLs Dukascopy ─────────────────────────────────────────────────────────
_ROOT = "https://datafeed.dukascopy.com/datafeed"

def _url_tick(sym: str, d: dt.date, h: int) -> str:
    return f"{_ROOT}/{sym}/{d.year}/{d.month-1:02d}/{d.day:02d}/{h:02d}h_ticks.bi5"

def _url_m1(sym: str, d: dt.date) -> str:
    return f"{_ROOT}/{sym}/{d.year}/{d.month-1:02d}/{d.day:02d}/BID_candles_min_1.bi5"

def _url_h1(sym: str, d: dt.date) -> str:
    return f"{_ROOT}/{sym}/{d.year}/{d.month-1:02d}/BID_candles_hour_1.bi5"


# ── Muestreador de RAM ─────────────────────────────────────────────────────
class RamSampler:
    def __init__(self):
        self._stop = threading.Event()
        self._peak = [PROC.memory_info().rss]
        self._t    = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._peak[0] = PROC.memory_info().rss
        self._t.start()
        return self

    def _run(self):
        while not self._stop.is_set():
            rss = PROC.memory_info().rss
            if rss > self._peak[0]:
                self._peak[0] = rss
            time.sleep(0.01)

    def stop(self) -> float:
        self._stop.set()
        self._t.join()
        return self._peak[0] / 1024 / 1024


# ── Contenedor de resultados ───────────────────────────────────────────────
class Result:
    def __init__(self, label: str):
        self.label          = label
        self.t_download_s   = 0.0   # tiempo puro de red
        self.t_process_s    = 0.0   # tiempo puro de decode + write
        self.t_total_s      = 0.0   # wall-clock completo
        self.rows           = 0
        self.bytes_dl       = 0
        self.peak_ram_mb    = 0.0
        self.disk_mb        = 0.0
        self.errors         = 0

    @property
    def rows_per_sec(self):
        return self.rows / self.t_total_s if self.t_total_s > 0 else 0.0

    @property
    def mb_per_sec_dl(self):
        return (self.bytes_dl / 1024**2) / self.t_download_s if self.t_download_s > 0 else 0.0


# ═══════════════════════════════════════════════════════════════════════════
# DESCARGA COMÚN: obtener todos los bytes crudos de una lista de URLs
# Se usa para alimentar AMBOS modos con los mismos bytes (equidad perfecta).
# ═══════════════════════════════════════════════════════════════════════════

def download_all_parallel(urls: list[str], workers: int = MAX_WORKERS) \
        -> tuple[dict[str, bytes | None], float, int]:
    """
    Descarga todas las URLs en paralelo con DukascopyClient.
    Retorna (cache: url→bytes|None, tiempo_descarga_s, bytes_totales).
    """
    cache: dict[str, bytes | None] = {}
    lock  = threading.Lock()
    total_bytes = [0]

    # Reutilizamos DukascopyClient que ya tiene HTTP/2 + pool + retry
    client = DukascopyClient(max_retries=3)

    def _fetch(url: str) -> tuple[str, bytes | None]:
        try:
            resp = client._client.get(url, timeout=30)
            if resp.status_code == 404:
                return url, None
            resp.raise_for_status()
            return url, resp.content
        except Exception:
            return url, None

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for url, data in pool.map(_fetch, urls):
            cache[url] = data
            if data:
                with lock:
                    total_bytes[0] += len(data)
    elapsed = time.perf_counter() - t0

    client.close()
    return cache, elapsed, total_bytes[0]


def download_all_sequential(urls: list[str]) \
        -> tuple[dict[str, bytes | None], float, int]:
    """
    Descarga todas las URLs en secuencia con requests (vanilla).
    Retorna (cache: url→bytes|None, tiempo_descarga_s, bytes_totales).
    """
    cache  = {}
    total  = 0
    sess   = requests.Session()
    sess.headers["User-Agent"] = "benchmark-vanilla/1.0"

    t0 = time.perf_counter()
    for url in urls:
        try:
            r = sess.get(url, timeout=30)
            if r.status_code == 404:
                cache[url] = None
            else:
                r.raise_for_status()
                cache[url] = r.content
                total += len(r.content)
        except Exception:
            cache[url] = None
    elapsed = time.perf_counter() - t0

    sess.close()
    return cache, elapsed, total


# ═══════════════════════════════════════════════════════════════════════════
# PROCESAMIENTO VANILLA: struct-unpack → pandas → CSV
# ═══════════════════════════════════════════════════════════════════════════

_TICK_FMT  = ">iiiff"
_TICK_SZ   = struct.calcsize(_TICK_FMT)
_OHLCV_FMT = ">iiiiif"
_OHLCV_SZ  = struct.calcsize(_OHLCV_FMT)


def _vanilla_decode_tick(raw: bytes, base_epoch_ms: int, factor: int) -> pd.DataFrame:
    """struct.unpack → DataFrame con floats (enfoque naive)."""
    data = lzma.decompress(raw)
    n    = len(data) // _TICK_SZ
    rows = []
    for i in range(n):
        rec = struct.unpack_from(_TICK_FMT, data, i * _TICK_SZ)
        rows.append({
            "timestamp": base_epoch_ms + rec[0],
            "ask":       rec[1] / factor,
            "bid":       rec[2] / factor,
            "ask_vol":   rec[3],
            "bid_vol":   rec[4],
        })
    return pd.DataFrame(rows)


def _vanilla_decode_ohlcv(raw: bytes, base_epoch_ms: int, factor: int,
                           is_seconds: bool = True) -> pd.DataFrame:
    """struct.unpack → DataFrame con floats (enfoque naive)."""
    data = lzma.decompress(raw)
    n    = len(data) // _OHLCV_SZ
    rows = []
    mult = 1000 if is_seconds else 1
    for i in range(n):
        rec = struct.unpack_from(_OHLCV_FMT, data, i * _OHLCV_SZ)
        rows.append({
            "timestamp": base_epoch_ms + rec[0] * mult,
            "open":      rec[1] / factor,
            "high":      rec[2] / factor,
            "low":       rec[3] / factor,
            "close":     rec[4] / factor,
            "volume":    rec[5],
        })
    return pd.DataFrame(rows)


def _vanilla_write_csv(df: pd.DataFrame, path: Path) -> None:
    """pandas.to_csv sin compresión (baseline vanilla de escritura)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    df.to_csv(path, mode="a", header=write_header, index=False)


# ═══════════════════════════════════════════════════════════════════════════
# PROCESAMIENTO PIPELINE: Bi5Decoder(raw_prices=True) → ParquetWriter ZSTD
# ═══════════════════════════════════════════════════════════════════════════

def _pipeline_process_tick(
    cache: dict,
    urls_by_meta: list[tuple[str, str, dt.datetime]],
    writer: ParquetWriter,
    decoder: Bi5Decoder,
) -> tuple[int, float]:
    """Procesa todos los chunks tick del cache con el pipeline.
    Retorna (filas, segundos_procesamiento)."""
    rows_total = [0]
    lock       = threading.Lock()

    def _proc(url: str, sym: str, hour_dt: dt.datetime) -> None:
        raw = cache.get(url)
        if not raw:
            return
        factor = FACTOR[sym]
        df = decoder.decode(raw, "tick", factor, hour_dt, raw_prices=True)
        if df.empty:
            return
        df.sort_values("timestamp", inplace=True)
        writer.write(sym, "tick", df, decimal_factor=factor)
        with lock:
            rows_total[0] += len(df)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = [pool.submit(_proc, url, sym, hour_dt)
                for url, sym, hour_dt in urls_by_meta]
        for f in as_completed(futs):
            f.result()

    for sym in SYMBOLS:
        writer.finalize(sym, "tick")

    return rows_total[0], time.perf_counter() - t0


def _pipeline_process_ohlcv(
    cache: dict,
    tasks: list[dict],
    writer: ParquetWriter,
    decoder: Bi5Decoder,
) -> tuple[int, float]:
    """Procesa todos los chunks OHLCV del cache con el pipeline.
    Retorna (filas, segundos_procesamiento)."""
    rows_total = [0]
    lock       = threading.Lock()

    def _proc(task: dict) -> None:
        url  = task["url"]
        sym  = task["sym"]
        tf   = task["tf"]
        base = task["base_dt"]
        dl_tf = task["dl_tf"]
        raw  = cache.get(url)
        if not raw:
            return
        factor = FACTOR[sym]
        df = decoder.decode(raw, dl_tf, factor, base, raw_prices=True)
        if df.empty:
            return
        if tf in ("m15", "h4"):
            rule = "15min" if tf == "m15" else "4h"
            df = _resample_ohlcv(df, rule)
        if df.empty:
            return
        writer.write(sym, tf, df, decimal_factor=factor)
        with lock:
            rows_total[0] += len(df)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = [pool.submit(_proc, t) for t in tasks]
        for f in as_completed(futs):
            f.result()

    finalize_targets = {(t["sym"], t["tf"]) for t in tasks}
    for sym, tf in sorted(finalize_targets):
        writer.finalize(sym, tf)

    return rows_total[0], time.perf_counter() - t0


# ═══════════════════════════════════════════════════════════════════════════
# PROCESAMIENTO VANILLA POST-CACHE: struct → pandas → CSV
# ═══════════════════════════════════════════════════════════════════════════

def _vanilla_process_tick(
    cache: dict,
    urls_by_meta: list[tuple[str, str, dt.datetime]],
    csvdir: Path,
) -> tuple[int, float]:
    rows_total = 0
    t0 = time.perf_counter()
    for url, sym, hour_dt in urls_by_meta:
        raw = cache.get(url)
        if not raw:
            continue
        factor     = FACTOR[sym]
        base_ms    = int(hour_dt.timestamp() * 1000)
        df         = _vanilla_decode_tick(raw, base_ms, factor)
        if df.empty:
            continue
        df.sort_values("timestamp", inplace=True)
        path = csvdir / sym / "tick" / f"{sym}_tick_{hour_dt.year}_{hour_dt.month:02d}.csv"
        _vanilla_write_csv(df, path)
        rows_total += len(df)
    return rows_total, time.perf_counter() - t0


def _vanilla_process_ohlcv(
    cache: dict,
    tasks: list[dict],
    csvdir: Path,
) -> tuple[int, float]:
    rows_total = 0
    t0 = time.perf_counter()
    for task in tasks:
        url   = task["url"]
        sym   = task["sym"]
        tf    = task["tf"]
        base  = task["base_dt"]
        dl_tf = task["dl_tf"]
        raw   = cache.get(url)
        if not raw:
            continue
        factor  = FACTOR[sym]
        base_ms = int(base.timestamp() * 1000)
        df = _vanilla_decode_ohlcv(raw, base_ms, factor, is_seconds=(dl_tf == "h1"))
        if df.empty:
            continue
        if tf in ("m15", "h4"):
            # Resample naive con pandas (sin la lógica optimizada del pipeline)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            rule = "15min" if tf == "m15" else "4h"
            df = (
                df.set_index("timestamp").sort_index()
                .resample(rule, closed="left", label="left")
                .agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"})
                .dropna(subset=["open"]).reset_index()
            )
            df["timestamp"] = df["timestamp"].astype("int64") // 1_000_000
        _TF_FOLDER = {"m15":"15min","h1":"1h","h4":"4h"}
        folder = _TF_FOLDER[tf]
        path = csvdir / sym / folder / f"{sym}_{folder}.csv"
        _vanilla_write_csv(df, path)
        rows_total += len(df)
    return rows_total, time.perf_counter() - t0


# ═══════════════════════════════════════════════════════════════════════════
# VALIDACIONES
# ═══════════════════════════════════════════════════════════════════════════

_VAL_RESULTS: list[tuple[str, bool, str]] = []


def _vrecord(name: str, ok: bool, detail: str = "") -> None:
    _VAL_RESULTS.append((name, ok, detail))


def _validate_parquet(path: Path, sym: str, tf: str, factor: int,
                      price_range: tuple, exp_schema: pa.Schema,
                      ms_align: int | None = None) -> list[str]:
    errs = []
    if not path.exists():
        return [f"no encontrado: {path.name}"]

    table = pq.read_table(path)
    meta  = pq.read_schema(path).metadata or {}
    df    = table.to_pandas()

    # Schema
    actual = {f.name: f.type for f in table.schema}
    for col, etype in {f.name: f.type for f in exp_schema}.items():
        at = actual.get(col)
        if at is None:
            errs.append(f"columna faltante: {col}")
        elif at != etype:
            errs.append(f"{col}: {at} ≠ {etype}")
    extra = set(actual) - {f.name for f in exp_schema}
    if extra:
        errs.append(f"columnas extra: {extra}")

    # Metadata
    req = {b"decimal_factor": str(factor).encode(),
           b"symbol": sym.upper().encode(),
           b"timeframe": tf.encode(),
           b"source": b"dukascopy"}
    for k, ev in req.items():
        av = meta.get(k)
        if av is None:
            errs.append(f"metadata faltante: {k.decode()}")
        elif av != ev:
            errs.append(f"metadata {k.decode()}: {av!r} ≠ {ev!r}")

    if len(df) == 0:
        errs.append("0 filas")
        return errs

    ts = df["timestamp"]
    if str(ts.dtype) != "int64":
        errs.append(f"timestamp dtype={ts.dtype}")
    if not ts.is_monotonic_increasing:
        errs.append("timestamps no ordenados")
    ts_min_y = dt.datetime.fromtimestamp(int(ts.min())/1000, tz=UTC).year
    ts_max_y = dt.datetime.fromtimestamp(int(ts.max())/1000, tz=UTC).year
    if ts_min_y != 2024:
        errs.append(f"ts_min year={ts_min_y}")
    if ts_max_y != 2024:
        errs.append(f"ts_max year={ts_max_y}")
    if ms_align:
        bad = (ts % ms_align != 0).sum()
        if bad:
            errs.append(f"{bad} ts no alineados a {ms_align}ms")

    lo, hi = price_range
    pcols = ["ask","bid"] if tf == "tick" else ["open","high","low","close"]
    for col in pcols:
        if col not in df.columns:
            continue
        if str(df[col].dtype) != "int32":
            errs.append(f"{col} dtype={df[col].dtype}")
        pmin, pmax = int(df[col].min()), int(df[col].max())
        if pmin <= 0:
            errs.append(f"{col} min={pmin} ≤ 0")
        if pmin < lo or pmax > hi:
            errs.append(f"{col} fuera de rango [{lo},{hi}]: {pmin}–{pmax}")
    if tf == "tick" and "ask" in df.columns:
        neg = (df["ask"] < df["bid"]).sum()
        if neg:
            errs.append(f"{neg} filas ask<bid")

    vcols = ["ask_vol","bid_vol"] if tf == "tick" else ["volume"]
    for col in vcols:
        if col not in df.columns:
            continue
        if str(df[col].dtype) != "float32":
            errs.append(f"{col} dtype={df[col].dtype}")
        if (df[col] < 0).any():
            errs.append(f"{col} con negativos")

    return errs


def run_validations(ohlcv_dir: Path, tick_dir: Path) -> None:
    _TF_FOLDER = {"tick":"tick","m15":"15min","h1":"1h","h4":"4h"}
    MS_15MIN, MS_4H = 15*60*1000, 4*3600*1000

    for sym in SYMBOLS:
        factor = FACTOR[sym]
        pr     = _PRICE_RANGES[sym]

        for tf in ["m15", "h1", "h4"]:
            folder = _TF_FOLDER[tf]
            path   = ohlcv_dir / sym / folder / f"{sym}_{folder}.parquet"
            align  = MS_15MIN if tf == "m15" else (MS_4H if tf == "h4" else None)
            errs   = _validate_parquet(path, sym, tf, factor, pr, OHLCV_SCHEMA, align)
            ok     = len(errs) == 0
            _vrecord(f"val_{sym}_{tf}", ok,
                     f"OK — {path.name}" if ok else f"{errs[0]}")

            # sin residuos
            cd = ohlcv_dir / sym / folder / ".chunks"
            if cd.exists():
                res = list(cd.glob("*.parquet"))
                _vrecord(f"chunks_{sym}_{tf}", len(res)==0,
                         f"{len(res)} residuos" if res else "")
            else:
                _vrecord(f"chunks_{sym}_{tf}", True, ".chunks/ limpio")

        # tick
        tf     = "tick"
        folder = _TF_FOLDER[tf]
        path   = tick_dir / sym / folder / f"{sym}_{folder}_2024_01.parquet"
        errs   = _validate_parquet(path, sym, tf, factor, pr, TICK_SCHEMA)
        ok     = len(errs) == 0
        _vrecord(f"val_{sym}_tick", ok,
                 f"OK — {path.name}" if ok else f"{errs[0]}")
        cd = tick_dir / sym / folder / ".chunks"
        if cd.exists():
            res = list(cd.glob("*.parquet"))
            _vrecord(f"chunks_{sym}_tick", len(res)==0,
                     f"{len(res)} residuos" if res else "")
        else:
            _vrecord(f"chunks_{sym}_tick", True, ".chunks/ limpio")


# ═══════════════════════════════════════════════════════════════════════════
# REPORTE
# ═══════════════════════════════════════════════════════════════════════════

def _bar(v: float, mx: float, w: int = 28) -> str:
    if mx <= 0:
        return "░" * w
    n = int(round(v / mx * w))
    return "█" * n + "░" * (w - n)


def _ratio(a: float, b: float) -> str:
    if b <= 0:
        return "  N/A"
    r = a / b
    return f"{r:5.1f}×"


def print_dim_report(title: str, label_a: str, label_b: str,
                     t_a: float, t_b: float,
                     rows_a: int, rows_b: int,
                     ram_a: float, ram_b: float,
                     disk_a_mb: float = 0.0, disk_b_mb: float = 0.0,
                     urls: int = 0) -> None:
    speedup    = t_a / t_b if t_b > 0 else 0
    ram_saving = ram_a - ram_b
    mx_t       = max(t_a, t_b, 0.001)
    mx_r       = max(ram_a, ram_b, 0.001)

    print(f"\n  {'━'*61}")
    print(f"  {title}")
    print(f"  {'━'*61}")
    if urls:
        print(f"  URLs procesadas: {urls}")
    print(f"\n  {'Métrica':<26} {label_a:>12} {label_b:>12}  {'Ventaja':>8}")
    print(f"  {'-'*26} {'-'*12} {'-'*12}  {'-'*8}")
    print(f"  {'Tiempo (s)':<26} {t_a:>11.1f}s {t_b:>11.1f}s  {_ratio(t_a,t_b):>8}")
    print(f"  {'Filas procesadas':<26} {rows_a:>12,} {rows_b:>12,}")
    if t_a > 0 and t_b > 0:
        rps_a = rows_a / t_a
        rps_b = rows_b / t_b
        print(f"  {'Throughput (filas/s)':<26} {rps_a:>12,.0f} {rps_b:>12,.0f}  {_ratio(rps_b,rps_a):>8}")
    print(f"  {'Pico RAM (MB)':<26} {ram_a:>11.0f}  {ram_b:>11.0f}  {ram_saving:>+7.0f}MB")
    if disk_a_mb or disk_b_mb:
        print(f"  {'Disco (MB)':<26} {disk_a_mb:>12.1f} {disk_b_mb:>12.1f}")
        if disk_a_mb > 0 and disk_b_mb > 0:
            ratio_disk = disk_a_mb / disk_b_mb
            print(f"  {'  compresión Parquet':<26} {'':>12} {'':>12}  {ratio_disk:>5.1f}× menos")

    print(f"\n  Tiempo  {label_a:<8} {_bar(t_a,mx_t)}  {t_a:.1f}s")
    print(f"          {label_b:<8} {_bar(t_b,mx_t)}  {t_b:.1f}s")
    print(f"\n  RAM     {label_a:<8} {_bar(ram_a,mx_r)}  {ram_a:.0f}MB")
    print(f"          {label_b:<8} {_bar(ram_b,mx_r)}  {ram_b:.0f}MB")

    print(f"\n  ┌{'─'*55}┐")
    print(f"  │  Speedup de tiempo  : {speedup:>5.1f}× más rápido"
          + " " * (32 - len(f"{speedup:.1f}")) + "│")
    pct = abs(ram_saving) / ram_a * 100 if ram_a > 0 else 0
    sign = "-" if ram_saving > 0 else "+"
    print(f"  │  Ahorro de RAM      : {sign}{abs(ram_saving):.0f} MB ({pct:.0f}%)"
          + " " * max(0, 31 - len(f"{ram_saving:.0f}") - len(f"{pct:.0f}")) + "│")
    print(f"  └{'─'*55}┘")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main() -> int:
    print(f"\n{'═'*65}")
    print("  BENCHMARK: Pipeline (16 workers + Parquet) vs Vanilla (CSV)")
    print(f"{'═'*65}")
    print(f"  Ventana : {WEEK_START} → {WEEK_END}  (7 días)")
    print(f"  Símbolos: {', '.join(SYMBOLS)}")
    print(f"  Workers : {MAX_WORKERS}  (solo pipeline)")
    print(f"{'═'*65}\n")

    ohlcv_pipeline_dir = Path(tempfile.mkdtemp(prefix="bench_ohlcv_pipe_"))
    ohlcv_vanilla_dir  = Path(tempfile.mkdtemp(prefix="bench_ohlcv_van_"))
    tick_pipeline_dir  = Path(tempfile.mkdtemp(prefix="bench_tick_pipe_"))
    tick_vanilla_dir   = Path(tempfile.mkdtemp(prefix="bench_tick_van_"))

    # ── Construir listas de URLs ───────────────────────────────────────────
    ohlcv_tasks: list[dict] = []
    ohlcv_urls:  list[str]  = []
    for sym in SYMBOLS:
        for d in WEEK_DAYS:
            url = _url_m1(sym, d)
            ohlcv_tasks.append({"url": url, "sym": sym, "tf": "m15",
                                 "dl_tf": "m1",
                                 "base_dt": dt.datetime(d.year, d.month, d.day, tzinfo=UTC)})
            ohlcv_urls.append(url)
        for tf, dl_tf in [("h1","h1"), ("h4","h1")]:
            url = _url_h1(sym, TEST_MONTH)
            ohlcv_tasks.append({"url": url, "sym": sym, "tf": tf,
                                 "dl_tf": dl_tf, "base_dt": TEST_MONTH_DT})
            ohlcv_urls.append(url)

    tick_urls_meta: list[tuple[str, str, dt.datetime]] = []
    tick_urls: list[str] = []
    for sym in SYMBOLS:
        for d in WEEK_DAYS:
            for h in range(24):
                url = _url_tick(sym, d, h)
                hour_dt = dt.datetime(d.year, d.month, d.day, h, tzinfo=UTC)
                tick_urls_meta.append((url, sym, hour_dt))
                tick_urls.append(url)

    decoder = Bi5Decoder()

    # ══════════════════════════════════════════════════════════════════════
    # FASE A — OHLCV
    # ══════════════════════════════════════════════════════════════════════
    print("━"*65)
    print("  FASE A — OHLCV  (m15 · h1 · h4)")
    print("━"*65)

    # ── A.1 Descarga paralela (pipeline) ──────────────────────────────────
    print(f"\n  [A-1] Descargando OHLCV en PARALELO ({MAX_WORKERS} workers)...")
    gc.collect()
    ram_pipe_ohlcv_dl = RamSampler().start()
    ohlcv_cache_pipe, t_dl_pipe_ohlcv, bytes_pipe_ohlcv = \
        download_all_parallel(list(dict.fromkeys(ohlcv_urls)))
    ram_pipe_ohlcv_dl_peak = ram_pipe_ohlcv_dl.stop()
    n_hits = sum(1 for v in ohlcv_cache_pipe.values() if v)
    print(f"        → {n_hits}/{len(ohlcv_cache_pipe)} URLs con datos  "
          f"{bytes_pipe_ohlcv/1024:.0f}KB  {t_dl_pipe_ohlcv:.2f}s")

    # Mismos bytes para vanilla (equidad total)
    ohlcv_cache_van = ohlcv_cache_pipe

    # ── A.2 Descarga secuencial (vanilla) ─────────────────────────────────
    print(f"\n  [A-2] Descargando OHLCV en SECUENCIA (vanilla, requests)...")
    gc.collect()
    ram_van_ohlcv_dl = RamSampler().start()
    _, t_dl_van_ohlcv, _ = download_all_sequential(list(dict.fromkeys(ohlcv_urls)))
    ram_van_ohlcv_dl_peak = ram_van_ohlcv_dl.stop()
    print(f"        → {t_dl_van_ohlcv:.2f}s")

    # ── A.3 Procesamiento pipeline ─────────────────────────────────────────
    print(f"\n  [A-3] Procesando OHLCV con PIPELINE (decode raw → Parquet)...")
    gc.collect()
    writer_ohlcv = ParquetWriter(ohlcv_pipeline_dir)
    ram_pipe_ohlcv_proc = RamSampler().start()
    rows_pipe_ohlcv, t_proc_pipe_ohlcv = _pipeline_process_ohlcv(
        ohlcv_cache_pipe, ohlcv_tasks, writer_ohlcv, decoder)
    ram_pipe_ohlcv_proc_peak = ram_pipe_ohlcv_proc.stop()
    disk_pipe_ohlcv = sum(f.stat().st_size for f in ohlcv_pipeline_dir.rglob("*.parquet")) / 1024**2
    print(f"        → {rows_pipe_ohlcv:,} filas  {t_proc_pipe_ohlcv:.2f}s  {disk_pipe_ohlcv:.2f}MB Parquet")

    # ── A.4 Procesamiento vanilla ──────────────────────────────────────────
    print(f"\n  [A-4] Procesando OHLCV con VANILLA (struct → pandas → CSV)...")
    gc.collect()
    ram_van_ohlcv_proc = RamSampler().start()
    rows_van_ohlcv, t_proc_van_ohlcv = _vanilla_process_ohlcv(
        ohlcv_cache_van, ohlcv_tasks, ohlcv_vanilla_dir)
    ram_van_ohlcv_proc_peak = ram_van_ohlcv_proc.stop()
    disk_van_ohlcv = sum(f.stat().st_size for f in ohlcv_vanilla_dir.rglob("*.csv")) / 1024**2
    print(f"        → {rows_van_ohlcv:,} filas  {t_proc_van_ohlcv:.2f}s  {disk_van_ohlcv:.2f}MB CSV")

    # ── Reporte OHLCV ──────────────────────────────────────────────────────
    n_ohlcv_urls = len(set(ohlcv_urls))
    print_dim_report(
        "DIMENSIÓN 1 — RED OHLCV (paralelo vs secuencial)",
        "Vanilla", "Pipeline",
        t_dl_van_ohlcv, t_dl_pipe_ohlcv,
        0, 0,
        ram_van_ohlcv_dl_peak, ram_pipe_ohlcv_dl_peak,
        urls=n_ohlcv_urls,
    )
    print_dim_report(
        "DIMENSIÓN 2 — PROCESAMIENTO OHLCV (bytes ya en RAM)",
        "Vanilla", "Pipeline",
        t_proc_van_ohlcv, t_proc_pipe_ohlcv,
        rows_van_ohlcv, rows_pipe_ohlcv,
        ram_van_ohlcv_proc_peak, ram_pipe_ohlcv_proc_peak,
        disk_a_mb=disk_van_ohlcv, disk_b_mb=disk_pipe_ohlcv,
    )

    # ══════════════════════════════════════════════════════════════════════
    # FASE B — TICKS
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n\n{'━'*65}")
    print("  FASE B — TICKS  (24h × 7 días × 2 símbolos = 336 URLs)")
    print("━"*65)
    print("  Aquí se demuestra la mayor ventaja del paralelismo.")

    # ── B.1 Descarga paralela (pipeline) ──────────────────────────────────
    print(f"\n  [B-1] Descargando ticks en PARALELO ({MAX_WORKERS} workers)...")
    print("        (puede tomar 1-3 min dependiendo del ancho de banda)")
    gc.collect()
    ram_pipe_tick_dl = RamSampler().start()
    tick_cache_pipe, t_dl_pipe_tick, bytes_pipe_tick = \
        download_all_parallel(tick_urls)
    ram_pipe_tick_dl_peak = ram_pipe_tick_dl.stop()
    n_hits_tick = sum(1 for v in tick_cache_pipe.values() if v)
    print(f"        → {n_hits_tick}/{len(tick_cache_pipe)} URLs con datos  "
          f"{bytes_pipe_tick/1024**2:.1f}MB  {t_dl_pipe_tick:.1f}s")

    tick_cache_van = tick_cache_pipe   # mismos bytes → comparación justa

    # ── B.2 Descarga secuencial (vanilla) ─────────────────────────────────
    print(f"\n  [B-2] Descargando ticks en SECUENCIA (vanilla, requests)...")
    print("        (esto es lo que haría alguien sin pipeline — puede tardar mucho)")
    gc.collect()
    ram_van_tick_dl = RamSampler().start()
    _, t_dl_van_tick, _ = download_all_sequential(tick_urls)
    ram_van_tick_dl_peak = ram_van_tick_dl.stop()
    print(f"        → {t_dl_van_tick:.1f}s")

    # ── B.3 Procesamiento pipeline ─────────────────────────────────────────
    print(f"\n  [B-3] Procesando ticks con PIPELINE (raw → Parquet)...")
    gc.collect()
    writer_tick = ParquetWriter(tick_pipeline_dir)
    ram_pipe_tick_proc = RamSampler().start()
    rows_pipe_tick, t_proc_pipe_tick = _pipeline_process_tick(
        tick_cache_pipe, tick_urls_meta, writer_tick, decoder)
    ram_pipe_tick_proc_peak = ram_pipe_tick_proc.stop()
    disk_pipe_tick = sum(f.stat().st_size for f in tick_pipeline_dir.rglob("*.parquet")) / 1024**2
    print(f"        → {rows_pipe_tick:,} filas  {t_proc_pipe_tick:.2f}s  {disk_pipe_tick:.2f}MB Parquet")

    # ── B.4 Procesamiento vanilla ──────────────────────────────────────────
    print(f"\n  [B-4] Procesando ticks con VANILLA (struct → pandas → CSV)...")
    gc.collect()
    ram_van_tick_proc = RamSampler().start()
    rows_van_tick, t_proc_van_tick = _vanilla_process_tick(
        tick_cache_van, tick_urls_meta, tick_vanilla_dir)
    ram_van_tick_proc_peak = ram_van_tick_proc.stop()
    disk_van_tick = sum(f.stat().st_size for f in tick_vanilla_dir.rglob("*.csv")) / 1024**2
    print(f"        → {rows_van_tick:,} filas  {t_proc_van_tick:.2f}s  {disk_van_tick:.2f}MB CSV")

    # ── Reporte Ticks ──────────────────────────────────────────────────────
    print_dim_report(
        "DIMENSIÓN 1 — RED TICK (paralelo vs secuencial)",
        "Vanilla", "Pipeline",
        t_dl_van_tick, t_dl_pipe_tick,
        0, 0,
        ram_van_tick_dl_peak, ram_pipe_tick_dl_peak,
        urls=len(tick_urls),
    )
    print_dim_report(
        "DIMENSIÓN 2 — PROCESAMIENTO TICK (bytes ya en RAM)",
        "Vanilla", "Pipeline",
        t_proc_van_tick, t_proc_pipe_tick,
        rows_van_tick, rows_pipe_tick,
        ram_van_tick_proc_peak, ram_pipe_tick_proc_peak,
        disk_a_mb=disk_van_tick, disk_b_mb=disk_pipe_tick,
    )

    # ══════════════════════════════════════════════════════════════════════
    # VALIDACIONES
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n\n{'━'*65}")
    print("  VALIDACIONES DE INTEGRIDAD (Parquets del pipeline)")
    print("━"*65)
    run_validations(ohlcv_pipeline_dir, tick_pipeline_dir)

    passed_v = sum(1 for _, ok, _ in _VAL_RESULTS if ok)
    total_v  = len(_VAL_RESULTS)
    for name, ok, detail in _VAL_RESULTS:
        icon = "✓" if ok else "✗"
        sfx  = f"  → {detail}" if detail else ""
        print(f"  [{icon}] {name}{sfx}")
    val_exit = 0 if passed_v == total_v else 1

    # ══════════════════════════════════════════════════════════════════════
    # RESUMEN GLOBAL
    # ══════════════════════════════════════════════════════════════════════
    total_dl_van  = t_dl_van_ohlcv  + t_dl_van_tick
    total_dl_pipe = t_dl_pipe_ohlcv + t_dl_pipe_tick
    total_pr_van  = t_proc_van_ohlcv + t_proc_van_tick
    total_pr_pipe = t_proc_pipe_ohlcv + t_proc_pipe_tick

    sp_dl   = total_dl_van  / total_dl_pipe  if total_dl_pipe  > 0 else 0
    sp_proc = total_pr_van  / total_pr_pipe  if total_pr_pipe  > 0 else 0
    disk_van_total  = disk_van_ohlcv  + disk_van_tick
    disk_pipe_total = disk_pipe_ohlcv + disk_pipe_tick

    print(f"\n{'═'*65}")
    print("  RESUMEN GLOBAL")
    print(f"{'═'*65}")
    print(f"  {'':30} {'Vanilla':>10} {'Pipeline':>10}  {'Speedup':>8}")
    print(f"  {'-'*30} {'-'*10} {'-'*10}  {'-'*8}")
    print(f"  {'Descarga total (s)':<30} {total_dl_van:>10.1f} {total_dl_pipe:>10.1f}  {sp_dl:>7.1f}×")
    print(f"  {'Procesamiento total (s)':<30} {total_pr_van:>10.1f} {total_pr_pipe:>10.1f}  {sp_proc:>7.1f}×")
    print(f"  {'Disco total (MB)':<30} {disk_van_total:>10.1f} {disk_pipe_total:>10.1f}  "
          f"{disk_van_total/disk_pipe_total if disk_pipe_total>0 else 0:>6.1f}× más pequeño")
    print(f"\n  Validaciones Parquet: {passed_v}/{total_v}")
    print(f"{'═'*65}\n")

    return val_exit


if __name__ == "__main__":
    sys.exit(main())