"""
Diagnóstico de memoria por fase — nueva implementación pandas chunked.
"""

import threading
import time
import tempfile
import datetime as dt
from pathlib import Path
import sys

import psutil

proc = psutil.Process()

def make_sampler():
    stop = threading.Event()
    peak = [proc.memory_info().rss]
    def _sample():
        while not stop.is_set():
            rss = proc.memory_info().rss
            if rss > peak[0]:
                peak[0] = rss
            time.sleep(0.005)
    t = threading.Thread(target=_sample, daemon=True)
    t.start()
    return stop, peak, t

def mb(b): return b / 1024 / 1024

def checkpoint(label, peak):
    rss = proc.memory_info().rss
    print(f"  {label:<50} rss={mb(rss):.0f}MB  pico={mb(peak[0]):.0f}MB")

# ── Generar CSV ───────────────────────────────────────────────────────────
if len(sys.argv) > 1:
    csv_path = Path(sys.argv[1])
else:
    d = Path(tempfile.mkdtemp())
    csv_path = d / "EURUSD" / "tick" / "EURUSD_tick_big.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    print("Generando CSV de 1.5M filas...")
    base_ts = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    with csv_path.open("w") as f:
        f.write("timestamp,ask,bid,ask_vol,bid_vol\n")
        for i in range(1_500_000):
            ts = (base_ts + dt.timedelta(seconds=i)).isoformat(sep=" ")
            f.write(f"{ts},{1.08500 + i*1e-6:.5f},{1.08499 + i*1e-6:.5f},1.0,2.0\n")
    print(f"CSV generado: {csv_path.stat().st_size/1024/1024:.0f} MB\n")

import migrate_csv_to_parquet as m
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid

symbol = "EURUSD"
timeframe = "tick"
factor = 100_000
parquet_path = csv_path.with_suffix(".parquet")
price_cols, vol_cols = m._price_and_vol_cols(timeframe)
schema = m._build_pa_schema(timeframe, factor, symbol)

stop, peak, sampler = make_sampler()
peak[0] = proc.memory_info().rss

print("=== DIAGNÓSTICO DE MEMORIA POR FASE (pandas chunked) ===\n")
checkpoint("INICIO", peak)

# Fase 1: contar filas (solo col 0)
rows_in = sum(
    len(c) for c in pd.read_csv(csv_path, usecols=[0], chunksize=m._ROW_GROUP_SIZE)
)
checkpoint(f"después de contar filas ({rows_in})", peak)

# Fase 2: escribir chunks al .tmp
parquet_path.parent.mkdir(parents=True, exist_ok=True)
tmp = parquet_path.with_name(f"{parquet_path.stem}_{uuid.uuid4().hex}.tmp.parquet")
writer = pq.ParquetWriter(tmp, schema, compression="zstd", compression_level=1)
n_chunks = 0
try:
    for chunk in pd.read_csv(csv_path, chunksize=m._ROW_GROUP_SIZE, low_memory=False):
        transformed = m._transform_chunk(chunk, timeframe, factor, price_cols, vol_cols)
        table = pa.Table.from_pandas(transformed, schema=schema, preserve_index=False)
        writer.write_table(table)
        n_chunks += 1
        del chunk, transformed, table
finally:
    writer.close()

checkpoint(f"después de escribir {n_chunks} chunks → {tmp.stat().st_size/1024/1024:.0f}MB en disco", peak)

# Fase 3: sort check (pasada 1, solo timestamps)
pf = pq.ParquetFile(tmp)
ts_col = pf.read(columns=["timestamp"]).column("timestamp")
ts_np = ts_col.to_pylist()
already_sorted = all(ts_np[i] <= ts_np[i+1] for i in range(len(ts_np)-1))
del ts_np, ts_col
pf.close()
checkpoint(f"después de check sort (ya_ordenado={already_sorted})", peak)

# Limpieza
if tmp.exists(): tmp.unlink()

stop.set()
sampler.join()

print(f"\n>>> PICO TOTAL RSS: {mb(peak[0]):.0f} MB")
print(f">>> Para pasar el test necesita ser < 75% del path pandas (~340MB)")