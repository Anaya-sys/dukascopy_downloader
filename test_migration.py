"""
test_migration.py
=================
Suite de tests para migrate_csv_to_parquet.py

Ejecución
---------
    python test_migration.py

No requiere pytest. Imprime el resultado de cada test y un resumen final
en formato:

    Tests: 10/10

Código de salida 0 si todos pasan, 1 si alguno falla.
"""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

import migrate_csv_to_parquet as m


# ── Mini test harness ──────────────────────────────────────────────────────

_TESTS: list = []


def test(fn):
    """Decorador para registrar un test."""
    _TESTS.append(fn)
    return fn


def _eq(a, b, msg=""):
    if a != b:
        raise AssertionError(f"{msg} esperado={b!r} obtenido={a!r}")


def _true(cond, msg=""):
    if not cond:
        raise AssertionError(msg or "condición falsa")


# ── Helpers de datos ────────────────────────────────────────────────────────

def _write_tick_csv(path: Path, n: int, dup: int = 0) -> None:
    """Escribe un CSV tick con n filas (+ dup filas duplicadas al inicio)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    base = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    rows = []
    if dup:
        ts = (base + dt.timedelta(hours=1)).isoformat(sep=" ")
        rows += [f"{ts},1.08500,1.08499,1.0,2.0"] * dup
    for i in range(n):
        ts = (base + dt.timedelta(seconds=i)).isoformat(sep=" ")
        rows.append(f"{ts},{1.08500 + i*1e-6:.5f},{1.08499 + i*1e-6:.5f},1.0,2.0")
    path.write_text("timestamp,ask,bid,ask_vol,bid_vol\n" + "\n".join(rows) + "\n")


def _write_ohlcv_csv(path: Path, n: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    base = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    rows = []
    for i in range(n):
        ts = (base + dt.timedelta(hours=i)).isoformat(sep=" ")
        rows.append(f"{ts},1.08500,1.08600,1.08400,1.08550,100.0")
    path.write_text(
        "timestamp,open,high,low,close,volume\n" + "\n".join(rows) + "\n"
    )


# ── Tests ───────────────────────────────────────────────────────────────────

@test
def test_to_ms_epoch_strings():
    """_to_ms_epoch convierte strings ISO con tz a ms-epoch UTC correctos."""
    s = pd.Series(["2024-01-01 00:00:00+00:00", "2024-06-15 12:30:00.500+00:00"])
    out = list(m._to_ms_epoch(s))
    e0 = int(dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    e1 = int(dt.datetime(2024, 6, 15, 12, 30, 0, 500000,
                         tzinfo=dt.timezone.utc).timestamp() * 1000)
    _eq(out, [e0, e1], "ms-epoch de strings")


@test
def test_to_ms_epoch_int_passthrough():
    """_to_ms_epoch deja pasar enteros ya en ms-epoch sin alterarlos."""
    s = pd.Series([1704067200000, 1704067260000])
    _eq(list(m._to_ms_epoch(s)), [1704067200000, 1704067260000], "passthrough int")


@test
def test_prices_to_int32_scaling():
    """_prices_to_int32 escala floats por factor y castea a int32."""
    df = pd.DataFrame({"ask": [1.08501, 1.08502], "bid": [1.08499, 1.08498]})
    out = m._prices_to_int32(df, "tick", 100_000)
    _eq(list(out["ask"]), [108501, 108502], "ask escalado")
    _eq(list(out["bid"]), [108499, 108498], "bid escalado")
    _eq(str(out["ask"].dtype), "int32", "dtype int32")


@test
def test_migrate_tick_values():
    """Migración tick end-to-end: timestamps, precios y dtypes correctos."""
    d = Path(tempfile.mkdtemp())
    csv = d / "EURUSD" / "tick" / "EURUSD_tick_2024_01.csv"
    _write_tick_csv(csv, n=5)
    res = m.migrate_file(csv, d, None, "polars", dry_run=False)
    _eq(res["status"], "ok", f"status ({res['error']})")
    _eq(res["rows_in"], 5, "rows_in")
    _eq(res["rows_out"], 5, "rows_out")

    t = pq.read_table(csv.with_suffix(".parquet")).to_pandas()
    _eq(str(t["timestamp"].dtype), "int64", "timestamp int64")
    _eq(str(t["ask"].dtype), "int32", "ask int32")
    _eq(str(t["ask_vol"].dtype), "float32", "ask_vol float32")
    e0 = int(dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    _eq(int(t["timestamp"].iloc[0]), e0, "primer timestamp")
    _eq(int(t["ask"].iloc[0]), 108500, "primer ask")


@test
def test_migrate_ohlcv_values():
    """Migración OHLCV end-to-end con schema de 6 columnas."""
    d = Path(tempfile.mkdtemp())
    csv = d / "EURUSD" / "1h" / "EURUSD_1h.csv"
    _write_ohlcv_csv(csv, n=10)
    res = m.migrate_file(csv, d, None, "polars", dry_run=False)
    _eq(res["status"], "ok", f"status ({res['error']})")
    t = pq.read_table(csv.with_suffix(".parquet")).to_pandas()
    _eq(list(t.columns), ["timestamp", "open", "high", "low", "close", "volume"],
        "columnas OHLCV")
    _eq(int(t["open"].iloc[0]), 108500, "open escalado")
    _eq(int(t["high"].iloc[0]), 108600, "high escalado")


@test
def test_metadata_roundtrip():
    """La file-level metadata se persiste y es leíble (factor, symbol, etc.)."""
    d = Path(tempfile.mkdtemp())
    csv = d / "USDJPY" / "tick" / "USDJPY_tick_2024_01.csv"
    _write_tick_csv(csv, n=3)
    m.migrate_file(csv, d, None, "polars", dry_run=False)
    meta = pq.read_schema(csv.with_suffix(".parquet")).metadata
    # USDJPY → factor 1000 según el mapping de símbolos
    _eq(meta[b"decimal_factor"], b"1000", "decimal_factor JPY")
    _eq(meta[b"symbol"], b"USDJPY", "symbol")
    _eq(meta[b"timeframe"], b"tick", "timeframe")
    _eq(meta[b"source"], b"dukascopy", "source")


@test
def test_duplicates_preserved():
    """No se deduplica: rows_in == rows_out aunque haya timestamps repetidos."""
    d = Path(tempfile.mkdtemp())
    csv = d / "EURUSD" / "tick" / "EURUSD_tick_2024_02.csv"
    _write_tick_csv(csv, n=50, dup=10)
    res = m.migrate_file(csv, d, None, "polars", dry_run=False)
    _eq(res["status"], "ok", f"status ({res['error']})")
    _eq(res["rows_in"], 60, "rows_in con duplicados")
    _eq(res["rows_out"], 60, "rows_out con duplicados")


@test
def test_sorted_output():
    """La salida queda ordenada por timestamp ascendente."""
    d = Path(tempfile.mkdtemp())
    csv = d / "EURUSD" / "tick" / "EURUSD_tick_unsorted.csv"
    csv.parent.mkdir(parents=True, exist_ok=True)
    # filas en orden invertido
    csv.write_text(
        "timestamp,ask,bid,ask_vol,bid_vol\n"
        "2024-01-01 00:00:02+00:00,1.2,1.1,1.0,1.0\n"
        "2024-01-01 00:00:00+00:00,1.2,1.1,1.0,1.0\n"
        "2024-01-01 00:00:01+00:00,1.2,1.1,1.0,1.0\n"
    )
    m.migrate_file(csv, d, None, "polars", dry_run=False)
    ts = list(pq.read_table(csv.with_suffix(".parquet")).to_pandas()["timestamp"])
    _eq(ts, sorted(ts), "timestamps ordenados")


@test
def test_dry_run_no_file():
    """dry_run no escribe ningún Parquet pero reporta rows_out."""
    d = Path(tempfile.mkdtemp())
    csv = d / "EURUSD" / "tick" / "EURUSD_tick_dry.csv"
    _write_tick_csv(csv, n=5)
    res = m.migrate_file(csv, d, None, "polars", dry_run=True)
    _eq(res["status"], "dry_run_ok", "status dry_run")
    _eq(res["rows_out"], 5, "rows_out dry_run")
    _true(not csv.with_suffix(".parquet").exists(), "no debe existir el Parquet")


@test
def test_empty_csv_skipped():
    """Un CSV solo con cabecera se marca como skipped_empty."""
    d = Path(tempfile.mkdtemp())
    csv = d / "EURUSD" / "tick" / "EURUSD_tick_empty.csv"
    csv.parent.mkdir(parents=True, exist_ok=True)
    csv.write_text("timestamp,ask,bid,ask_vol,bid_vol\n")
    res = m.migrate_file(csv, d, None, "polars", dry_run=False)
    _eq(res["status"], "skipped_empty", "status vacío")
    _true(not csv.with_suffix(".parquet").exists(), "no debe escribir Parquet")


def _run_migration_peak(csv: Path, base: Path, force_pandas: bool) -> tuple[int, float]:
    """
    Ejecuta migrate_file en un subproceso aislado y devuelve (rows_out, pico_MB).

    Se aísla en subproceso porque ru_maxrss mide el pico de RSS de todo el
    proceso; un proceso por medición da lecturas limpias. Con force_pandas se
    desactiva el path streaming para medir el path clásico (carga completa).
    """
    runner = textwrap.dedent(f"""
        import resource
        from pathlib import Path
        import migrate_csv_to_parquet as m
        if {force_pandas!r}:
            m._STREAMING_AVAILABLE = False
        res = m.migrate_file(Path({str(csv)!r}), Path({str(base)!r}),
                             None, "polars", dry_run=False)
        peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        assert res["status"] == "ok", res["error"]
        print(f"{{res['rows_out']}},{{peak_mb:.1f}}")
    """)
    proc = subprocess.run(
        [sys.executable, "-c", runner],
        capture_output=True, text=True, cwd=str(Path(__file__).parent),
    )
    _true(proc.returncode == 0, f"subproceso falló: {proc.stderr.strip()}")
    rows_s, peak_s = proc.stdout.strip().split(",")
    return int(rows_s), float(peak_s)


@test
def test_memory_bounded():
    """
    FIX OOM: el path streaming usa mucha menos memoria que el path pandas.

    Mide el pico real de RSS de ambos paths sobre el mismo CSV tick grande.
    El path pandas carga el archivo completo (~8x el CSV); el streaming debe
    quedar claramente por debajo. Se exige una reducción de al menos 25%.
    """
    d = Path(tempfile.mkdtemp())
    csv = d / "EURUSD" / "tick" / "EURUSD_tick_big.csv"
    _write_tick_csv(csv, n=1_500_000)
    csv_mb = csv.stat().st_size / 1024 / 1024

    # Path pandas escribe el .parquet; lo borramos antes de medir streaming
    rows_p, peak_pandas = _run_migration_peak(csv, d, force_pandas=True)
    csv.with_suffix(".parquet").unlink(missing_ok=True)
    rows_s, peak_stream = _run_migration_peak(csv, d, force_pandas=False)

    _eq(rows_p, 1_500_000, "filas (pandas)")
    _eq(rows_s, 1_500_000, "filas (streaming)")
    print(f"      [memoria] CSV={csv_mb:.0f}MB  pandas={peak_pandas:.0f}MB  "
          f"streaming={peak_stream:.0f}MB  "
          f"({peak_stream / peak_pandas * 100:.0f}% del pandas)", end=" ")
    _true(peak_stream < peak_pandas * 0.75,
          f"streaming no reduce memoria: {peak_stream:.0f}MB vs "
          f"pandas {peak_pandas:.0f}MB")


# ── Runner ──────────────────────────────────────────────────────────────────

def main() -> int:
    passed = 0
    total = len(_TESTS)
    print(f"Ejecutando {total} tests de migración...\n")
    for i, fn in enumerate(_TESTS, 1):
        name = fn.__name__
        try:
            fn()
            print(f"  [{i:2d}/{total}] PASS  {name}")
            passed += 1
        except Exception as exc:
            print(f"  [{i:2d}/{total}] FAIL  {name}")
            print(f"            -> {type(exc).__name__}: {exc}")
    print()
    print(f"Tests: {passed}/{total}")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
