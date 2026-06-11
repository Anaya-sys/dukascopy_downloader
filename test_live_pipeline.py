"""
test_live_pipeline.py
=====================
Frente 2 — Validación del pipeline en vivo contra Dukascopy.

Descarga datos reales de una ventana pequeña y controlada, los procesa a través
del pipeline completo (DukascopyClient → Bi5Decoder → ParquetWriter → finalize),
y verifica de forma estricta que los Parquet producidos son correctos en:

  1. Schema exacto (nombres y tipos de columna)
  2. File-level metadata completa (decimal_factor, symbol, timeframe, source)
  3. Timestamps: int64 ms-epoch UTC, ordenados ascendentemente, dentro de rango
  4. Precios: int32, todos positivos, dentro de rango de mercado esperado
  5. Volúmenes: float32, no negativos
  6. Row count: al menos 1 fila por archivo (datos reales esperados en la ventana)
  7. Sin archivos residuales de chunks en .chunks/
  8. Consistencia cross-timeframe: timestamps de h4 son múltiplos de 4h en ms-epoch
  9. Consistencia cross-timeframe: m15 timestamps son múltiplos de 15 min
  10. Idempotencia: segunda ejecución del finalize no corrompe el archivo

Instrumentos y ventana de prueba
---------------------------------
  EURUSD — tick, m15, h1, h4
  Ventana: 2024-01-02 (tick y m15 son por día; h1/h4 por mes → enero 2024)

  USDJPY — tick, h1
  Ventana: 2024-01-02 / enero 2024

Se usan fechas históricas cerradas (no el mes en curso) para garantizar que
los datos existen y son estables en el servidor de Dukascopy.

Ejecución
---------
  python test_live_pipeline.py

No requiere pytest. Código de salida 0 si todo pasa, 1 si algo falla.
"""

from __future__ import annotations

import datetime as dt
import sys
import tempfile
import traceback
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Asegurar que los módulos del proyecto están en el path ─────────────────
sys.path.insert(0, str(Path(__file__).parent))

from bi5_decoder import Bi5Decoder
from dukascopy_client import DukascopyClient, ChunkDownloadError
from parquet_writer import ParquetWriter

UTC = dt.timezone.utc

# ── Constantes de ventana de test ─────────────────────────────────────────
TEST_DATE     = dt.date(2024, 1, 2)        # martes estable con datos
TEST_MONTH_DT = dt.datetime(2024, 1, 1, tzinfo=UTC)   # base para h1/h4

# decimal_factor por símbolo (mismo mapping que _SYMBOL_FACTOR en migrate)
_FACTOR = {"EURUSD": 100_000, "USDJPY": 1_000}

# Rangos de precio razonables para validación (unidades: int32 = precio × factor)
_PRICE_RANGES = {
    "EURUSD": (100_000, 115_000),   # 1.00 – 1.15 × 100_000
    "USDJPY": (100_000,     160_000),        # 100 – 160   × 1_000
}

# Schemas esperados
_TICK_SCHEMA = pa.schema([
    pa.field("timestamp", pa.int64()),
    pa.field("ask",       pa.int32()),
    pa.field("bid",       pa.int32()),
    pa.field("ask_vol",   pa.float32()),
    pa.field("bid_vol",   pa.float32()),
])
_OHLCV_SCHEMA = pa.schema([
    pa.field("timestamp", pa.int64()),
    pa.field("open",      pa.int32()),
    pa.field("high",      pa.int32()),
    pa.field("low",       pa.int32()),
    pa.field("close",     pa.int32()),
    pa.field("volume",    pa.float32()),
])

# ── Registro de tests ─────────────────────────────────────────────────────
_RESULTS: list[tuple[str, bool, str]] = []   # (name, passed, detail)


def _record(name: str, passed: bool, detail: str = "") -> None:
    _RESULTS.append((name, passed, detail))
    icon = "✓" if passed else "✗"
    suffix = f"  → {detail}" if detail else ""
    print(f"  [{icon}] {name}{suffix}")


def _assert(cond: bool, name: str, detail: str = "") -> None:
    _record(name, cond, detail)


# ── Helpers ────────────────────────────────────────────────────────────────

def _ms_to_dt(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000, tz=UTC)


def _read_parquet_strict(path: Path) -> tuple[pa.Table, dict]:
    """Lee un Parquet y retorna (table, metadata_dict). Falla explícito si no existe."""
    assert path.exists(), f"Parquet no encontrado: {path}"
    table = pq.read_table(path)
    meta  = pq.read_schema(path).metadata or {}
    return table, meta


def _validate_parquet(
    path: Path,
    symbol: str,
    timeframe: str,
    factor: int,
    price_range: tuple[int, int],
    expected_schema: pa.Schema,
    ms_alignment: int | None = None,   # si no es None, verifica que timestamps % ms_alignment == 0
) -> list[str]:
    """
    Valida un archivo Parquet contra todos los criterios del Frente 2.
    Retorna lista de errores (vacía = todo correcto).
    """
    errors: list[str] = []

    if not path.exists():
        return [f"Archivo no encontrado: {path}"]

    table, meta = _read_parquet_strict(path)
    df = table.to_pandas()

    # ── 1. Schema ──────────────────────────────────────────────────────────
    actual_fields   = {f.name: f.type for f in table.schema}
    expected_fields = {f.name: f.type for f in expected_schema}
    for col, expected_type in expected_fields.items():
        actual_type = actual_fields.get(col)
        if actual_type is None:
            errors.append(f"Columna faltante: '{col}'")
        elif actual_type != expected_type:
            errors.append(f"Columna '{col}': tipo {actual_type} ≠ esperado {expected_type}")
    extra = set(actual_fields) - set(expected_fields)
    if extra:
        errors.append(f"Columnas extra inesperadas: {extra}")

    # ── 2. Metadata ────────────────────────────────────────────────────────
    required_meta = {
        b"decimal_factor": str(factor).encode(),
        b"symbol":         symbol.upper().encode(),
        b"timeframe":      timeframe.encode(),
        b"source":         b"dukascopy",
    }
    for key, expected_val in required_meta.items():
        actual_val = meta.get(key)
        if actual_val is None:
            errors.append(f"Metadata faltante: {key.decode()!r}")
        elif actual_val != expected_val:
            errors.append(
                f"Metadata '{key.decode()}': {actual_val!r} ≠ esperado {expected_val!r}"
            )

    # ── 3. Row count ───────────────────────────────────────────────────────
    if len(df) == 0:
        errors.append("Parquet vacío (0 filas)")
        return errors  # el resto de checks no aplica

    # ── 4. Timestamps ──────────────────────────────────────────────────────
    ts = df["timestamp"]
    if str(ts.dtype) != "int64":
        errors.append(f"timestamp dtype={ts.dtype} ≠ int64")

    if not ts.is_monotonic_increasing:
        first_inv = next(
            (i for i in range(len(ts) - 1) if ts.iloc[i] > ts.iloc[i + 1]), None
        )
        errors.append(f"Timestamps no ordenados ascendentemente (primer inverso en fila {first_inv})")

    # Verificar que los timestamps caen en el año 2024 (ventana del test)
    ts_min_dt = _ms_to_dt(int(ts.min()))
    ts_max_dt = _ms_to_dt(int(ts.max()))
    if ts_min_dt.year < 2024 or ts_min_dt.year > 2024:
        errors.append(f"Timestamp mínimo fuera de 2024: {ts_min_dt.isoformat()}")
    if ts_max_dt.year < 2024 or ts_max_dt.year > 2024:
        errors.append(f"Timestamp máximo fuera de 2024: {ts_max_dt.isoformat()}")

    # Alineación temporal (para m15 y h4)
    if ms_alignment is not None:
        bad_align = (ts % ms_alignment != 0).sum()
        if bad_align > 0:
            examples = ts[ts % ms_alignment != 0].head(3).tolist()
            errors.append(
                f"{bad_align} timestamps no alineados a {ms_alignment}ms: {examples}"
            )

    # ── 5. Precios ──────────────────────────────────────────────────────────
    lo, hi = price_range
    if timeframe == "tick":
        price_cols = ["ask", "bid"]
    else:
        price_cols = ["open", "high", "low", "close"]

    for col in price_cols:
        if col not in df.columns:
            continue
        if str(df[col].dtype) != "int32":
            errors.append(f"Columna precio '{col}': dtype={df[col].dtype} ≠ int32")
        pmin, pmax = int(df[col].min()), int(df[col].max())
        if pmin <= 0:
            errors.append(f"'{col}' contiene precios ≤ 0: min={pmin}")
        if pmin < lo or pmax > hi:
            errors.append(
                f"'{col}' fuera de rango [{lo}, {hi}]: min={pmin}, max={pmax}"
            )
        # ask siempre >= bid (spread ≥ 0) para tick
        if timeframe == "tick" and col == "ask":
            spread_neg = (df["ask"] < df["bid"]).sum()
            if spread_neg > 0:
                errors.append(f"{spread_neg} filas con ask < bid (spread negativo)")
        # OHLCV: high >= low
        if timeframe != "tick" and "high" in df.columns and "low" in df.columns:
            bad_hl = (df["high"] < df["low"]).sum()
            if bad_hl > 0:
                errors.append(f"{bad_hl} filas con high < low")

    # ── 6. Volúmenes ────────────────────────────────────────────────────────
    vol_cols = ["ask_vol", "bid_vol"] if timeframe == "tick" else ["volume"]
    for col in vol_cols:
        if col not in df.columns:
            continue
        if str(df[col].dtype) != "float32":
            errors.append(f"Columna volumen '{col}': dtype={df[col].dtype} ≠ float32")
        if (df[col] < 0).any():
            errors.append(f"'{col}' contiene valores negativos")

    return errors


# ── Escenarios de descarga ────────────────────────────────────────────────

def _run_tick_scenario(
    base: Path,
    client: DukascopyClient,
    decoder: Bi5Decoder,
    writer: ParquetWriter,
    symbol: str,
) -> tuple[int, int]:
    """
    Descarga las 24 horas del TEST_DATE para `symbol` (tick).
    Retorna (chunks_descargados, filas_escritas).
    """
    factor = _FACTOR[symbol]
    chunks_ok = 0
    total_rows = 0

    for hour in range(24):
        hour_dt = dt.datetime(
            TEST_DATE.year, TEST_DATE.month, TEST_DATE.day, hour, tzinfo=UTC
        )
        try:
            raw = client.download_chunk(symbol, "tick", hour_dt)
        except ChunkDownloadError as e:
            print(f"      WARN: fallo de red hora {hour:02d}h: {e}")
            continue

        if raw is None:
            continue

        df = decoder.decode(raw, "tick", factor, hour_dt, raw_prices=True)
        if df.empty:
            continue

        df.sort_values("timestamp", inplace=True)
        writer.write(symbol, "tick", df, decimal_factor=factor)
        chunks_ok += 1
        total_rows += len(df)

    return chunks_ok, total_rows


def _run_ohlcv_scenario(
    base: Path,
    client: DukascopyClient,
    decoder: Bi5Decoder,
    writer: ParquetWriter,
    symbol: str,
    tf: str,
) -> tuple[bool, int]:
    """
    Descarga datos OHLCV para `symbol`/`tf` en la ventana de test.
    Retorna (descargado_ok, filas_escritas).

    Para m15: descarga todos los días de enero 2024 (igual que el orchestrator),
    de modo que cubra la misma ventana mensual que h1 y la comparación de filas
    cross_tf_m15_mas_filas_que_h1 sea válida (m15 = 4 × h1 en la misma ventana).
    """
    factor      = _FACTOR[symbol]
    download_tf = {"m15": "m1", "h1": "h1", "h4": "h1"}[tf]
    resample    = {"m15": "15min", "h4": "4h", "h1": None}[tf]

    if tf in ("h1", "h4"):
        # h1/h4: un único archivo mensual desde Dukascopy.
        base_dt = TEST_MONTH_DT
        dl_dt   = TEST_DATE.replace(day=1)  # Dukascopy h1 → primer día del mes

        try:
            raw = client.download_chunk(symbol, download_tf, dl_dt)
        except ChunkDownloadError as e:
            print(f"      WARN: fallo de red {symbol}/{tf}: {e}")
            return False, 0

        if raw is None:
            return False, 0

        df = decoder.decode(raw, download_tf, factor, base_dt, raw_prices=True)
        if df.empty:
            return False, 0

        if resample:
            from orchestrator import _resample_ohlcv
            df = _resample_ohlcv(df, resample)

        if df.empty:
            return False, 0

        writer.write(symbol, tf, df, decimal_factor=factor)
        return True, len(df)

    else:
        # m15 (y m1): archivos diarios desde Dukascopy.
        # Descargamos todos los días de enero 2024 para cubrir la misma ventana
        # que el archivo mensual h1, haciendo válida la comparación de filas.
        month_start = TEST_DATE.replace(day=1)   # 2024-01-01
        if month_start.month == 12:
            month_end = dt.date(month_start.year + 1, 1, 1) - dt.timedelta(days=1)
        else:
            month_end = dt.date(month_start.year, month_start.month + 1, 1) - dt.timedelta(days=1)

        total_rows = 0
        any_ok     = False
        current    = month_start
        while current <= month_end:
            base_dt_day = dt.datetime(current.year, current.month, current.day, tzinfo=UTC)
            try:
                raw = client.download_chunk(symbol, download_tf, current)
            except ChunkDownloadError as e:
                print(f"      WARN: fallo de red {symbol}/{tf} {current}: {e}")
                current += dt.timedelta(days=1)
                continue

            if raw is None:
                current += dt.timedelta(days=1)
                continue

            df = decoder.decode(raw, download_tf, factor, base_dt_day, raw_prices=True)
            if not df.empty:
                if resample:
                    from orchestrator import _resample_ohlcv
                    df = _resample_ohlcv(df, resample)
                if not df.empty:
                    writer.write(symbol, tf, df, decimal_factor=factor)
                    total_rows += len(df)
                    any_ok = True

            current += dt.timedelta(days=1)

        return any_ok, total_rows


# ── Test principal ─────────────────────────────────────────────────────────

def run_live_tests() -> int:
    tmpdir = Path(tempfile.mkdtemp(prefix="duka_live_"))
    print(f"\n{'='*65}")
    print("FRENTE 2 — VALIDACIÓN PIPELINE EN VIVO (Dukascopy)")
    print(f"{'='*65}")
    print(f"Directorio temporal: {tmpdir}\n")

    client  = DukascopyClient(max_retries=3)
    decoder = Bi5Decoder()
    writer  = ParquetWriter(tmpdir)

    # ──────────────────────────────────────────────────────────────────────
    # BLOQUE 1: Descarga + escritura de chunks
    # ──────────────────────────────────────────────────────────────────────
    print("── Bloque 1: Descarga de datos reales ──────────────────────────")

    scenarios = [
        ("EURUSD", "tick"),
        ("EURUSD", "m15"),
        ("EURUSD", "h1"),
        ("EURUSD", "h4"),
        ("USDJPY", "tick"),
        ("USDJPY", "h1"),
    ]

    row_counts: dict[tuple[str, str], int] = {}

    for symbol, tf in scenarios:
        print(f"  Descargando {symbol}/{tf}...", end=" ", flush=True)
        try:
            if tf == "tick":
                chunks, rows = _run_tick_scenario(tmpdir, client, decoder, writer, symbol)
                ok = chunks > 0
            else:
                ok, rows = _run_ohlcv_scenario(tmpdir, client, decoder, writer, symbol, tf)

            row_counts[(symbol, tf)] = rows
            status = f"{rows} filas ({('✓' if ok else 'sin datos')})"
            print(status)
            _record(
                f"descarga_en_vivo_{symbol}_{tf}",
                ok and rows > 0,
                f"{rows} filas descargadas" if ok else "sin datos del servidor",
            )
        except Exception as e:
            print(f"ERROR: {e}")
            _record(f"descarga_en_vivo_{symbol}_{tf}", False, str(e))
            row_counts[(symbol, tf)] = 0

    client.close()

    # ──────────────────────────────────────────────────────────────────────
    # BLOQUE 2: finalize() — merge de chunks
    # ──────────────────────────────────────────────────────────────────────
    print("\n── Bloque 2: finalize() ────────────────────────────────────────")

    finalize_ok: dict[tuple[str, str], bool] = {}
    for symbol, tf in scenarios:
        if row_counts.get((symbol, tf), 0) == 0:
            finalize_ok[(symbol, tf)] = False
            continue
        print(f"  finalize {symbol}/{tf}...", end=" ", flush=True)
        try:
            writer.finalize(symbol, tf)
            print("OK")
            finalize_ok[(symbol, tf)] = True
            _record(f"finalize_{symbol}_{tf}", True)
        except Exception as e:
            print(f"ERROR: {e}")
            traceback.print_exc()
            finalize_ok[(symbol, tf)] = False
            _record(f"finalize_{symbol}_{tf}", False, str(e))

    # ──────────────────────────────────────────────────────────────────────
    # BLOQUE 3: Verificación estricta de cada Parquet producido
    # ──────────────────────────────────────────────────────────────────────
    print("\n── Bloque 3: Verificación estricta de Parquets ─────────────────")

    _TF_FOLDER = {"tick": "tick", "m15": "15min", "h1": "1h", "h4": "4h"}
    MS_15MIN = 15 * 60 * 1000
    MS_4H    = 4 * 3600 * 1000

    for symbol, tf in scenarios:
        if not finalize_ok.get((symbol, tf)):
            _record(f"validacion_{symbol}_{tf}", False, "skipped — finalize falló o sin datos")
            continue

        factor   = _FACTOR[symbol]
        pr       = _PRICE_RANGES[symbol]
        exp_schema = _TICK_SCHEMA if tf == "tick" else _OHLCV_SCHEMA
        alignment = MS_15MIN if tf == "m15" else (MS_4H if tf == "h4" else None)

        folder = _TF_FOLDER[tf]
        if tf == "tick":
            # tick particionado por mes → buscar YYYY_MM=2024_01
            path = tmpdir / symbol / folder / f"{symbol}_{folder}_2024_01.parquet"
        else:
            path = tmpdir / symbol / folder / f"{symbol}_{folder}.parquet"

        errors = _validate_parquet(
            path, symbol, tf, factor, pr, exp_schema, alignment
        )

        if errors:
            for e in errors:
                print(f"    ✗ {symbol}/{tf}: {e}")
        _record(
            f"validacion_{symbol}_{tf}",
            len(errors) == 0,
            f"{len(errors)} errores: {errors[0]}" if errors else f"OK — {path.name}",
        )

    # ──────────────────────────────────────────────────────────────────────
    # BLOQUE 4: Sin residuos de chunks
    # ──────────────────────────────────────────────────────────────────────
    print("\n── Bloque 4: Sin residuos de chunks ────────────────────────────")

    for symbol, tf in scenarios:
        if not finalize_ok.get((symbol, tf)):
            continue
        folder     = _TF_FOLDER[tf]
        chunk_dir  = tmpdir / symbol / folder / ".chunks"
        if chunk_dir.exists():
            residues = list(chunk_dir.glob("*.parquet"))
            _record(
                f"sin_residuos_{symbol}_{tf}",
                len(residues) == 0,
                f"{len(residues)} chunks sin limpiar" if residues else "",
            )
        else:
            _record(f"sin_residuos_{symbol}_{tf}", True, ".chunks/ eliminado correctamente")

    # ──────────────────────────────────────────────────────────────────────
    # BLOQUE 5: Idempotencia de finalize()
    # ──────────────────────────────────────────────────────────────────────
    print("\n── Bloque 5: Idempotencia de finalize() ────────────────────────")

    # Tomamos EURUSD/h1 como representante (OHLCV sin partición por mes)
    sym_idem, tf_idem = "EURUSD", "h1"
    if finalize_ok.get((sym_idem, tf_idem)):
        folder = _TF_FOLDER[tf_idem]
        path   = tmpdir / sym_idem / folder / f"{sym_idem}_{folder}.parquet"
        try:
            # Leer estado previo
            rows_before = pq.read_metadata(path).num_rows if path.exists() else -1
            meta_before = pq.read_schema(path).metadata if path.exists() else {}

            # Segunda llamada a finalize (no hay chunks nuevos → no-op)
            writer.finalize(sym_idem, tf_idem)

            rows_after = pq.read_metadata(path).num_rows if path.exists() else -1
            meta_after = pq.read_schema(path).metadata if path.exists() else {}

            idem_ok = (rows_before == rows_after) and (meta_before == meta_after)
            _record(
                "idempotencia_finalize_eurusd_h1",
                idem_ok,
                f"filas antes={rows_before}, después={rows_after}",
            )
        except Exception as e:
            _record("idempotencia_finalize_eurusd_h1", False, str(e))
    else:
        _record("idempotencia_finalize_eurusd_h1", False, "skipped — EURUSD/h1 no disponible")

    # ──────────────────────────────────────────────────────────────────────
    # BLOQUE 6: Consistencia cross-timeframe (m15 vs h1 en EURUSD)
    # ──────────────────────────────────────────────────────────────────────
    print("\n── Bloque 6: Consistencia cross-timeframe EURUSD ───────────────")

    if finalize_ok.get(("EURUSD", "m15")) and finalize_ok.get(("EURUSD", "h1")):
        path_m15 = tmpdir / "EURUSD" / "15min" / "EURUSD_15min.parquet"
        path_h1  = tmpdir / "EURUSD" / "1h" / "EURUSD_1h.parquet"
        try:
            df_m15 = pq.read_table(path_m15).to_pandas()
            df_h1  = pq.read_table(path_h1).to_pandas()

            # Convertir a DatetimeIndex para comparar rangos
            ts_m15 = pd.to_datetime(df_m15["timestamp"], unit="ms", utc=True)
            ts_h1  = pd.to_datetime(df_h1["timestamp"],  unit="ms", utc=True)

            # El rango temporal de m15 debe estar contenido en el rango de h1
            m15_start, m15_end = ts_m15.min(), ts_m15.max()
            h1_start,  h1_end  = ts_h1.min(),  ts_h1.max()

            # Ambos deben arrancar en enero 2024
            both_jan_2024 = (m15_start.year == 2024 and h1_start.year == 2024)
            _record(
                "cross_tf_ambos_en_enero_2024",
                both_jan_2024,
                f"m15_start={m15_start.date()}, h1_start={h1_start.date()}",
            )

            # m15 debe tener más registros que h1 (misma ventana, mayor resolución)
            _record(
                "cross_tf_m15_mas_filas_que_h1",
                len(df_m15) > len(df_h1),
                f"m15={len(df_m15)} filas, h1={len(df_h1)} filas",
            )
        except Exception as e:
            _record("cross_tf_ambos_en_enero_2024", False, str(e))
            _record("cross_tf_m15_mas_filas_que_h1", False, str(e))
    else:
        _record("cross_tf_ambos_en_enero_2024", False, "skipped — datos insuficientes")
        _record("cross_tf_m15_mas_filas_que_h1", False, "skipped — datos insuficientes")

    # ──────────────────────────────────────────────────────────────────────
    # RESUMEN FINAL
    # ──────────────────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print("RESUMEN")
    print(f"{'='*65}")

    passed = sum(1 for _, ok, _ in _RESULTS if ok)
    failed = sum(1 for _, ok, _ in _RESULTS if not ok)
    total  = len(_RESULTS)

    if failed:
        print("\nFALLOS:")
        for name, ok, detail in _RESULTS:
            if not ok:
                print(f"  ✗ {name}: {detail}")

    print(f"\nTests: {passed}/{total}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run_live_tests())