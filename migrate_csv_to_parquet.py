"""
migrate_csv_to_parquet.py
=========================
Script one-shot de migración de archivos CSV del pipeline Dukascopy
a formato Parquet con el schema int32/int64 de Fase 2B.

Uso
---
  python migrate_csv_to_parquet.py --base /ruta/a/datos
  python migrate_csv_to_parquet.py --base /ruta/a/datos --dry-run
  python migrate_csv_to_parquet.py --base /ruta/a/datos --symbol EURUSD

Garantías
---------
  - NO elimina los CSV originales.
  - Sobreescritura atómica: escribe a .tmp y luego rename.
  - Aborta si el conteo de filas input ≠ output.
  - Genera migration.log con ruta, filas_in, filas_out, resultado.
  - Si pyarrow no está instalado, usa Polars como backend alternativo.
  - Si ninguno de los dos está disponible, informa y aborta.

Schema de salida
----------------
  Tick:  timestamp(int64 ms-epoch), ask(int32), bid(int32),
         ask_vol(float32), bid_vol(float32)
  OHLCV: timestamp(int64 ms-epoch), open(int32), high(int32),
         low(int32), close(int32), volume(float32)

File-level metadata (Parquet)
------------------------------
  decimal_factor, symbol, timeframe, source="dukascopy"

Notas sobre el decimal_factor
------------------------------
  Se intenta obtener de: (1) argumento --decimal-factor, (2) mapping
  hardcoded de símbolos comunes, (3) default 100_000. Si el factor es
  incorrecto los precios int32 son erróneos pero el conteo de filas
  y la estructura seguirán siendo válidos.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Backend detection ─────────────────────────────────────────────────────

def _detect_backend() -> str:
    """Retorna 'pyarrow', 'polars', o lanza ImportError."""
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except ImportError:
        pass
    try:
        import polars  # noqa: F401
        return "polars"
    except ImportError:
        pass
    raise ImportError(
        "Se requiere pyarrow o polars para escribir Parquet. "
        "Instala uno de los dos: pip install pyarrow  o  pip install polars"
    )


def _module_available(name: str) -> bool:
    """True si un módulo puede importarse sin importarlo realmente."""
    import importlib.util
    return importlib.util.find_spec(name) is not None


# El path de migración en streaming (memoria acotada) requiere Polars para el
# parseo/transformación lazy + sink_parquet, y PyArrow para reinyectar la
# file-level metadata escribiendo por row-group. Si falta alguno, se usa el
# path clásico basado en pandas (carga el archivo completo en memoria).
_STREAMING_AVAILABLE = _module_available("polars") and _module_available("pyarrow")

# Tamaño de row-group / batch para escritura y finalización en streaming.
# Acota el working set de memoria durante la copia con metadata.
_ROW_GROUP_SIZE = 256_000


# ── Timeframe mapping ─────────────────────────────────────────────────────

# Detecta el timeframe a partir de la carpeta en la convención de rutas:
#   {base}/{SYMBOL}/{tf_folder}/{SYMBOL}_{tf_folder}[_YYYY_MM].csv
_FOLDER_TO_TF: dict[str, str] = {
    "tick":  "tick",
    "15min": "m15",
    "1h":    "h1",
    "4h":    "h4",
    "1min":  "m1",
}

# Símbolos comunes → decimal_factor (fallback si no se pasa --decimal-factor)
_SYMBOL_FACTOR: dict[str, int] = {
    "EURUSD": 100_000, "GBPUSD": 100_000, "AUDUSD": 100_000,
    "NZDUSD": 100_000, "USDCAD": 100_000, "USDCHF": 100_000,
    "EURGBP": 100_000, "EURJPY":   1_000, "USDJPY":   1_000,
    "GBPJPY":   1_000, "AUDJPY":   1_000,
    "XAUUSD":     100, "XAGUSD":   1_000,
    "BTCUSD":       1, "ETHUSD":     100,
}

_DEFAULT_FACTOR = 100_000


# ── File discovery ────────────────────────────────────────────────────────

def _find_csv_files(base: Path, symbol_filter: str | None) -> list[Path]:
    """Busca todos los CSV bajo base/SYMBOL/tf_folder/*.csv."""
    pattern = "**/*.csv"
    files = sorted(base.glob(pattern))
    # Excluir archivos que no estén en una subcarpeta reconocida
    result = []
    for f in files:
        parts = f.relative_to(base).parts
        if len(parts) < 3:
            continue  # no encaja en la estructura esperada
        symbol = parts[0].upper()
        if symbol_filter and symbol != symbol_filter.upper():
            continue
        tf_folder = parts[1]
        if tf_folder not in _FOLDER_TO_TF:
            continue
        result.append(f)
    return result


def _parse_csv_path(f: Path, base: Path) -> tuple[str, str, str]:
    """Extrae (symbol, timeframe, tf_folder) de una ruta CSV."""
    parts = f.relative_to(base).parts
    symbol    = parts[0].upper()
    tf_folder = parts[1]
    timeframe = _FOLDER_TO_TF[tf_folder]
    return symbol, timeframe, tf_folder


def _parquet_path_for(csv_path: Path) -> Path:
    """Retorna la ruta .parquet correspondiente a un .csv."""
    return csv_path.with_suffix(".parquet")


# ── Timestamp conversion ──────────────────────────────────────────────────

def _to_ms_epoch(ts_series: pd.Series) -> pd.Series:
    """
    Convierte la columna timestamp de un CSV a int64 ms-desde-epoch UTC.

    Formatos soportados:
      - "2024-03-01 10:00:00+00:00"  (CSV writer actual)
      - "2024-03-01 10:00:00"        (sin tz → asume UTC)
      - int/float ya en ms-epoch     (pass-through)

    Nota pandas 3: datetime64[us, UTC].astype('int64') da microsegundos.
    Dividir entre 1_000 convierte a milisegundos.
    """
    if pd.api.types.is_integer_dtype(ts_series):
        return ts_series.astype("int64")
    parsed = pd.to_datetime(ts_series, utc=True, format="mixed")
    # astype('int64') devuelve microsegundos en pandas 3 (datetime64[us])
    # o nanosegundos en pandas < 2 (datetime64[ns]).
    # El unit del dtype nos indica la escala.
    unit = getattr(parsed.dtype, "unit", "ns")  # "us" en pandas 3, "ns" en pandas <2
    divisor = {"us": 1_000, "ns": 1_000_000, "ms": 1}.get(unit, 1_000_000)
    return (parsed.astype("int64") // divisor).astype("int64")


# ── Price conversion ──────────────────────────────────────────────────────

def _prices_to_int32(df: pd.DataFrame, timeframe: str, factor: int) -> pd.DataFrame:
    """
    Convierte columnas de precio de float a int32 escalado.

    Si las columnas ya son int (raw_prices=True), sólo hace cast.
    Si son float (CSV clásico), multiplica por factor y redondea.
    """
    price_cols = (
        ["ask", "bid"] if timeframe == "tick"
        else ["open", "high", "low", "close"]
    )
    df = df.copy()
    for col in price_cols:
        if col not in df.columns:
            continue
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].astype("int32")
        else:
            df[col] = (df[col] * factor).round().astype("int32")
    return df


# ── Writers ───────────────────────────────────────────────────────────────

def _write_parquet_pyarrow(
    df: pd.DataFrame,
    path: Path,
    symbol: str,
    timeframe: str,
    factor: int,
) -> None:
    """Escribe con pyarrow; sobreescritura atómica."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if timeframe == "tick":
        schema = pa.schema([
            pa.field("timestamp", pa.int64()),
            pa.field("ask",       pa.int32()),
            pa.field("bid",       pa.int32()),
            pa.field("ask_vol",   pa.float32()),
            pa.field("bid_vol",   pa.float32()),
        ])
    else:
        schema = pa.schema([
            pa.field("timestamp", pa.int64()),
            pa.field("open",      pa.int32()),
            pa.field("high",      pa.int32()),
            pa.field("low",       pa.int32()),
            pa.field("close",     pa.int32()),
            pa.field("volume",    pa.float32()),
        ])

    file_meta = {
        b"decimal_factor": str(factor).encode(),
        b"symbol":         symbol.upper().encode(),
        b"timeframe":      timeframe.encode(),
        b"source":         b"dukascopy",
        b"migrated_from":  b"csv",
        b"migration_ts":   datetime.now(tz=timezone.utc).isoformat().encode(),
    }
    schema_with_meta = schema.with_metadata(file_meta)
    table = pa.Table.from_pandas(df, schema=schema_with_meta, preserve_index=False)

    tmp = path.with_name(f"{path.stem}_{uuid.uuid4().hex}.tmp.parquet")
    try:
        pq.write_table(table, tmp, compression="zstd", compression_level=1)
        tmp.replace(path)
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise


def _write_parquet_polars(
    df: pd.DataFrame,
    path: Path,
    symbol: str,
    timeframe: str,
    factor: int,
) -> None:
    """Escribe con polars (fallback si pyarrow no está disponible).

    BUG #4a FIX: tras el sink_parquet de Polars, reinyecta la file-level
    metadata (decimal_factor, symbol, timeframe, source) usando PyArrow para
    que los archivos migrados sean autocontenidos, igual que los escritos por
    _write_parquet_pyarrow. Polars sink_parquet no expone metadata custom.
    Si PyArrow tampoco está disponible este path no se alcanza (el backend
    detection lo excluye), así que el import es seguro aquí.
    """
    import polars as pl

    # Cast explícito de tipos
    lf = pl.from_pandas(df).lazy()

    if timeframe == "tick":
        lf = (
            lf.with_columns([
                pl.col("timestamp").cast(pl.Int64),
                pl.col("ask").cast(pl.Int32),
                pl.col("bid").cast(pl.Int32),
                pl.col("ask_vol").cast(pl.Float32),
                pl.col("bid_vol").cast(pl.Float32),
            ])
        )
    else:
        lf = (
            lf.with_columns([
                pl.col("timestamp").cast(pl.Int64),
                pl.col("open").cast(pl.Int32),
                pl.col("high").cast(pl.Int32),
                pl.col("low").cast(pl.Int32),
                pl.col("close").cast(pl.Int32),
                pl.col("volume").cast(pl.Float32),
            ])
        )

    tmp = path.with_name(f"{path.stem}_{uuid.uuid4().hex}.tmp.parquet")
    try:
        lf.sink_parquet(tmp, compression="zstd", compression_level=1)

        # Reinyectar metadata con PyArrow (Polars no expone metadata custom)
        import pyarrow.parquet as pq
        import pyarrow as pa
        table     = pq.read_table(tmp)
        file_meta = {
            b"decimal_factor": str(factor).encode(),
            b"symbol":         symbol.upper().encode(),
            b"timeframe":      timeframe.encode(),
            b"source":         b"dukascopy",
            b"migrated_from":  b"csv",
            b"migration_ts":   datetime.now(tz=timezone.utc).isoformat().encode(),
        }
        # table.schema.metadata puede ser None si el Parquet no traía metadata;
        # `or {}` evita "'NoneType' object is not a mapping".
        table = table.replace_schema_metadata({**(table.schema.metadata or {}), **file_meta})
        meta_tmp = path.with_name(f"{path.stem}_{uuid.uuid4().hex}.meta.parquet")
        try:
            pq.write_table(table, meta_tmp, compression="zstd", compression_level=1)
            meta_tmp.replace(path)
        except Exception:
            if meta_tmp.exists():
                meta_tmp.unlink(missing_ok=True)
            raise
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise


# ── Streaming migration (memoria acotada) ─────────────────────────────────

def _price_and_vol_cols(timeframe: str) -> tuple[list[str], list[str]]:
    """Retorna (price_cols, vol_cols) según el timeframe."""
    if timeframe == "tick":
        return ["ask", "bid"], ["ask_vol", "bid_vol"]
    return ["open", "high", "low", "close"], ["volume"]


def _build_lazy_plan(csv_path: Path, timeframe: str, factor: int):
    """
    Construye un LazyFrame de Polars con todas las transformaciones:
      - timestamp str/int → int64 ms-epoch UTC
      - precios float → int32 escalado (o cast si ya son int)
      - volúmenes → float32
      - orden por timestamp

    No materializa datos: el sink_parquet posterior ejecuta el plan en
    streaming con memoria acotada.
    """
    import polars as pl

    lf = pl.scan_csv(csv_path)
    schema = lf.collect_schema()
    names = set(schema.names())

    # timestamp: pass-through si ya es entero (ms-epoch), si no parsear ISO→ms
    if schema["timestamp"].is_integer():
        ts_expr = pl.col("timestamp").cast(pl.Int64)
    else:
        ts_expr = (
            pl.col("timestamp")
            .str.to_datetime(time_zone="UTC")
            .dt.epoch(time_unit="ms")
            .cast(pl.Int64)
        )

    price_cols, vol_cols = _price_and_vol_cols(timeframe)

    exprs = [ts_expr.alias("timestamp")]
    for col in price_cols:
        if col not in names:
            continue
        if schema[col].is_integer():
            exprs.append(pl.col(col).cast(pl.Int32))
        else:
            exprs.append((pl.col(col) * factor).round(0).cast(pl.Int32).alias(col))
    for col in vol_cols:
        if col in names:
            exprs.append(pl.col(col).cast(pl.Float32))

    lf = lf.with_columns(exprs)

    final_cols = (
        ["timestamp"]
        + [c for c in price_cols if c in names]
        + [c for c in vol_cols if c in names]
    )
    return lf.select(final_cols).sort("timestamp")


def _finalize_with_metadata(
    tmp: Path,
    final: Path,
    symbol: str,
    timeframe: str,
    factor: int,
) -> None:
    """
    Reinyecta la file-level metadata al Parquet escrito por sink_parquet.

    Polars sink_parquet no expone metadata custom, así que se reescribe el
    archivo con PyArrow añadiendo la metadata. Para mantener la memoria
    acotada se copia row-group por row-group (NO se carga la tabla entera).
    Sobreescritura atómica vía archivo .meta temporal + rename.
    """
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(tmp)
    file_meta = {
        b"decimal_factor": str(factor).encode(),
        b"symbol":         symbol.upper().encode(),
        b"timeframe":      timeframe.encode(),
        b"source":         b"dukascopy",
        b"migrated_from":  b"csv",
        b"migration_ts":   datetime.now(tz=timezone.utc).isoformat().encode(),
    }
    existing = pf.schema_arrow.metadata or {}
    schema_with_meta = pf.schema_arrow.with_metadata({**existing, **file_meta})

    meta_tmp = final.with_name(f"{final.stem}_{uuid.uuid4().hex}.meta.parquet")
    writer = pq.ParquetWriter(
        meta_tmp, schema_with_meta, compression="zstd", compression_level=1
    )
    try:
        # iter_batches mantiene la memoria acotada (no carga el archivo entero)
        for batch in pf.iter_batches(batch_size=_ROW_GROUP_SIZE):
            writer.write_batch(batch)
        writer.close()
        meta_tmp.replace(final)
    except Exception:
        writer.close()
        if meta_tmp.exists():
            meta_tmp.unlink(missing_ok=True)
        raise
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _migrate_file_streaming(
    csv_path: Path,
    timeframe: str,
    symbol: str,
    factor: int,
    parquet_path: Path,
    dry_run: bool,
    result: dict,
) -> dict:
    """Migración con memoria acotada (Polars sink + PyArrow row-group copy)."""
    import polars as pl

    rows_in = pl.scan_csv(csv_path).select(pl.len()).collect().item()
    result["rows_in"] = rows_in

    if rows_in == 0:
        result["status"] = "skipped_empty"
        return result

    lf = _build_lazy_plan(csv_path, timeframe, factor)

    if dry_run:
        result["status"]   = "dry_run_ok"
        result["rows_out"] = rows_in
        return result

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = parquet_path.with_name(f"{parquet_path.stem}_{uuid.uuid4().hex}.tmp.parquet")
    try:
        lf.sink_parquet(
            tmp, compression="zstd", compression_level=1,
            row_group_size=_ROW_GROUP_SIZE,
        )
        _finalize_with_metadata(tmp, parquet_path, symbol, timeframe, factor)
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise

    rows_out = pl.scan_parquet(parquet_path).select(pl.len()).collect().item()
    result["rows_out"] = rows_out

    if rows_out != rows_in:
        parquet_path.unlink(missing_ok=True)
        raise ValueError(
            f"Discrepancia de filas: CSV={rows_in} vs Parquet={rows_out}. "
            "Archivo Parquet eliminado para evitar corrupción."
        )

    result["status"] = "ok"
    return result


# ── Core migration logic ──────────────────────────────────────────────────

def migrate_file(
    csv_path: Path,
    base: Path,
    factor_override: int | None,
    backend: str,
    dry_run: bool,
) -> dict:
    """
    Migra un único archivo CSV a Parquet.

    Returns
    -------
    dict con claves: csv, parquet, rows_in, rows_out, status, error
    """
    symbol, timeframe, tf_folder = _parse_csv_path(csv_path, base)
    factor = factor_override or _SYMBOL_FACTOR.get(symbol, _DEFAULT_FACTOR)
    parquet_path = _parquet_path_for(csv_path)

    result: dict = {
        "csv":      str(csv_path),
        "parquet":  str(parquet_path),
        "symbol":   symbol,
        "timeframe": timeframe,
        "rows_in":  None,
        "rows_out": None,
        "status":   "pending",
        "error":    None,
    }

    try:
        # Path preferido: streaming con memoria acotada (Polars + PyArrow).
        if _STREAMING_AVAILABLE:
            return _migrate_file_streaming(
                csv_path=csv_path,
                timeframe=timeframe,
                symbol=symbol,
                factor=factor,
                parquet_path=parquet_path,
                dry_run=dry_run,
                result=result,
            )

        # ── Fallback pandas (carga el archivo completo en memoria) ──────────
        # 1. Leer CSV
        df = pd.read_csv(csv_path, low_memory=False)
        rows_in = len(df)
        result["rows_in"] = rows_in

        if rows_in == 0:
            result["status"] = "skipped_empty"
            return result

        # 2. Transformar timestamp → int64 ms-epoch
        df["timestamp"] = _to_ms_epoch(df["timestamp"])

        # 3. Convertir precios → int32 escalado
        df = _prices_to_int32(df, timeframe, factor)

        # 4. Asegurar columnas de volumen como float32
        vol_cols = {"ask_vol", "bid_vol", "volume"}
        for col in vol_cols & set(df.columns):
            df[col] = df[col].astype("float32")

        # 5. Ordenar por timestamp
        df = df.sort_values("timestamp").reset_index(drop=True)

        if dry_run:
            result["status"] = "dry_run_ok"
            result["rows_out"] = rows_in
            return result

        # 6. Escribir Parquet
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        if backend == "pyarrow":
            _write_parquet_pyarrow(df, parquet_path, symbol, timeframe, factor)
        else:
            _write_parquet_polars(df, parquet_path, symbol, timeframe, factor)

        # 7. Verificar conteo de filas
        # BUG #4b FIX: pd.read_parquet requiere pyarrow o fastparquet. En el path
        # polars-only (backend="polars"), usar polars para la verificación para
        # evitar ImportError no capturado después de haber escrito el archivo.
        if backend == "pyarrow":
            rows_out = len(pd.read_parquet(parquet_path, columns=["timestamp"]))
        else:
            import polars as pl
            rows_out = pl.scan_parquet(parquet_path).select("timestamp").collect().height
        result["rows_out"] = rows_out

        if rows_out != rows_in:
            parquet_path.unlink(missing_ok=True)
            raise ValueError(
                f"Discrepancia de filas: CSV={rows_in} vs Parquet={rows_out}. "
                "Archivo Parquet eliminado para evitar corrupción."
            )

        result["status"] = "ok"

    except Exception as exc:
        result["status"] = "error"
        result["error"]  = str(exc)
        log.error("Error migrando %s: %s", csv_path, exc)

    return result


def write_migration_log(results: list[dict], log_path: Path) -> None:
    """Escribe migration.log con un resumen JSON por archivo."""
    with log_path.open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "migration_timestamp": datetime.now(tz=timezone.utc).isoformat(),
                "total_files": len(results),
                "ok":          sum(1 for r in results if r["status"] == "ok"),
                "errors":      sum(1 for r in results if r["status"] == "error"),
                "skipped":     sum(1 for r in results if r["status"] not in ("ok", "error")),
                "files":       results,
            },
            fh,
            indent=2,
            ensure_ascii=False,
        )


# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migra archivos CSV del pipeline Dukascopy a Parquet (one-shot).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--base", "-b", required=True, metavar="PATH",
                        help="Directorio raíz de datos (el mismo que --ruta en main.py).")
    parser.add_argument("--symbol", "-s", default=None, metavar="SYMBOL",
                        help="Filtrar por símbolo (e.g. EURUSD). Default: todos.")
    parser.add_argument("--decimal-factor", "-d", type=int, default=None,
                        metavar="N",
                        help="Factor de escala (e.g. 100000). Default: mapping por símbolo.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula la migración sin escribir ningún archivo.")
    parser.add_argument("--log-file", default=None, metavar="PATH",
                        help="Ruta del log de migración. Default: {base}/migration.log")
    args = parser.parse_args()

    base = Path(args.base)
    if not base.exists():
        print(f"ERROR: El directorio base no existe: {base}", file=sys.stderr)
        sys.exit(1)

    log_path = Path(args.log_file) if args.log_file else base / "migration.log"

    # Detectar backend
    try:
        backend = _detect_backend()
        log.info("Backend de escritura Parquet: %s", backend)
    except ImportError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)

    # Descubrir archivos
    csv_files = _find_csv_files(base, args.symbol)
    if not csv_files:
        print("No se encontraron archivos CSV en la estructura esperada.")
        sys.exit(0)

    print(f"Archivos CSV encontrados: {len(csv_files)}")
    if args.dry_run:
        print("Modo DRY-RUN activo: no se escribirá ningún archivo.")
    print()

    results: list[dict] = []
    errors = 0

    for csv_path in csv_files:
        rel = csv_path.relative_to(base)
        log.info("Procesando: %s", rel)
        result = migrate_file(
            csv_path=csv_path,
            base=base,
            factor_override=args.decimal_factor,
            backend=backend,
            dry_run=args.dry_run,
        )
        results.append(result)

        status_icon = "✓" if result["status"] == "ok" else (
            "~" if "dry_run" in result["status"] else "✗"
        )
        print(
            f"  {status_icon} {rel}  "
            f"({result['rows_in']} filas → {result['rows_out']} filas)"
            + (f"  ERROR: {result['error']}" if result["error"] else "")
        )
        if result["status"] == "error":
            errors += 1

    # Log
    write_migration_log(results, log_path)
    print(f"\nLog de migración escrito en: {log_path}")

    ok_count = sum(1 for r in results if r["status"] == "ok")
    print(f"\nResumen: {ok_count}/{len(results)} archivos migrados correctamente.")

    if errors:
        print(f"⚠  {errors} archivos con errores. Revisa {log_path}.")
        sys.exit(3)


if __name__ == "__main__":
    main()
