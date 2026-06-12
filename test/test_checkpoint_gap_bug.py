"""
test_checkpoint_gap_bug.py

Valida la corrección del bug de gap silencioso en el checkpoint del orchestrator.

Patrón vista-controlador
------------------------
  Controlador  : DownloadOrchestrator real (orchestrator.py subido + fix aplicado).
                 Se instancia con sus módulos reales: CheckpointManager,
                 FailureLogger.  Los módulos de red y escritura se parchean
                 con unittest.mock para aislar el comportamiento bajo prueba.

  Vista (assert): funciones _assert_* independientes que leen el estado
                  final (progress.json, failed.log) y formulan las aserciones.
                  Están desacopladas del controlador — sólo reciben la ruta
                  de salida y los valores esperados.

Dependencias que se parchean (no se instalan, no hay red):
  - DukascopyClient.download_chunk  → bytes vacíos (éxito) o ChunkDownloadError
  - GitHubScraper.scrape            → lista fija de Instrument
  - ParquetWriter / CsvWriter       → MagicMock (no escribe nada en disco)
  - tqdm                            → MagicMock (sin barra de progreso en CI)

Ejecución:
  python test_checkpoint_gap_bug.py
  pytest test_checkpoint_gap_bug.py -v
"""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))

# Mockear módulos de sistema que no están disponibles en el entorno de test
# (httpx, tqdm) ANTES de que orchestrator.py los importe a nivel de módulo.
# Esto es necesario porque Python resuelve los imports top-level al cargar
# el módulo, antes de que unittest.mock.patch pueda interceptarlos.
from unittest.mock import MagicMock as _MagicMock
# Stubs de módulos no disponibles en el entorno de test.
# parquet_writer se mockea como módulo completo para cortar la cadena
# pyarrow → pyarrow.compute → … que pandas también necesita internamente.
for _mod in ("httpx", "tqdm", "tqdm.auto", "polars", "parquet_writer"):
    if _mod not in sys.modules:
        sys.modules[_mod] = _MagicMock()

from checkpoint_manager import CheckpointManager


# ────────────────────────────────────────────────────────────────────────────
# Helpers de fixture (Controlador)
# ────────────────────────────────────────────────────────────────────────────

def _make_orchestrator(output_path: Path, day_behavior: dict[date, bool]):
    """
    Instancia un DownloadOrchestrator real con todos los colaboradores
    externos mockeados.

    day_behavior: {date: True=éxito, False=ChunkDownloadError}
    """
    from dukascopy_client import ChunkDownloadError

    def _fake_download(symbol, timeframe, dt):
        d = dt.date() if hasattr(dt, "date") else dt
        if day_behavior.get(d, True):
            return b""
        raise ChunkDownloadError(f"Simulado: fallo en {symbol}/{d}")

    mock_writer = MagicMock()
    mock_writer.write.return_value = None

    with (
        patch("orchestrator.DukascopyClient") as MockClient,
        patch("orchestrator.GitHubScraper"),
        patch("orchestrator.ParquetWriter", return_value=mock_writer),
        patch("orchestrator.CsvWriter",     return_value=mock_writer),
        patch("orchestrator.tqdm",          side_effect=lambda it, **kw: it),
    ):
        MockClient.return_value.download_chunk.side_effect = _fake_download
        from orchestrator import DownloadOrchestrator
        orch = DownloadOrchestrator(output_path=output_path, max_workers=4)

    # Restaurar el cliente mock después de __init__
    orch._client = MagicMock()
    orch._client.download_chunk.side_effect = _fake_download
    return orch


def _run_tasks(orch, tasks):
    """Ejecuta las tareas en paralelo y hace flush final, igual que run()."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(orch._execute_task, t): t for t in tasks}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception:
                pass
    orch._flush_checkpoints(force=True)


def _make_tasks(symbol, tf, days, decimal_factor=100_000):
    from orchestrator import Task
    return [Task(symbol=symbol, timeframe=tf, dt=d, decimal_factor=decimal_factor)
            for d in days]


# ────────────────────────────────────────────────────────────────────────────
# Vista: funciones de aserción puras
# ────────────────────────────────────────────────────────────────────────────

def _assert_checkpoint(output_path: Path, symbol: str, tf: str, expected: date | None):
    """Afirma que el checkpoint final para (symbol, tf) es exactamente expected."""
    progress_file = output_path / "progress.json"

    if expected is None:
        if not progress_file.exists():
            return
        with open(progress_file) as f:
            progress = json.load(f)
        got = progress.get(symbol, {}).get(tf)
        assert got is None, (
            f"Checkpoint debería ser None para {symbol}/{tf}, pero es {got}"
        )
        return

    assert progress_file.exists(), "progress.json no fue creado"
    with open(progress_file) as f:
        progress = json.load(f)
    got_raw = progress.get(symbol, {}).get(tf)
    assert got_raw is not None, f"No hay checkpoint para {symbol}/{tf}"
    got = date.fromisoformat(got_raw)
    assert got == expected, (
        f"Checkpoint incorrecto {symbol}/{tf}: obtenido={got}, esperado={expected}"
    )


def _assert_in_failed_log(output_path: Path, symbol: str, tf: str, dt: date):
    """Afirma que (symbol, tf, dt) aparece en failed.log."""
    log_path = output_path / "failed.log"
    assert log_path.exists(), "failed.log no fue creado"
    content = log_path.read_text()
    assert str(dt) in content, f"{symbol}/{tf}/{dt} no aparece en failed.log"


def _assert_not_in_failed_log(output_path: Path, dt: date):
    """Afirma que dt NO aparece en failed.log."""
    log_path = output_path / "failed.log"
    if not log_path.exists():
        return
    assert str(dt) not in log_path.read_text(), (
        f"{dt} apareció en failed.log pero no debería"
    )


# ────────────────────────────────────────────────────────────────────────────
# Tests
# ────────────────────────────────────────────────────────────────────────────

def test_gap_silencioso_dia_intermedio_falla():
    """
    Escenario principal del bug documentado.

    Checkpoint previo: día 1.
    day2 falla, day3 y day4 tienen éxito.

    Bug original : checkpoint avanzaba a day3 o day4 (gap silencioso).
    Fix esperado : checkpoint queda en day1 (no salta sobre el fallo).
    """
    SYMBOL, TF = "EURUSD", "m15"
    day1 = date(2024, 1, 1)   # checkpoint previo
    day2 = date(2024, 1, 2)   # FALLA
    day3 = date(2024, 1, 3)   # éxito
    day4 = date(2024, 1, 4)   # éxito

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        CheckpointManager(path).update(SYMBOL, TF, day1)

        orch = _make_orchestrator(path, {day2: False, day3: True, day4: True})
        _run_tasks(orch, _make_tasks(SYMBOL, TF, [day2, day3, day4]))

        _assert_checkpoint(path, SYMBOL, TF, expected=day1)
        _assert_in_failed_log(path, SYMBOL, TF, day2)

    print("  ✅ test_gap_silencioso_dia_intermedio_falla")


def test_no_regresion_todos_exitosos():
    """
    Con todos los días exitosos el checkpoint debe avanzar normalmente.
    """
    SYMBOL, TF = "EURUSD", "m15"
    day1 = date(2024, 1, 1)
    day2 = date(2024, 1, 2)
    day3 = date(2024, 1, 3)
    day4 = date(2024, 1, 4)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        CheckpointManager(path).update(SYMBOL, TF, day1)

        orch = _make_orchestrator(path, {day2: True, day3: True, day4: True})
        _run_tasks(orch, _make_tasks(SYMBOL, TF, [day2, day3, day4]))

        _assert_checkpoint(path, SYMBOL, TF, expected=day4)
        _assert_not_in_failed_log(path, day2)
        _assert_not_in_failed_log(path, day3)
        _assert_not_in_failed_log(path, day4)

    print("  ✅ test_no_regresion_todos_exitosos")


def test_primer_dia_falla_sin_checkpoint_previo():
    """
    Sin checkpoint previo, si el primer día falla los siguientes exitosos
    no deben generar checkpoint (hay un hueco desde el origen).
    """
    SYMBOL, TF = "GBPUSD", "m15"
    day1 = date(2024, 2, 1)   # FALLA, sin checkpoint previo
    day2 = date(2024, 2, 2)   # éxito
    day3 = date(2024, 2, 3)   # éxito

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)

        orch = _make_orchestrator(path, {day1: False, day2: True, day3: True})
        _run_tasks(orch, _make_tasks(SYMBOL, TF, [day1, day2, day3]))

        _assert_checkpoint(path, SYMBOL, TF, expected=None)
        _assert_in_failed_log(path, SYMBOL, TF, day1)

    print("  ✅ test_primer_dia_falla_sin_checkpoint_previo")


def test_multiples_simbolos_gaps_independientes():
    """
    Con dos símbolos en paralelo, el gap de uno no contamina al otro.

    EURUSD: day2 falla → checkpoint queda en day1.
    GBPUSD: todos éxito → checkpoint avanza a day4.
    """
    day1 = date(2024, 3, 1)
    day2 = date(2024, 3, 2)
    day3 = date(2024, 3, 3)
    day4 = date(2024, 3, 4)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        CheckpointManager(path).update("EURUSD", "m15", day1)
        CheckpointManager(path).update("GBPUSD", "m15", day1)

        # Construir orchestrator con behavior genérico; luego sobreescribir
        # el side_effect para diferenciar por símbolo.
        orch = _make_orchestrator(path, {})

        from dukascopy_client import ChunkDownloadError

        def _per_symbol_download(symbol, timeframe, dt):
            d = dt.date() if hasattr(dt, "date") else dt
            if symbol == "EURUSD" and d == day2:
                raise ChunkDownloadError(f"Simulado EURUSD {d}")
            return b""

        orch._client.download_chunk.side_effect = _per_symbol_download

        tasks = (
            _make_tasks("EURUSD", "m15", [day2, day3, day4]) +
            _make_tasks("GBPUSD", "m15", [day2, day3, day4])
        )
        _run_tasks(orch, tasks)

        _assert_checkpoint(path, "EURUSD", "m15", expected=day1)  # gap → no avanza
        _assert_checkpoint(path, "GBPUSD", "m15", expected=day4)  # sin gap → avanza
        _assert_in_failed_log(path, "EURUSD", "m15", day2)

    print("  ✅ test_multiples_simbolos_gaps_independientes")


# ────────────────────────────────────────────────────────────────────────────
# Entry point (compatible con pytest y ejecución directa)
# ────────────────────────────────────────────────────────────────────────────

_TESTS = [
    ("test_gap_silencioso_dia_intermedio_falla",    test_gap_silencioso_dia_intermedio_falla),
    ("test_no_regresion_todos_exitosos",            test_no_regresion_todos_exitosos),
    ("test_primer_dia_falla_sin_checkpoint_previo", test_primer_dia_falla_sin_checkpoint_previo),
    ("test_multiples_simbolos_gaps_independientes", test_multiples_simbolos_gaps_independientes),
]

if __name__ == "__main__":
    print(f"\nCorriendo {len(_TESTS)} tests...\n")
    results: dict[str, bool] = {}
    for name, fn in _TESTS:
        try:
            fn()
            results[name] = True
        except Exception as exc:
            print(f"  ❌ {name}\n     {exc}\n")
            results[name] = False

    print(f"\n{'='*62}")
    print("RESUMEN")
    print(f"{'='*62}")
    for name, passed in results.items():
        print(f"  {'✅' if passed else '❌'}  {name}")

    failed = [n for n, p in results.items() if not p]
    if failed:
        print(f"\n  {len(failed)} test(s) fallaron.")
        sys.exit(1)
    else:
        print(f"\n  Todos los tests pasaron. Listo para producción.")
        sys.exit(0)