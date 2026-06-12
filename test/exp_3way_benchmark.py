"""
exp_3way_benchmark.py — Comparación estricta de 3 sistemas de descarga
=======================================================================

OBJETIVO
--------
Decidir, de forma reproducible y con resultados claros, CUÁL de los tres
sistemas es mejor, bajo el criterio ponderado acordado:

        INTEGRIDAD  >  ESTABILIDAD  >  VELOCIDAD   (orden lexicográfico)

Es decir: gana el que entrega datos completos (sin huecos silenciosos);
la estabilidad de conexión desempata; la velocidad solo desempata al final.

LOS TRES SISTEMAS
-----------------
  S_SEQ   "Secuencial simple"   workers=1, HTTP/1.1, 404 aceptado al instante
  S_ORIG  "Original paralelo"   workers=8, HTTP/2,   404 aceptado al instante
  S_FINAL "Final paralelo"      workers=4, HTTP/1.1, 404 reverificado (retry)

Los tres comparten exactamente el mismo cliente base (DukascopyClient); solo
cambian: nº de workers, http2 on/off, y max_404_retries.  Así la comparación
aísla las variables que importan y nada más.

DOS WORKLOADS (los pedidos)
---------------------------
  WORKLOAD A  "OHLC 15 años"   símbolos: EURUSD, USDJPY ; timeframes m15/h1/h4
              → primitivos reales: m1 (chunk diario) + h1 (chunk mensual).
                m15 se resamplea de m1; h4 se resamplea de h1 → NO se descargan
                aparte, así que el set de descarga primitivo es {m1, h1}.
  WORKLOAD B  "Ticks 1 semana" símbolos: EURUSD, USDJPY ; timeframe tick
              → chunk horario (HHh_ticks.bi5).

RIGOR EXPERIMENTAL (por qué los resultados son creíbles)
--------------------------------------------------------
  1. MISMO TRABAJO: los tres sistemas reciben la MISMA lista de chunks, en el
     mismo orden determinista (semilla fija).  Cero ventaja de muestreo.
  2. ROTACIÓN DE ORDEN: en cada ronda se rota qué sistema corre primero, para
     neutralizar el sesgo de estado del servidor (throttle/caché por IP que ya
     demostramos que existe).  Promediar sobre rondas elimina el orden.
  3. COOLDOWN: pausa entre sistemas para no arrastrar throttling de uno a otro.
  4. GROUND TRUTH: el "verdadero" set de chunks-con-datos = UNIÓN de todo lo
     que CUALQUIER sistema logró descargar con bytes no vacíos en CUALQUIER
     ronda.  Un sistema que devuelve None donde el ground truth SÍ tiene datos
     = HUECO SILENCIOSO → penalización de integridad.  Esto mide directamente
     la Teoría 2.
  5. INSTRUMENTACIÓN NO INVASIVA: un logging.Handler cuenta reintentos de red
     y reverificaciones de 404 sin tocar el código de producción.

SALIDA
------
  - Tabla por (sistema × workload × ronda) con métricas crudas.
  - Tabla agregada (promedio de rondas) por sistema × workload.
  - Veredicto final: ranking lexicográfico y el sistema ganador, con la razón.

USO
---
    python exp_3way_benchmark.py                # workloads completos (pesado)
    python exp_3way_benchmark.py --smoke        # versión mínima de humo
    python exp_3way_benchmark.py --rounds 3     # nº de rondas (default 2)
    python exp_3way_benchmark.py --years 3      # recorta años de OHLC (sanity)

NOTA DE ESCALA
--------------
  OHLC 15 años completo ≈ (≈5.475 chunks m1/símbolo + 180 chunks h1/símbolo)
  × 2 símbolos ≈ 11.300 descargas POR sistema × 3 sistemas × rondas.  Es
  ejecutable pero pesado y presiona al servidor.  --years y --smoke permiten
  validar el test antes de lanzar el full.  El default es FIEL a lo pedido.
"""

from __future__ import annotations

import argparse
import logging
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import config as _cfg
import dukascopy_client as dc

# ── Parámetros fijos del experimento ────────────────────────────────────────
SYMBOLS = ["EURUSD", "USDJPY"]
SEED = 20240611            # semilla fija → orden de chunks reproducible
COOLDOWN_S = 8.0          # pausa entre sistemas (neutraliza throttle arrastrado)

# Ventana de la semana de ticks (lunes–domingo, fija y reproducible).
TICK_WEEK_START = date(2024, 1, 8)   # lunes
TICK_WEEK_DAYS = 7

# Año final del bloque OHLC de 15 años.  El rango es [END-years+1 .. END].
OHLC_END_YEAR = 2024


# ════════════════════════════════════════════════════════════════════════════
#  Definición de los 3 sistemas (única fuente de diferencias)
# ════════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class SystemSpec:
    key: str
    label: str
    workers: int
    http2: bool
    max_404_retries: int


SYSTEMS = [
    SystemSpec("S_SEQ",   "Secuencial simple (1w, HTTP/1.1, 404 inmediato)", 1, False, 0),
    SystemSpec("S_ORIG",  "Original (8w, HTTP/2, 404 inmediato)",            8, True,  0),
    SystemSpec("S_FINAL", "Final (4w, HTTP/1.1, 404 reverificado)",          4, False, 2),
]


def build_client(spec: SystemSpec) -> dc.DukascopyClient:
    """Construye un DukascopyClient configurado según el sistema.

    DukascopyClient lee http2 y el tamaño del pool desde config en __init__,
    así que fijamos esos atributos ANTES de instanciar.  No se muta nada
    permanente: cada build sobreescribe y construye su propio cliente.
    """
    _cfg.HTTP2_ENABLED = spec.http2
    # pool >= workers para que cada worker tenga su conexión keep-alive propia
    _cfg.HTTPX_MAX_CONNECTIONS = max(spec.workers, 4)
    _cfg.HTTPX_MAX_KEEPALIVE_CONNECTIONS = max(spec.workers, 4)
    return dc.DukascopyClient(max_404_retries=spec.max_404_retries)


# ════════════════════════════════════════════════════════════════════════════
#  Enumeración determinista de chunks por workload
# ════════════════════════════════════════════════════════════════════════════
# Una "tarea de chunk" es la tripleta que download_chunk() espera.
@dataclass(frozen=True)
class ChunkTask:
    symbol: str
    timeframe: str          # primitivo real: "m1" | "h1" | "tick"
    dt: date | datetime

    def key(self) -> tuple:
        # Clave canónica para ground-truth / dedupe / detección de huecos.
        d = self.dt
        if isinstance(d, datetime):
            stamp = (d.year, d.month, d.day, d.hour)
        else:
            stamp = (d.year, d.month, d.day, -1)
        return (self.symbol, self.timeframe, stamp)


def enumerate_ohlc(years: int) -> list[ChunkTask]:
    """OHLC 15 años → descarga primitiva {m1 diario, h1 mensual} por símbolo.

    m15 se resamplea de m1 y h4 de h1, por lo que NO añaden descargas.  Esto
    refleja exactamente lo que hace el orchestrator real (ver _TF_CONFIG).
    """
    tasks: list[ChunkTask] = []
    start_year = OHLC_END_YEAR - years + 1
    for symbol in SYMBOLS:
        for y in range(start_year, OHLC_END_YEAR + 1):
            # h1: un chunk por mes
            for m in range(1, 13):
                tasks.append(ChunkTask(symbol, "h1", date(y, m, 1)))
            # m1: un chunk por día del año
            d = date(y, 1, 1)
            end = date(y, 12, 31)
            while d <= end:
                tasks.append(ChunkTask(symbol, "m1", d))
                d += timedelta(days=1)
    return tasks


def enumerate_ticks() -> list[ChunkTask]:
    """Ticks 1 semana → chunk horario por símbolo (7 días × 24 h)."""
    tasks: list[ChunkTask] = []
    for symbol in SYMBOLS:
        for day_off in range(TICK_WEEK_DAYS):
            d = TICK_WEEK_START + timedelta(days=day_off)
            for h in range(24):
                tasks.append(
                    ChunkTask(symbol, "tick", datetime(d.year, d.month, d.day, h))
                )
    return tasks


def ordered(tasks: list[ChunkTask]) -> list[ChunkTask]:
    """Orden determinista pero NO trivialmente secuencial (mezcla con semilla
    fija), idéntico para los 3 sistemas → trabajo perfectamente comparable."""
    rng = random.Random(SEED)
    out = list(tasks)
    rng.shuffle(out)
    return out


# ════════════════════════════════════════════════════════════════════════════
#  Instrumentación de estabilidad (no invasiva)
# ════════════════════════════════════════════════════════════════════════════
class StabilityCounter(logging.Handler):
    """Cuenta, leyendo los logs del cliente, los eventos de inestabilidad:
        - net_retries  : reintentos por error de red/transporte (incl. ReadError HTTP/2)
        - recheck_404  : reverificaciones de 404 (solo S_FINAL las dispara)
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.net_retries = 0
        self.recheck_404 = 0

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        if "Reintentando" in msg:
            self.net_retries += 1
        elif "Reverificando" in msg:
            self.recheck_404 += 1

    def reset(self) -> None:
        self.net_retries = 0
        self.recheck_404 = 0


# ════════════════════════════════════════════════════════════════════════════
#  Métricas de una corrida (sistema × workload × ronda)
# ════════════════════════════════════════════════════════════════════════════
@dataclass
class RunResult:
    system: str
    workload: str
    round_idx: int
    wall_s: float = 0.0
    attempted: int = 0
    ok: int = 0              # bytes no vacíos
    empty_none: int = 0      # None (404 aceptado como "sin datos")
    errors: int = 0          # ChunkDownloadError (fallo permanente)
    bytes_total: int = 0
    net_retries: int = 0
    recheck_404: int = 0
    # key -> True si devolvió bytes no vacíos (para construir ground truth)
    had_data: dict = field(default_factory=dict)


def run_once(spec: SystemSpec, workload: str, tasks: list[ChunkTask],
             round_idx: int, counter: StabilityCounter) -> RunResult:
    """Ejecuta UN sistema sobre UN workload, una vez."""
    counter.reset()
    client = build_client(spec)
    res = RunResult(spec.key, workload, round_idx)

    def fetch(t: ChunkTask):
        try:
            data = client.download_chunk(t.symbol, t.timeframe, t.dt)
            return t, data, None
        except dc.ChunkDownloadError as e:
            return t, None, e

    t0 = time.perf_counter()
    if spec.workers == 1:
        results = [fetch(t) for t in tasks]
    else:
        with ThreadPoolExecutor(max_workers=spec.workers) as ex:
            futures = [ex.submit(fetch, t) for t in tasks]
            results = [f.result() for f in as_completed(futures)]
    res.wall_s = time.perf_counter() - t0

    for t, data, err in results:
        res.attempted += 1
        if err is not None:
            res.errors += 1
        elif data is None:
            res.empty_none += 1
        elif len(data) == 0:
            res.empty_none += 1           # bytes vacíos == sin datos efectivo
        else:
            res.ok += 1
            res.bytes_total += len(data)
            res.had_data[t.key()] = True

    res.net_retries = counter.net_retries
    res.recheck_404 = counter.recheck_404
    client.close()
    return res


# ════════════════════════════════════════════════════════════════════════════
#  Orquestación del benchmark
# ════════════════════════════════════════════════════════════════════════════
def run_workload(workload: str, tasks: list[ChunkTask], rounds: int) -> list[RunResult]:
    counter = StabilityCounter()
    logging.getLogger("dukascopy_client").addHandler(counter)
    logging.getLogger("dukascopy_client").setLevel(logging.WARNING)

    all_runs: list[RunResult] = []
    for r in range(rounds):
        # ROTACIÓN: el sistema que va primero cambia cada ronda.
        order = SYSTEMS[r % len(SYSTEMS):] + SYSTEMS[:r % len(SYSTEMS)]
        print(f"\n  Ronda {r+1}/{rounds}  ·  orden: {[s.key for s in order]}")
        for spec in order:
            print(f"    → {spec.key:8s} ...", end="", flush=True)
            res = run_once(spec, workload, tasks, r, counter)
            all_runs.append(res)
            print(f" {res.wall_s:7.1f}s  ok={res.ok} none={res.empty_none} "
                  f"err={res.errors} net_retry={res.net_retries} "
                  f"404recheck={res.recheck_404}")
            time.sleep(COOLDOWN_S)   # cooldown anti-throttle
    logging.getLogger("dukascopy_client").removeHandler(counter)
    return all_runs


def ground_truth_keys(runs: list[RunResult]) -> set:
    """Unión de todos los chunks que CUALQUIER sistema/ronda probó tener datos."""
    truth: set = set()
    for run in runs:
        truth |= set(run.had_data.keys())
    return truth


# ── Agregación y scoring ────────────────────────────────────────────────────
@dataclass
class Aggregate:
    system: str
    workload: str
    rounds: int
    wall_s_avg: float
    attempted: int
    ok_avg: float
    none_avg: float
    err_avg: float
    bytes_avg: float
    net_retries_avg: float
    recheck_404_avg: float
    completeness: float       # ok cubierto / ground-truth total  (INTEGRIDAD)
    silent_gaps: float        # chunks con datos en truth que este sist. marcó None
    throughput: float         # chunks/s  (VELOCIDAD)
    instability: float        # (errores + net_retries) / attempted (ESTABILIDAD)


def aggregate(runs: list[RunResult], truth: set) -> list[Aggregate]:
    by_sys: dict[str, list[RunResult]] = defaultdict(list)
    for run in runs:
        by_sys[run.system].append(run)

    truth_total = max(len(truth), 1)
    aggs: list[Aggregate] = []
    for sys_key, sys_runs in by_sys.items():
        n = len(sys_runs)
        wall = sum(r.wall_s for r in sys_runs) / n
        ok = sum(r.ok for r in sys_runs) / n
        none = sum(r.empty_none for r in sys_runs) / n
        err = sum(r.errors for r in sys_runs) / n
        byt = sum(r.bytes_total for r in sys_runs) / n
        nret = sum(r.net_retries for r in sys_runs) / n
        rech = sum(r.recheck_404 for r in sys_runs) / n
        attempted = sys_runs[0].attempted

        # Huecos silenciosos: claves del ground truth que ESTE sistema nunca
        # logró traer con datos en NINGUNA de sus rondas.
        sys_truth_hit: set = set()
        for r in sys_runs:
            sys_truth_hit |= set(r.had_data.keys())
        missed = truth - sys_truth_hit
        silent_gaps = len(missed)
        completeness = len(sys_truth_hit & truth) / truth_total

        aggs.append(Aggregate(
            system=sys_key, workload=sys_runs[0].workload, rounds=n,
            wall_s_avg=wall, attempted=attempted, ok_avg=ok, none_avg=none,
            err_avg=err, bytes_avg=byt, net_retries_avg=nret, recheck_404_avg=rech,
            completeness=completeness, silent_gaps=float(silent_gaps),
            throughput=(attempted / wall if wall > 0 else 0.0),
            instability=((err + nret) / attempted if attempted else 0.0),
        ))
    return aggs


def rank(aggs: list[Aggregate]) -> list[Aggregate]:
    """Ranking LEXICOGRÁFICO: integridad > estabilidad > velocidad.

      1. completeness  ↓ (mayor mejor)
      2. silent_gaps   ↑ (menor mejor)
      3. instability   ↑ (menor mejor)
      4. throughput    ↓ (mayor mejor)
    """
    return sorted(
        aggs,
        key=lambda a: (-round(a.completeness, 6),
                       a.silent_gaps,
                       round(a.instability, 6),
                       -a.throughput),
    )


# ── Impresión ────────────────────────────────────────────────────────────────
def print_workload_report(name: str, aggs: list[Aggregate]) -> None:
    print(f"\n{'='*78}\n  RESULTADO · {name}\n{'='*78}")
    hdr = (f"{'sistema':8s} {'wall(s)':>9s} {'ok':>7s} {'none':>6s} {'err':>5s} "
           f"{'compl%':>7s} {'huecos':>7s} {'inestab':>8s} {'chunks/s':>9s}")
    print(hdr)
    print("-" * len(hdr))
    ranked = rank(aggs)
    for a in ranked:
        print(f"{a.system:8s} {a.wall_s_avg:9.1f} {a.ok_avg:7.0f} {a.none_avg:6.0f} "
              f"{a.err_avg:5.0f} {a.completeness*100:7.2f} {a.silent_gaps:7.0f} "
              f"{a.instability:8.3f} {a.throughput:9.2f}")
    win = ranked[0]
    print(f"\n  GANADOR ({name}): {win.system}")
    print(f"    integridad={win.completeness*100:.2f}%  huecos_silenciosos={win.silent_gaps:.0f}"
          f"  inestabilidad={win.instability:.3f}  velocidad={win.throughput:.2f} chunks/s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--years", type=int, default=15, help="años de OHLC (default 15)")
    ap.add_argument("--smoke", action="store_true",
                    help="versión mínima: 1 año OHLC + 1 día de ticks, 1 ronda")
    args = ap.parse_args()

    if args.smoke:
        args.years = 3
        args.rounds = 2

    logging.basicConfig(level=logging.CRITICAL)  # silencia ruido; el handler cuenta

    # ── Construir workloads (mismo orden determinista para los 3 sistemas) ──
    ohlc_tasks = ordered(enumerate_ohlc(args.years))
    tick_tasks = ordered(enumerate_ticks())
    if args.smoke:
        tick_tasks = tick_tasks[: 2 * 24]  # 1 día × 2 símbolos

    print(f"Config: rounds={args.rounds}  años_OHLC={args.years}  smoke={args.smoke}")
    print(f"  WORKLOAD A (OHLC {args.years}a): {len(ohlc_tasks)} chunks/sistema "
          f"× {len(SYSTEMS)} sistemas × {args.rounds} rondas "
          f"= {len(ohlc_tasks)*len(SYSTEMS)*args.rounds} descargas")
    print(f"  WORKLOAD B (Ticks 1 sem): {len(tick_tasks)} chunks/sistema "
          f"× {len(SYSTEMS)} sistemas × {args.rounds} rondas "
          f"= {len(tick_tasks)*len(SYSTEMS)*args.rounds} descargas")

    # ── WORKLOAD A: OHLC ────────────────────────────────────────────────────
    print(f"\n### WORKLOAD A — OHLC {args.years} años (m15/h1/h4 → m1+h1) ###")
    runs_a = run_workload("OHLC", ohlc_tasks, args.rounds)
    truth_a = ground_truth_keys(runs_a)
    aggs_a = aggregate(runs_a, truth_a)

    # ── WORKLOAD B: Ticks ────────────────────────────────────────────────────
    print(f"\n### WORKLOAD B — Ticks 1 semana ###")
    runs_b = run_workload("TICKS", tick_tasks, args.rounds)
    truth_b = ground_truth_keys(runs_b)
    aggs_b = aggregate(runs_b, truth_b)

    # ── Reportes ──────────────────────────────────────────────────────────────
    print_workload_report(f"OHLC {args.years} años", aggs_a)
    print_workload_report("Ticks 1 semana", aggs_b)

    # ── Veredicto global (combina ambos workloads con el mismo criterio) ──────
    combined: dict[str, Aggregate] = {}
    for a in aggs_a + aggs_b:
        if a.system not in combined:
            combined[a.system] = a
        else:
            c = combined[a.system]
            combined[a.system] = Aggregate(
                system=a.system, workload="GLOBAL", rounds=a.rounds,
                wall_s_avg=c.wall_s_avg + a.wall_s_avg,
                attempted=c.attempted + a.attempted,
                ok_avg=c.ok_avg + a.ok_avg, none_avg=c.none_avg + a.none_avg,
                err_avg=c.err_avg + a.err_avg, bytes_avg=c.bytes_avg + a.bytes_avg,
                net_retries_avg=c.net_retries_avg + a.net_retries_avg,
                recheck_404_avg=c.recheck_404_avg + a.recheck_404_avg,
                completeness=(c.completeness + a.completeness) / 2,
                silent_gaps=c.silent_gaps + a.silent_gaps,
                throughput=(c.throughput + a.throughput) / 2,
                instability=(c.instability + a.instability) / 2,
            )
    print_workload_report("VEREDICTO GLOBAL (A+B)", list(combined.values()))
    print("\nCriterio: INTEGRIDAD > ESTABILIDAD > VELOCIDAD (lexicográfico).")
    print("compl% = chunks-con-datos recuperados vs. ground truth (unión de los 3).")
    print("huecos = chunks con datos reales que el sistema marcó como vacíos (Teoría 2).")


if __name__ == "__main__":
    main()
