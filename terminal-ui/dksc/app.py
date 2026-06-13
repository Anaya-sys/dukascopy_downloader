"""
DKSC Pipeline Control — app.py
Vista-Controlador: la Vista (este archivo) solo maneja UI y delega toda la
lógica al Controlador (DownloadOrchestrator y clases del proyecto).

Cambios respecto a la versión anterior:
  - _download_worker conectado al orquestador REAL (no simulado)
  - Progreso real: barra refleja done/total de tareas del orquestador
  - Clock muestra hora LOCAL de la computadora (no UTC)
  - Boot carga progress.json y muestra estado de checkpoint en el log
  - Log de terminal sigue el mismo estilo de test_finale.py (sin emojis)
  - Subclase _GUIOrchestrator inyecta callback de progreso; no toca orchestrator.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import queue
import tkinter as tk
import tkinter.font as tkfont
import tkinter.filedialog as fd
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import customtkinter as ctk

# ── sys.path: garantizar que el paquete backend sea importable ────────────────
# app.py vive en dukascopy_downloader/terminal-ui/dksc/ (o similar).
# Los módulos backend (config, orchestrator, etc.) viven en el directorio raíz
# del proyecto (donde está config.py). Buscamos hacia arriba hasta encontrarlo.
def _find_project_root() -> Path | None:
    """Sube desde __file__ hasta encontrar el directorio que contiene config.py."""
    candidate = Path(__file__).resolve().parent
    for _ in range(6):   # máximo 6 niveles hacia arriba
        if (candidate / "config.py").exists():
            return candidate
        parent = candidate.parent
        if parent == candidate:   # llegamos al filesystem root
            break
        candidate = parent
    return None

_PROJECT_ROOT = _find_project_root()
if _PROJECT_ROOT is not None:
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))
    print(f"[DKSC] backend root: {_PROJECT_ROOT}")
else:
    print("[DKSC] WARNING: config.py not found in any parent directory — imports may fail")

try:
    from . import theme as T
    from .feed import AlphaVantageFeed, fmt_price
except ImportError:
    import theme as T
    from feed import AlphaVantageFeed, fmt_price

ctk.set_appearance_mode("dark")


# ── UI log handler (BUG-3 FIX) ───────────────────────────────────────────────
# Redirige mensajes WARNING+ del logging estándar al panel SYSTEM LOG de la app.
# Se instala en _boot_logs() una vez que add_log() ya está disponible.
class _UILogHandler(logging.Handler):
    """Puente entre logging estándar y el panel SYSTEM LOG de la UI."""

    def __init__(self, add_log_fn):
        super().__init__(level=logging.WARNING)
        self._add = add_log_fn
        self.setFormatter(logging.Formatter("%(name)s: %(message)s"))

    def emit(self, record):
        try:
            typ = "err" if record.levelno >= logging.ERROR else "warn"
            msg = self.format(record)
            self._add(f"[LOG] {msg}", typ)
        except Exception:
            pass  # nunca romper la app por un handler de logging


# ── Confirmation Dialog ───────────────────────────────────────────────────────
# Solo para RECONSTRUCT y MIGRATE — las acciones más confusas/destructivas.
# Diseño congruente con la app: BG0/BG1/BG2, mono font, colores del tema.
class ConfirmDialog(ctk.CTkToplevel):
    """
    Modal de confirmación bloqueante para acciones irreversibles (RECONSTRUCT, MIGRATE).
    Retorna True si el usuario confirmó, False si canceló o cerró la ventana.
    """

    def __init__(self, parent, title: str, lines: list[str], confirm_color: str):
        super().__init__(parent)
        self.result = False

        self.title(title)
        self.resizable(False, False)
        self.configure(fg_color=T.BG0)
        self.transient(parent)
        self.grab_set()            # bloquea interacción con la ventana principal

        # ── Cabecera ─────────────────────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color=T.BG2, corner_radius=0, height=28)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text=title, text_color=confirm_color,
                     font=T.f_sans(9, "bold"), fg_color="transparent"
                     ).pack(side="left", padx=10, pady=4)
        ctk.CTkFrame(hdr, fg_color=T.BORDER, width=1).pack(side="left", fill="y")

        # ── Separador ────────────────────────────────────────────────────────
        ctk.CTkFrame(self, fg_color=T.BORDER, height=1).pack(fill="x")

        # ── Cuerpo del mensaje ───────────────────────────────────────────────
        body = ctk.CTkFrame(self, fg_color=T.BG1, corner_radius=0)
        body.pack(fill="both", expand=True, padx=0, pady=0)

        for line in lines:
            color = T.MUTED
            if line.startswith(">>"):
                color = T.WHITE
            elif line.startswith("!"):
                color = T.AMBER
            ctk.CTkLabel(body, text=line, text_color=color,
                         font=T.f_mono(9), fg_color="transparent",
                         anchor="w").pack(anchor="w", padx=14, pady=(4, 0))

        ctk.CTkFrame(body, fg_color=T.BG0, height=8).pack(fill="x")

        # ── Separador ────────────────────────────────────────────────────────
        ctk.CTkFrame(self, fg_color=T.BORDER, height=1).pack(fill="x")

        # ── Botones ──────────────────────────────────────────────────────────
        btn_row = ctk.CTkFrame(self, fg_color=T.BG0, corner_radius=0)
        btn_row.pack(fill="x")

        ctk.CTkButton(
            btn_row, text="CANCEL", command=self._cancel,
            corner_radius=0, fg_color=T.BG0, hover_color=T.BG3,
            text_color=T.MUTED, font=T.f_mono(9), height=28, border_width=0,
            width=100,
        ).pack(side="right", padx=(0, 8), pady=6)

        ctk.CTkButton(
            btn_row, text="CONFIRM", command=self._confirm,
            corner_radius=0, fg_color=T.BG2, hover_color=T.BG3,
            text_color=confirm_color, font=T.f_mono(9, "bold"), height=28,
            border_width=1, border_color=confirm_color, width=100,
        ).pack(side="right", padx=(0, 4), pady=6)

        # ── Centrar sobre la ventana padre ───────────────────────────────────
        self.update_idletasks()
        pw = parent.winfo_width()
        ph = parent.winfo_height()
        px = parent.winfo_rootx()
        py = parent.winfo_rooty()
        dw = self.winfo_reqwidth()
        dh = self.winfo_reqheight()
        self.geometry(f"{dw}x{dh}+{px + pw//2 - dw//2}+{py + ph//2 - dh//2}")

        # Bind Escape y cierre de ventana como CANCEL
        self.bind("<Escape>", lambda e: self._cancel())
        self.protocol("WM_DELETE_WINDOW", self._cancel)

        self.wait_window()   # bloquea hasta que el diálogo se cierre

    def _confirm(self):
        self.result = True
        self.destroy()

    def _cancel(self):
        self.result = False
        self.destroy()


def _confirm_dialog(parent, title: str, lines: list[str],
                    confirm_color: str) -> bool:
    """Crea el diálogo y retorna True si el usuario confirmó."""
    dlg = ConfirmDialog(parent, title, lines, confirm_color)
    return dlg.result


# ── Small widget helpers ──────────────────────────────────────────────────────
def frame(master, fg=T.BG1, **kw):
    return ctk.CTkFrame(master, fg_color=fg, corner_radius=0,
                        border_width=0, **kw)


def label(master, text, color=T.WHITE, font=None, **kw):
    return ctk.CTkLabel(master, text=text, text_color=color,
                        font=font or T.f_sans(10), fg_color="transparent", **kw)


# ── GUI-aware orchestrator subclass ──────────────────────────────────────────
# Overrides only run() to feed real progress to the UI via a callback.
# All task execution (_execute_task, _execute_tick_day, etc.) is inherited
# unchanged from DownloadOrchestrator — no backend logic duplicated here.
class _GUIOrchestrator:
    """
    Thin wrapper around DownloadOrchestrator that adds UI callbacks.
    Constructed lazily inside the worker thread; imports happen there
    so the main thread never blocks on heavy imports.
    """

    def __init__(self, output_path, max_workers, max_retries, storage_fmt,
                 on_log, on_progress, on_task_done, on_finalize_step, on_done,
                 stop_event: threading.Event | None = None):
        # Callbacks — all called from the worker thread via self.after() in app
        self._on_log          = on_log            # (msg, typ)
        self._on_progress     = on_progress       # (done, total)
        self._on_task_done    = on_task_done       # (task_label|None, failed:bool, err_count:int)
        self._on_finalize_step = on_finalize_step # (sym, tf, i, total)
        self._on_done         = on_done            # (interrupted, paused)
        self._stop_event      = stop_event or threading.Event()

        self._output      = Path(output_path)
        self._max_workers = max_workers
        self._max_retries = max_retries
        self._storage_fmt = storage_fmt

    def run(self, instruments, timeframes, override_start_date: str | None = None,
            date_to: str | None = None):
        """
        Real download loop.  Mirrors DownloadOrchestrator.run() pero:
          - acepta instruments pre-fetched (scrapeados por el worker antes de llamar)
          - dispara UI callbacks en vez de tqdm
          - no requiere tqdm
          - respeta self._stop_event para pausa limpia

        override_start_date : "YYYY-MM-DD" o None.
        date_to             : "YYYY-MM-DD" o None (se guarda en checkpoint como metadata).
        """
        import config as _cfg
        from orchestrator import DownloadOrchestrator
        from checkpoint_manager import CheckpointManager
        from datetime import date

        _override_date: date | None = None
        if override_start_date:
            try:
                _override_date = date.fromisoformat(override_start_date)
            except ValueError:
                pass

        _date_to: date | None = None
        if date_to:
            try:
                _date_to = date.fromisoformat(date_to)
            except ValueError:
                pass

        # Patch config.STORAGE_FORMAT for this run
        _cfg.STORAGE_FORMAT = self._storage_fmt

        orch = DownloadOrchestrator(
            output_path=self._output,
            max_workers=self._max_workers,
            max_retries=self._max_retries,
        )

        # Guardar metadatos de sesión (fecha inicio/fin del usuario) en el checkpoint
        _ck = orch._checkpoint
        _session_date_from = _override_date or date.today()
        _session_date_to   = _date_to or date.today()
        _ck.save_session(_session_date_from, _session_date_to, paused=False)

        # Build tasks
        self._on_log("Building task list...", "info")
        tasks = orch._build_tasks(instruments, timeframes,
                                  override_start_date=_override_date)
        total = len(tasks)

        if total == 0:
            self._on_log("No tasks — all data is up to date.", "ok")
            self._on_done(False, False)
            return

        self._on_log(f"{total:,} tasks queued  [workers: {self._max_workers}]", "info")
        self._on_progress(0, total)

        done_count = 0
        err_count  = 0
        interrupted = False
        paused = False

        def _execute_and_report(task):
            orch._execute_task(task)

        try:
            with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                futures = {pool.submit(_execute_and_report, t): t for t in tasks}
                for fut in as_completed(futures):
                    # Chequear señal de pausa/stop ANTES de procesar el resultado
                    if self._stop_event.is_set():
                        paused = True
                        # Cancelar futures pendientes (los que ya están ejecutando
                        # terminan solos; los no iniciados se cancelan)
                        for f in futures:
                            f.cancel()
                        break

                    task = futures[fut]
                    task_failed = False
                    try:
                        fut.result()
                    except Exception as exc:
                        task_failed = True
                        self._on_log(
                            f"TASK ERROR {task.symbol}/{task.timeframe}/{task.dt}: {exc}",
                            "err")
                    done_count += 1
                    if task_failed:
                        err_count += 1
                        self._on_task_done(
                            f"{task.symbol}/{task.timeframe}/{task.dt}", True, err_count)
                    else:
                        if done_count % 50 == 0 or done_count == total:
                            self._on_task_done(
                                f"{task.symbol}/{task.timeframe}/{task.dt}", False, err_count)
                        else:
                            self._on_task_done(None, False, err_count)
                    self._on_progress(done_count, total)
        except Exception as exc:
            interrupted = True
            self._on_log(f"Download interrupted: {exc}", "warn")
        finally:
            orch._flush_checkpoints(force=True)
            # Marcar paused en el checkpoint para que la UI lo restaure al recargar
            if paused:
                _ck.set_paused(True)

        if not interrupted and not paused:
            self._on_log("Download complete. Consolidating chunks...", "ok")

        # Finalize solo si no fue pausado (los chunks quedan para la siguiente reanudación)
        if not paused:
            finalize_targets = sorted({(t.symbol, t.timeframe) for t in tasks})
            n_fin = len(finalize_targets)
            for i, (sym, tf) in enumerate(finalize_targets, 1):
                try:
                    orch._writer.finalize(sym, tf)
                    self._on_finalize_step(sym, tf, i, n_fin)
                except Exception as exc:
                    self._on_log(f"Finalize error {sym}/{tf}: {exc}", "err")

            fail_path = self._output / "failed.log"
            if fail_path.exists() and fail_path.stat().st_size > 0:
                lines = fail_path.read_text(encoding="utf-8").strip().splitlines()
                self._on_log(f"{len(lines)} chunk(s) failed — see failed.log", "warn")

        self._on_done(interrupted, paused)


# ── Main application ──────────────────────────────────────────────────────────
class DKSCTerminal(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("DKSC")
        self.geometry(f"{T.WIN_W}x{T.WIN_H}")
        self.resizable(False, False)
        self.configure(fg_color=T.BG0)

        # state
        self.selected_tfs = {"tick", "m1", "m15", "h1", "h4"}
        self.log_count = 0
        self.done = self.pend = self.err = self.ckpt = 0
        self.running = False
        self.paused = False                        # True mientras la descarga está pausada
        self._stop_event = threading.Event()       # set() → señal de pausa al worker
        self.current_fmt = "parquet"
        self.symbol = ""
        self.tf_defs = [("tick", "TICK"), ("m1", "M1"), ("m15", "M15"),
                        ("h1", "H1"), ("h4", "H4")]

        # instrument meta-data
        self._instr_meta = self._load_instrument_meta()
        self._search_popup_frame = None  # FIX BUG-1: nombre canónico; init antes de _build_left

        # scope state
        self.scope_mode = "single"
        self.scope_tickers = []
        self.output_folder = os.path.expanduser("~")

        # feed
        self.feed = AlphaVantageFeed()
        self.feed.set_log(self._feed_log)

        # ticker animation state
        self._tick_offset = 0.0
        self._tick_segments = []
        self._tick_total = 0

        self._build_topbar()
        self._build_menubar()
        self._build_workspace()
        self._build_ticker()
        self._select_default_instrument()

        # boot sequence
        self._boot_logs()
        self._tick_clock()
        self._animate_ticker()
        self.after(800, self._refresh_ticker_data)

        self.feed.start()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ===================================================================== #
    # INSTRUMENT META-DATA
    # ===================================================================== #
    def _load_instrument_meta(self):
        # Candidatos en orden de prioridad:
        #   1. _PROJECT_ROOT (donde vive config.py) — la ubicación canónica del JSON
        #   2. CWD — útil cuando la app se lanza desde la raíz del proyecto
        #   3. Subiendo desde __file__ hasta 6 niveles — cobertura para layouts alternativos
        candidates: list[str] = []
        if _PROJECT_ROOT is not None:
            candidates.append(str(_PROJECT_ROOT / "instrument-meta-data.json"))
        candidates.append(os.path.join(os.getcwd(), "instrument-meta-data.json"))

        search = os.path.dirname(os.path.abspath(__file__))
        for _ in range(6):
            candidates.append(os.path.join(search, "instrument-meta-data.json"))
            parent = os.path.dirname(search)
            if parent == search:
                break
            search = parent

        # Deduplicar preservando orden
        seen: set[str] = set()
        unique: list[str] = []
        for c in candidates:
            if c not in seen:
                seen.add(c)
                unique.append(c)

        for candidate in unique:
            if os.path.isfile(candidate):
                try:
                    with open(candidate, encoding="utf-8") as f:
                        data = json.load(f)
                    print(f"[DKSC] instrument-meta-data loaded: {candidate} ({len(data)} instruments)")
                    return data
                except Exception as exc:
                    print(f"[DKSC] failed to load {candidate}: {exc}")
        print("[DKSC] instrument-meta-data.json not found — search disabled")
        return {}

    def _search_instruments(self, query: str, max_results: int = 50):
        q = query.strip().lower()
        if not q:
            return []
        results = []
        for key, meta in self._instr_meta.items():
            name = meta.get("name", "")
            desc = meta.get("description", "")
            if (q in key.lower() or q in name.lower() or q in desc.lower()):
                results.append((key, name, desc))
            if len(results) >= max_results:
                break
        return results

    def _select_default_instrument(self):
        if not self._instr_meta:
            return
        key = "eurusd" if "eurusd" in self._instr_meta else next(iter(self._instr_meta))
        meta = self._instr_meta[key]
        self._select_instrument(key, meta.get("name", key.upper()), meta.get("description", ""))

    # ===================================================================== #
    # TOPBAR
    # ===================================================================== #
    def _build_topbar(self):
        bar = frame(self, fg=T.BG1, height=T.TOPBAR_H, width=T.WIN_W)
        bar.place(x=0, y=0)
        bar.pack_propagate(False)

        def seg(parent):
            s = frame(parent, fg=T.BG1)
            s.pack(side="left", fill="y", padx=(0, 0))
            return s

        # brand
        s = seg(bar)
        label(s, "DKSC", T.AMBER, T.f_sans(11, "bold")).pack(side="left", padx=(6, 6), pady=4)
        self._vsep(bar)

        # conn status
        s = seg(bar)
        self.conn_dot = tk.Canvas(s, width=8, height=8, bg=T.BG1, highlightthickness=0)
        self.conn_dot.create_oval(1, 1, 7, 7, fill=T.GREEN, outline="")
        self.conn_dot.pack(side="left", padx=(8, 4))
        label(s, "LIVE", T.WHITE, T.f_sans(9)).pack(side="left", padx=(0, 10))
        self._vsep(bar)

        # pills
        for txt in ("PARQUET/ZSTD", "HTTP/1.1", "RLock"):
            s = seg(bar)
            pill = label(s, txt, T.MUTED, T.f_sans(8))
            pill.configure(fg_color=T.BG2)
            pill.pack(side="left", padx=8, pady=4, ipadx=4, ipady=1)
            if txt == "PARQUET/ZSTD":
                self.mode_pill = pill
            self._vsep(bar)

        # right: local clock + date
        r = frame(bar, fg=T.BG1)
        r.pack(side="right", fill="y")
        self.clock_lbl = label(r, "--:--:--", T.AMBER, T.f_mono(10, "bold"))
        self.clock_lbl.pack(side="left", padx=(0, 10))
        self.date_lbl = label(r, "-- --- ----", T.LABEL, T.f_sans(8))
        self.date_lbl.pack(side="left", padx=(0, 10))

    def _vsep(self, parent):
        sep = frame(parent, fg=T.BORDER, width=1)
        sep.pack(side="left", fill="y")

    def _vsep_center(self, parent):
        sep = frame(parent, fg=T.BORDER, width=1)
        sep.pack(side="left", fill="y")

    # ===================================================================== #
    # MENUBAR
    # ===================================================================== #
    def _build_menubar(self):
        pass

    def _select_menu(self, name):
        pass

    # ===================================================================== #
    # WORKSPACE
    # ===================================================================== #
    def _build_workspace(self):
        y = T.TOPBAR_H
        ws_h = T.WORKSPACE_H + T.MENUBAR_H
        ws = frame(self, fg=T.BG0, width=T.WIN_W, height=ws_h)
        ws.place(x=0, y=y)
        self._build_left(ws)
        self._build_center(ws)
        # dummy labels so worker threads can call .configure() safely
        self.m_workers = label(ws, "", T.AMBER)
        self.m_ckpt = label(ws, "", T.WHITE)

    def _panel_header(self, parent, lbl, tag=None, right=None):
        h = frame(parent, fg=T.BG2, height=22)
        h.pack(fill="x")
        h.pack_propagate(False)
        label(h, lbl, T.AMBER, T.f_sans(9, "bold")).pack(side="left", padx=8)
        if tag:
            t = label(h, tag, T.LABEL, T.f_sans(8))
            t.configure(fg_color=T.BG0)
            t.pack(side="left", padx=4, ipadx=4)
        rl = None
        if right is not None:
            rl = label(h, right, T.LABEL, T.f_sans(9))
            rl.pack(side="right", padx=8)
        frame(parent, fg=T.BORDER, height=1).pack(fill="x")
        return rl

    # ----------------------------------------------------------------- LEFT
    def _build_left(self, ws):
        panel = frame(ws, fg=T.BG1, width=T.COL_LEFT_W, height=T.WORKSPACE_H + T.MENUBAR_H)
        panel.place(x=0, y=0)
        panel.pack_propagate(False)
        frame(panel, fg=T.BORDER, width=1, height=T.WORKSPACE_H + T.MENUBAR_H).place(x=T.COL_LEFT_W - 1, y=0)

        self.sym_display = self._panel_header(panel, "CONFIG", "INSTRUMENT", self.symbol)

        body = ctk.CTkScrollableFrame(panel, fg_color=T.BG1, corner_radius=0,
                                      width=T.COL_LEFT_W - 16, height=T.WORKSPACE_H + T.MENUBAR_H - 23)
        body.pack(fill="both", expand=True)

        # Instrument search field
        self._field_label(body, "INSTRUMENT SEARCH")
        search_wrap = frame(body, fg=T.BG0)
        search_wrap.pack(fill="x", padx=8, pady=(0, 0))
        self.sym_entry = ctk.CTkEntry(search_wrap, fg_color=T.BG0, border_color=T.BORDER,
                                      border_width=1, corner_radius=0, text_color=T.WHITE,
                                      font=T.f_mono(11), height=26)
        self.sym_entry.pack(fill="x")
        self.sym_entry.bind("<KeyRelease>", self._on_sym_keyrelease)
        self.sym_entry.bind("<FocusOut>", self._close_search_popup)
        self.sym_entry.bind("<Escape>", self._close_search_popup)

        _act = frame(search_wrap, fg=T.BG0)
        _act.pack(fill="x", pady=(1, 0))
        ctk.CTkButton(_act, text="x CLEAR", command=self._clear_sym_entry,
                      corner_radius=0, fg_color=T.BG0, hover_color=T.BG3,
                      text_color=T.LABEL, font=T.f_mono(8), height=14,
                      border_width=0).pack(side="left")

        # _search_popup_frame already initialised in __init__ (before _build_left)
        self._search_result_labels = []

        self._instr_name_lbl = label(body, "", T.MUTED, T.f_sans(8))
        self._instr_name_lbl.pack(anchor="w", padx=8, pady=(2, 6))

        # Date from / to
        self._field_label(body, "DATE FROM")
        self.date_from = ctk.CTkEntry(body, fg_color=T.BG0, border_color=T.BORDER,
                                      border_width=1, corner_radius=0, text_color=T.WHITE,
                                      font=T.f_mono(11), height=26)
        self.date_from.pack(fill="x", padx=8, pady=(0, 8))

        self._field_label(body, "DATE TO")
        self.date_to = ctk.CTkEntry(body, fg_color=T.BG0, border_color=T.BORDER,
                                    border_width=1, corner_radius=0, text_color=T.WHITE,
                                    font=T.f_mono(11), height=26)
        self.date_to.pack(fill="x", padx=8, pady=(0, 8))
        self._init_dates()

        # Workers
        self._field_label(body, "WORKERS")
        self.workers_sel = ctk.CTkOptionMenu(body, values=["2", "3", "4", "5", "6"],
                                             fg_color=T.BG0, button_color=T.BG3,
                                             button_hover_color=T.BORDER_HI,
                                             text_color=T.WHITE, corner_radius=0,
                                             font=T.f_mono(11), height=26,
                                             command=lambda v: self.ib_workers.configure(text=v))
        self.workers_sel.set("4")
        self.workers_sel.pack(fill="x", padx=8, pady=(0, 8))

        # Storage format toggle
        self._field_label(body, "STORAGE FORMAT")
        ftog = frame(body, fg=T.BG1)
        ftog.pack(fill="x", padx=8, pady=(0, 8))
        self.fmt_parquet = self._toggle_opt(ftog, "PARQUET", True, lambda: self._set_format("parquet"))
        self.fmt_parquet.pack(side="left", expand=True, fill="x")
        self.fmt_csv = self._toggle_opt(ftog, "CSV", False, lambda: self._set_format("csv"))
        self.fmt_csv.pack(side="left", expand=True, fill="x")

        # Timeframes
        tfh = frame(body, fg=T.BG2, height=22)
        tfh.pack(fill="x")
        tfh.pack_propagate(False)
        label(tfh, "TIMEFRAMES", T.AMBER, T.f_sans(9, "bold")).pack(side="left", padx=8)
        self.tf_count = label(tfh, "5/5", T.LABEL, T.f_sans(9))
        self.tf_count.pack(side="right", padx=8)

        grid = frame(body, fg=T.BG1)
        grid.pack(fill="x", pady=(4, 8))
        self.tf_widgets = {}
        for i, (tid, lbl_) in enumerate(self.tf_defs):
            w = self._tf_item(grid, tid, lbl_)
            w.grid(row=i // 2, column=i % 2, sticky="ew", padx=2, pady=2)
            grid.grid_columnconfigure(i % 2, weight=1)

        # Buttons
        bgrid = frame(body, fg=T.BG1)
        bgrid.pack(fill="x", pady=(4, 4))
        bgrid.grid_columnconfigure(0, weight=1, uniform="btn")
        bgrid.grid_columnconfigure(1, weight=1, uniform="btn")
        self._dl_btn = self._btn(bgrid, "> DOWNLOAD", T.GREEN, self.start_download)
        self._dl_btn.grid(row=0, column=0, sticky="ew", padx=2, pady=(0, 2))
        self._pause_btn = self._btn(bgrid, "|| PAUSE", T.AMBER, self._toggle_pause)
        self._pause_btn.grid(row=0, column=1, sticky="ew", padx=2, pady=(0, 2))
        self._pause_btn.configure(state="disabled", text_color=T.BORDER)
        self._btn(bgrid, "~ RECONSTRUCT", T.AMBER, self.run_reconstruct).grid(row=1, column=0, sticky="ew", padx=2, pady=(0, 2))
        self._btn(bgrid, "< MIGRATE",     T.MUTED, self.run_migrate).grid(row=1, column=1, sticky="ew", padx=2, pady=(0, 2))
        bgrid.grid_columnconfigure(0, weight=1, uniform="btn")
        bgrid.grid_columnconfigure(1, weight=1, uniform="btn")
        self._btn(bgrid, "X CLEAR LOG", T.RED, self.clear_log).grid(row=2, column=0, columnspan=2, sticky="ew", padx=2, pady=(2, 0))

        # SCOPE section
        sc_h = frame(body, fg=T.BG2, height=22)
        sc_h.pack(fill="x", pady=(6, 0))
        sc_h.pack_propagate(False)
        label(sc_h, "SCOPE", T.AMBER, T.f_sans(9, "bold")).pack(side="left", padx=8)

        scope_row = frame(body, fg=T.BG1)
        scope_row.pack(fill="x", pady=(4, 0))

        scope_tog = frame(scope_row, fg=T.BG1)
        scope_tog.pack(fill="x", padx=8, pady=(0, 4))
        self._scope_single_btn = self._toggle_opt(
            scope_tog, "SINGLE", True, self._scope_set_single)
        self._scope_single_btn.pack(side="left", expand=True, fill="x")
        self._scope_all_btn = self._toggle_opt(
            scope_tog, "ALL", False, self._scope_set_all)
        self._scope_all_btn.pack(side="left", expand=True, fill="x")
        self._scope_custom_btn = self._toggle_opt(
            scope_tog, "CUSTOM LIST", False, self._scope_set_custom)
        self._scope_custom_btn.pack(side="left", expand=True, fill="x")

        self._scope_info_lbl = label(body, "", T.MUTED, T.f_sans(8))
        self._scope_info_lbl.pack(anchor="w", padx=8, pady=(0, 4))

        # Output folder
        self._field_label(body, "OUTPUT FOLDER")
        out_row = frame(body, fg=T.BG1)
        out_row.pack(fill="x", padx=8, pady=(0, 4))
        self._folder_lbl = label(out_row, self._trunc_path(self.output_folder),
                                 T.CYAN, T.f_mono(8))
        self._folder_lbl.pack(side="left", fill="x", expand=True)
        ctk.CTkButton(out_row, text="...", command=self._pick_folder,
                      corner_radius=0, fg_color=T.BG3, hover_color=T.BORDER_HI,
                      text_color=T.WHITE, font=T.f_mono(9), height=18,
                      width=22, border_width=0).pack(side="right", padx=(4, 0))

        # Load JSON tickers
        self._field_label(body, "LOAD TICKER LIST  (.json)")
        load_row = frame(body, fg=T.BG1)
        load_row.pack(fill="x", padx=8, pady=(0, 6))
        self._load_json_btn = ctk.CTkButton(
            load_row, text="OPEN FILE", command=self._load_ticker_json,
            corner_radius=0, fg_color=T.BG0, hover_color=T.BG3,
            text_color=T.LABEL, font=T.f_mono(9), height=22,
            border_width=1, border_color=T.BORDER, state="disabled")
        self._load_json_btn.pack(fill="x")

    def _field_label(self, parent, text):
        label(parent, text, T.LABEL, T.f_sans(9)).pack(anchor="w", padx=8, pady=(8, 2))

    def _toggle_opt(self, parent, text, active, cmd):
        lbl = label(parent, text, T.AMBER if active else T.LABEL, T.f_sans(9))
        lbl.configure(fg_color=T.BG3 if active else T.BG0)
        lbl.bind("<Button-1>", lambda e: cmd())
        return lbl

    def _tf_item(self, parent, tid, lbl_):
        on = tid in self.selected_tfs
        f = frame(parent, fg=T.BG1)
        box = tk.Canvas(f, width=10, height=10, bg=T.BG1, highlightthickness=0)
        rect = box.create_rectangle(0, 0, 9, 9,
                                    fill=T.GREEN_DIM if on else T.BG0,
                                    outline=T.GREEN if on else T.BORDER_HI)
        box.pack(side="left", padx=(6, 6), pady=5)
        name = label(f, lbl_, T.GREEN if on else T.MUTED, T.f_sans(10))
        name.pack(side="left")
        self.tf_widgets[tid] = (box, rect, name)
        handler = lambda e: self._toggle_tf(tid)
        f.bind("<Button-1>", handler)
        box.bind("<Button-1>", handler)
        name.bind("<Button-1>", handler)
        return f

    def _toggle_tf(self, tid):
        if tid in self.selected_tfs:
            self.selected_tfs.discard(tid)
        else:
            self.selected_tfs.add(tid)
        on = tid in self.selected_tfs
        box, rect, name = self.tf_widgets[tid]
        box.itemconfig(rect, fill=T.GREEN_DIM if on else T.BG0,
                       outline=T.GREEN if on else T.BORDER_HI)
        name.configure(text_color=T.GREEN if on else T.MUTED)
        self.tf_count.configure(text=f"{len(self.selected_tfs)}/{len(self.tf_defs)}")

    def _btn(self, parent, text, color, cmd):
        return ctk.CTkButton(parent, text=text, command=cmd, corner_radius=0,
                             fg_color=T.BG0, hover_color=T.BG3, text_color=color,
                             font=T.f_mono(9), height=26, border_width=0)

    def _phase_row(self, parent, pid, pname, status):
        row = frame(parent, fg=T.BG1, height=24)
        row.pack(fill="x")
        row.pack_propagate(False)
        id_col = T.GREEN if status == "done" else T.AMBER if status == "active" else T.LABEL
        name_col = T.WHITE if status == "done" else T.AMBER if status == "active" else T.MUTED
        label(row, pid, id_col, T.f_sans(9, "bold")).pack(side="left", padx=7)
        label(row, pname, name_col, T.f_sans(9)).pack(side="left", padx=7)
        bmap = {"done": ("DONE", T.GREEN, T.GREEN_DIM),
                "active": ("RUNNING", T.AMBER, T.AMBER_DIM),
                "pend": ("PENDING", T.LABEL, T.BG0)}
        txt, fg, bg = bmap[status]
        badge = label(row, txt, fg, T.f_sans(8))
        badge.configure(fg_color=bg)
        badge.pack(side="right", padx=7, ipadx=4)
        frame(parent, fg=T.BORDER, height=1).pack(fill="x")

    # ===================================================================== #
    # INSTRUMENT SEARCH POPUP
    # ===================================================================== #
    def _clear_sym_entry(self):
        self.sym_entry.delete(0, "end")
        self._instr_name_lbl.configure(text="", text_color=T.MUTED)
        self._close_search_popup()

    def _on_sym_keyrelease(self, event=None):
        q = self.sym_entry.get()
        results = self._search_instruments(q, max_results=8)
        self._show_search_popup(results)

    def _show_search_popup(self, results):
        self._close_search_popup()
        if not results:
            return

        self.update_idletasks()
        ex = self.sym_entry.winfo_rootx() - self.winfo_rootx()
        ey = self.sym_entry.winfo_rooty() - self.winfo_rooty() + self.sym_entry.winfo_height()
        ew = self.sym_entry.winfo_width()

        row_h = 26
        popup_h = min(len(results), 8) * row_h

        popup = tk.Frame(self, bg=T.BG2, bd=1, relief="flat",
                         highlightbackground=T.BORDER_HI, highlightthickness=1)
        popup.place(x=ex, y=ey, width=ew, height=popup_h)
        popup.lift()
        self._search_popup_frame = popup

        for key, name, desc in results:
            row = tk.Frame(popup, bg=T.BG2, height=row_h)
            row.pack(fill="x")
            row.pack_propagate(False)

            name_lbl = tk.Label(row, text=name.upper(), fg=T.WHITE, bg=T.BG2,
                                font=(T.MONO, 9), anchor="w")
            name_lbl.pack(side="left", padx=(6, 4))
            desc_lbl = tk.Label(row, text=desc, fg=T.MUTED, bg=T.BG2,
                                font=(T.SANS, 8), anchor="w")
            desc_lbl.pack(side="left")

            def _select(e, k=key, n=name, d=desc):
                self._select_instrument(k, n, d)

            def _enter(e, r=row, nl=name_lbl, dl=desc_lbl):
                r.configure(bg=T.BG3); nl.configure(bg=T.BG3); dl.configure(bg=T.BG3)

            def _leave(e, r=row, nl=name_lbl, dl=desc_lbl):
                r.configure(bg=T.BG2); nl.configure(bg=T.BG2); dl.configure(bg=T.BG2)

            for w in (row, name_lbl, desc_lbl):
                w.bind("<Button-1>", _select)
                w.bind("<Enter>", _enter)
                w.bind("<Leave>", _leave)

    def _close_search_popup(self, event=None):
        if self._search_popup_frame is not None:
            try:
                self._search_popup_frame.destroy()
            except Exception:
                pass
            self._search_popup_frame = None

    def _select_instrument(self, key, name, description):
        display_name = name.upper()
        self.symbol = display_name
        self.sym_entry.delete(0, "end")
        self.sym_entry.insert(0, display_name)
        self._instr_name_lbl.configure(text=description, text_color=T.CYAN)
        self.sym_display.configure(text=display_name)
        self.ib_sym.configure(text=display_name)
        self._close_search_popup()
        self.add_log(f"INSTRUMENT SET -> {display_name}  [{key}]  {description}", "ok")

    # ===================================================================== #
    # SCOPE HANDLERS
    # ===================================================================== #
    def _scope_set_mode(self, mode):
        self.scope_mode = mode
        for m, btn in (("single", self._scope_single_btn),
                       ("all", self._scope_all_btn),
                       ("custom", self._scope_custom_btn)):
            active = (m == mode)
            btn.configure(fg_color=T.BG3 if active else T.BG0,
                          text_color=T.AMBER if active else T.LABEL)
        self.sym_entry.configure(state="normal" if mode == "single" else "disabled")
        self._load_json_btn.configure(state="normal" if mode == "custom" else "disabled")

    def _scope_set_single(self):
        self._scope_set_mode("single")
        self._scope_info_lbl.configure(text="")
        if hasattr(self, "ib_scope"):
            self.ib_scope.configure(text="SINGLE", text_color=T.CYAN)
        self.add_log("SCOPE -> SINGLE", "info")

    def _scope_set_all(self):
        self._scope_set_mode("all")
        self._scope_info_lbl.configure(text="")
        if hasattr(self, "ib_scope"):
            self.ib_scope.configure(text="ALL", text_color=T.CYAN)
        self.add_log("SCOPE -> ALL INSTRUMENTS", "info")

    def _scope_set_custom(self):
        self._scope_set_mode("custom")
        n = len(self.scope_tickers)
        self._scope_info_lbl.configure(
            text=f"{n} tickers loaded" if n else "No list loaded -- use OPEN FILE",
            text_color=T.CYAN if n else T.AMBER)
        if hasattr(self, "ib_scope"):
            tag = f"{n} TICKERS" if n else "CUSTOM"
            self.ib_scope.configure(text=tag, text_color=T.AMBER)
        self.add_log(f"SCOPE -> CUSTOM LIST  ({n} tickers)", "info")

    def _pick_folder(self):
        folder = fd.askdirectory(title="Select output folder",
                                 initialdir=self.output_folder)
        if folder:
            self.output_folder = folder
            self._folder_lbl.configure(text=self._trunc_path(folder))
            self.add_log(f"OUTPUT FOLDER -> {folder}", "ok")
            # Load checkpoint state for the new folder
            self._show_checkpoint_state(folder)

    def _load_ticker_json(self):
        path = fd.askopenfilename(
            title="Load ticker list JSON",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialdir=self.output_folder)
        if not path:
            return
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("tickers", [])
            if not isinstance(raw, list):
                raise ValueError("'tickers' key must be a list.")
            known = set(self._instr_meta.keys())
            valid = [t for t in raw if isinstance(t, str) and t.lower() in known]
            invalid = [t for t in raw if t not in valid]
            self.scope_tickers = valid
            self._scope_set_mode("custom")
            msg = f"{len(valid)} tickers loaded"
            if invalid:
                msg += f"  ·  {len(invalid)} unknown skipped"
            self._scope_info_lbl.configure(text=msg,
                                           text_color=T.CYAN if valid else T.AMBER)
            if hasattr(self, "ib_scope"):
                self.ib_scope.configure(
                    text=f"{len(valid)} TICKERS" if valid else "CUSTOM",
                    text_color=T.AMBER)
            self.add_log(
                f"TICKER LIST LOADED  ·  {len(valid)} valid  ·  {len(invalid)} skipped"
                + (f"  ·  e.g. {invalid[:3]}" if invalid else ""),
                "ok" if valid else "warn")
        except Exception as exc:
            self.add_log(f"TICKER LIST ERROR -> {exc}", "err")

    def _trunc_path(self, path, max_len=28):
        if len(path) <= max_len:
            return path
        return "..." + path[-(max_len - 1):]

    # --------------------------------------------------------------- CENTER
    def _build_center(self, ws):
        CENTER_OFFSET = 4
        panel = frame(ws, fg=T.BG1, width=T.COL_CENTER_W - CENTER_OFFSET, height=T.WORKSPACE_H + T.MENUBAR_H)
        panel.place(x=T.COL_LEFT_W + CENTER_OFFSET, y=0)
        panel.pack_propagate(False)

        # instrument bar
        ib = frame(panel, fg=T.BG2, height=46)
        ib.pack(fill="x")
        ib.pack_propagate(False)
        self.ib_sym = label(ib, self.symbol, T.WHITE, T.f_sans(13, "bold"))
        self.ib_sym.pack(side="left", padx=(12, 8), pady=4)
        self._vsep_center(ib)
        self._ib_field(ib, "SYM",       self.symbol,   T.WHITE,  "ib_sym_tag")
        self._vsep_center(ib)
        self._ib_field(ib, "SCOPE",     "SINGLE",      T.CYAN,   "ib_scope")
        self._vsep_center(ib)
        self._ib_field(ib, "FORMAT",    "PARQUET",     T.AMBER,  "ib_format")
        self._vsep_center(ib)
        self._ib_field(ib, "WORKERS",   "4", T.WHITE, "ib_workers")
        self._vsep_center(ib)
        self._ib_field(ib, "DECIMAL_F", "100000",      T.WHITE)
        self._vsep_center(ib)
        self._ib_field(ib, "SCHEMA",    "INT32/INT64", T.GREEN)
        self._vsep_center(ib)
        self._ib_field(ib, "COMPRESS",  "ZSTD/LV1",   T.WHITE,  "ib_compress")
        self._vsep_center(ib)
        frame(panel, fg=T.BORDER, height=1).pack(fill="x")

        # progress block
        pb = frame(panel, fg=T.BG1)
        pb.pack(fill="x", pady=10, padx=10)
        head = frame(pb, fg=T.BG1)
        head.pack(fill="x", pady=(0, 4))
        label(head, "DOWNLOAD PROGRESS", T.LABEL, T.f_sans(9)).pack(side="left")
        self.prog_pct = label(head, "0%", T.GREEN, T.f_mono(11, "bold"))
        self.prog_pct.pack(side="right")

        self.prog_canvas = tk.Canvas(pb, height=16, bg=T.BG1, highlightthickness=0)
        self.prog_canvas.pack(fill="x", pady=(2, 8))
        self.TOTAL_BLOCKS = 48
        self._render_blocks(0, False)

        stats = frame(pb, fg=T.BG1)
        stats.pack(fill="x")
        self.stat_done = self._prog_stat(stats, "COMPLETED", "0", T.GREEN, 0)
        self.stat_pend = self._prog_stat(stats, "PENDING", "0", T.AMBER, 1)
        self.stat_err  = self._prog_stat(stats, "ERRORS", "0", T.RED, 2)
        self.stat_thr  = self._prog_stat(stats, "THROUGHPUT", "--", T.WHITE, 3)
        for i in range(4):
            stats.grid_columnconfigure(i, weight=1)

        frame(panel, fg=T.BORDER, height=1).pack(fill="x")

        # log header
        lh = frame(panel, fg=T.BG2, height=22)
        lh.pack(fill="x")
        lh.pack_propagate(False)
        label(lh, "SYSTEM LOG", T.AMBER, T.f_sans(9, "bold")).pack(side="left", padx=8)
        self.log_count_lbl = label(lh, "0 ENTRIES", T.LABEL, T.f_sans(8))
        self.log_count_lbl.configure(fg_color=T.BG0)
        self.log_count_lbl.pack(side="left", padx=4, ipadx=4)
        label(lh, "LOCAL", T.LABEL, T.f_sans(9)).pack(side="right", padx=8)
        frame(panel, fg=T.BORDER, height=1).pack(fill="x")

        # log area
        self.log_box = ctk.CTkTextbox(panel, fg_color=T.BG0, corner_radius=0,
                                      text_color=T.MUTED, font=T.f_mono(10),
                                      wrap="word", activate_scrollbars=True)
        self.log_box.pack(fill="both", expand=True)
        tb = self.log_box._textbox
        tb.tag_config("info", foreground=T.CYAN)
        tb.tag_config("ok",   foreground=T.GREEN)
        tb.tag_config("warn", foreground=T.AMBER)
        tb.tag_config("err",  foreground=T.RED)
        tb.tag_config("ts",   foreground=T.LABEL)
        tb.tag_config("sep",  foreground=T.BORDER)
        tb.tag_config("msg",  foreground=T.MUTED)
        tb.tag_config("hi",   foreground=T.WHITE)
        self.log_box.configure(state="disabled")

    def _ib_field(self, parent, lbl, val, color, attr=None):
        f = frame(parent, fg=T.BG2)
        f.pack(side="left", padx=5)
        label(f, lbl, T.LABEL, T.f_sans(8)).pack(anchor="w")
        v = label(f, val, color, T.f_sans(9, "bold"))
        v.pack(anchor="w")
        if attr:
            setattr(self, attr, v)

    def _prog_stat(self, parent, lbl, val, color, col):
        f = frame(parent, fg=T.BG1)
        f.grid(row=0, column=col, sticky="ew")
        v = label(f, val, color, T.f_sans(10, "bold"))
        v.pack()
        label(f, lbl, T.LABEL, T.f_sans(8)).pack()
        return v

    def _render_blocks(self, filled, active):
        c = self.prog_canvas
        c.delete("all")
        self.update_idletasks()
        total_w = c.winfo_width()
        if total_w <= 1:
            total_w = 580   # fallback antes del primer layout pass
        w = 10
        gap = 2
        n_blocks = max(1, (total_w - 1) // (w + gap))
        for i in range(n_blocks):
            x = i * (w + gap) + 1
            if i < int(filled / max(self.TOTAL_BLOCKS, 1) * n_blocks):
                fill, out = T.GREEN_DIM, T.GREEN
            elif active and i == int(filled / max(self.TOTAL_BLOCKS, 1) * n_blocks):
                fill, out = T.AMBER_DIM, T.AMBER
            else:
                fill, out = T.BG0, T.BORDER
            c.create_rectangle(x, 1, x + w, 15, fill=fill, outline=out)

    # ===================================================================== #
    # TICKER (FEED)
    # ===================================================================== #
    def _build_ticker(self):
        y = T.WIN_H - T.TICKER_H
        bar = frame(self, fg=T.BG2, width=T.WIN_W, height=T.TICKER_H)
        bar.place(x=0, y=y)
        bar.pack_propagate(False)
        frame(bar, fg=T.BORDER, height=1).pack(fill="x", side="top")

        lbl = label(bar, "FEED", T.BG0, T.f_sans(9, "bold"))
        lbl.configure(fg_color=T.AMBER)
        lbl.pack(side="left", fill="y", ipadx=8)

        self.tick_canvas = tk.Canvas(bar, bg=T.BG2, highlightthickness=0,
                                     height=T.TICKER_H)
        self.tick_canvas.pack(side="left", fill="both", expand=True)
        self._tick_font = tkfont.Font(family=T.MONO, size=9)
        self._tick_offset = float(T.WIN_W)

    def _refresh_ticker_data(self):
        snap = self.feed.snapshot()
        segs = []
        if not snap:
            segs.append(("AWAITING ALPHA VANTAGE FEED...", T.LABEL))
        else:
            for ticker in sorted(snap.keys()):
                q = snap[ticker]
                up = (q.change_pct is None) or (q.change_pct >= 0)
                arrow = "^" if up else "v"
                col = T.GREEN if up else T.RED
                price = fmt_price(q.price)
                chg = ""
                if q.change_pct is not None:
                    chg = f" {q.change_pct:+.2f}%"
                segs.append((f"{ticker.upper()}  {price} {arrow}{chg}", col))
                segs.append(("|", T.BORDER_HI))
        self._tick_model = segs
        self.after(5000, self._refresh_ticker_data)

    def _animate_ticker(self):
        c = self.tick_canvas
        c.delete("all")
        model = getattr(self, "_tick_model", [("CONNECTING...", T.LABEL)])
        font = self._tick_font
        gap = 18
        total = 0
        for txt, _ in model:
            total += font.measure(txt) + gap
        if total <= 0:
            total = 1
        x = self._tick_offset
        for _pass in range(2):
            xx = x + _pass * total
            for txt, col in model:
                c.create_text(xx, T.TICKER_H // 2, text=txt, fill=col,
                              font=font, anchor="w")
                xx += font.measure(txt) + gap
        self._tick_offset -= 2
        if self._tick_offset <= -total:
            self._tick_offset += total
        self.after(30, self._animate_ticker)

    # ===================================================================== #
    # LOG
    # ===================================================================== #
    def _ts_now(self):
        # Local machine time for display
        return datetime.now().strftime("%H:%M:%S")

    def add_log(self, msg, typ="info", cls="msg"):
        tmap = {"info": "INFO", "ok": "OK  ", "warn": "WARN", "err": "ERR "}
        self.log_box.configure(state="normal")
        tb = self.log_box._textbox
        tb.insert("end", self._ts_now(),           ("ts",))
        tb.insert("end", " | ",                    ("sep",))
        tb.insert("end", tmap.get(typ, "INFO"),    (typ,))
        tb.insert("end", " | ",                    ("sep",))
        tb.insert("end", f"{msg}\n",               (cls,))
        line_count = int(tb.index("end-1c").split(".")[0])
        if line_count > 200:
            tb.delete("1.0", f"{line_count - 200}.0")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")
        self.log_count += 1
        self.log_count_lbl.configure(text=f"{self.log_count} ENTRIES")

    def _feed_log(self, msg, typ):
        self._ui(lambda m=msg, t=typ: self.add_log(f"[FEED] {m}", t))

    def clear_log(self):
        self.log_box.configure(state="normal")
        self.log_box._textbox.delete("1.0", "end")
        self.log_box.configure(state="disabled")
        self.log_count = 0
        self.log_count_lbl.configure(text="0 ENTRIES")
        self.add_log("Log cleared by operator.", "info")

    # ===================================================================== #
    # PROGRESS / FORMAT
    # ===================================================================== #
    def _set_progress(self, done, total):
        pct = round(done / total * 100) if total > 0 else 0
        filled = int(pct / 100 * self.TOTAL_BLOCKS)
        self._render_blocks(filled, done < total and total > 0)
        self.prog_pct.configure(text=f"{pct}%")
        self.stat_done.configure(text=str(done))
        pending = max(0, total - done)
        self.stat_pend.configure(text=str(pending))

    def _set_format(self, fmt):
        self.current_fmt = fmt
        self.fmt_parquet.configure(fg_color=T.BG3 if fmt == "parquet" else T.BG0,
                                   text_color=T.AMBER if fmt == "parquet" else T.LABEL)
        self.fmt_csv.configure(fg_color=T.BG3 if fmt == "csv" else T.BG0,
                               text_color=T.AMBER if fmt == "csv" else T.LABEL)
        self.ib_format.configure(text=fmt.upper())
        self.mode_pill.configure(text="PARQUET/ZSTD" if fmt == "parquet" else "CSV/PLAIN")
        self.ib_compress.configure(text="ZSTD/LV1" if fmt == "parquet" else "NONE")
        self.add_log(
            f"STORAGE_FORMAT -> {fmt.upper()}  "
            + ("int32/int64 schema active." if fmt == "parquet" else "Fallback CSV (deprecated)."),
            "ok" if fmt == "parquet" else "warn")

    # ===================================================================== #
    # PAUSE / RESUME
    # ===================================================================== #
    def _toggle_pause(self):
        """Pausa o reanuda la descarga en curso."""
        if not self.running and not self.paused:
            return
        if self.paused:
            # Reanudar: relanzar el worker desde checkpoint
            self.add_log("RESUME — continuing from checkpoint...", "ok")
            self.paused = False
            self._pause_btn.configure(text="|| PAUSE", text_color=T.AMBER)
            self._dl_btn.configure(state="disabled", text_color=T.BORDER)
            # Re-read params from widgets (same as start_download but skips preflight UI)
            self._resume_download()
        else:
            # Pausar: señalizar al worker
            self.add_log("PAUSE requested — finishing in-flight tasks...", "warn")
            self._stop_event.set()
            self._pause_btn.configure(text="...", state="disabled", text_color=T.MUTED)

    def _resume_download(self):
        """Relanza el worker usando los mismos parámetros guardados en self._last_dl_args."""
        if not hasattr(self, "_last_dl_args"):
            self.add_log("No session to resume — use DOWNLOAD to start fresh.", "warn")
            return
        sym, workers, tfs, override_start, date_to = self._last_dl_args
        self.running = True
        self._stop_event.clear()
        threading.Thread(
            target=self._download_worker,
            args=(sym, workers, tfs, override_start, date_to),
            daemon=True,
        ).start()

    # ===================================================================== #
    # CHECKPOINT STATE DISPLAY
    # ===================================================================== #
    def _show_checkpoint_state(self, folder=None):
        """
        Reads progress.json from the output folder and logs checkpoint state.
        Called on boot and whenever the output folder changes.
        Also restores progress bar if a paused session is found.
        """
        path = Path(folder or self.output_folder) / "progress.json"
        if not path.exists():
            self.add_log("CHECKPOINT: no progress.json found in output folder", "info")
            return
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if not data:
                self.add_log("CHECKPOINT: progress.json exists but is empty", "info")
                return

            # Session metadata (special key, not a symbol)
            session = data.get("_session")
            symbol_data = {k: v for k, v in data.items() if k != "_session"}

            n_syms = len(symbol_data)
            self.add_log(f"CHECKPOINT: progress.json loaded  [{n_syms} symbols]", "ok")

            if session:
                date_from = session.get("date_from", "?")
                date_to   = session.get("date_to",   "?")
                paused    = session.get("paused", False)
                self.add_log(
                    f"  session  from:{date_from}  to:{date_to}"
                    + ("  [PAUSED — press RESUME to continue]" if paused else ""),
                    "warn" if paused else "info")
                if paused:
                    # Restaurar estado visual de pausa para que el botón aparezca activo
                    self.paused = True
                    self._pause_btn.configure(
                        text="> RESUME", state="normal", text_color=T.GREEN)
                    self._dl_btn.configure(state="normal")  # DOWNLOAD sigue disponible (sobrescribe)

            for sym, tfs in sorted(symbol_data.items()):
                for tf, last_date in sorted(tfs.items()):
                    self.add_log(f"  {sym}/{tf.upper()}  last: {last_date}", "info")

        except Exception as exc:
            self.add_log(f"CHECKPOINT: error reading progress.json: {exc}", "warn")

    # ===================================================================== #
    # PIPELINE ACTIONS
    # ===================================================================== #
    def _ui(self, fn, *a):
        self.after(0, lambda: fn(*a))

    def start_download(self):
        """
        Pre-flight robusta antes de lanzar el hilo de descarga.
        Toda lectura de widgets ocurre aquí, en el hilo principal, para
        ser thread-safe. El worker recibe los valores ya validados.

        Si hay una sesión pausada, DOWNLOAD sobrescribe todo y empieza desde cero.
        """
        if self.running:
            self.add_log("Pipeline already running — wait for it to finish.", "warn")
            return

        # ── 1. Símbolo ────────────────────────────────────────────────────
        sym = self.sym_entry.get().strip().upper()
        if self.scope_mode == "single":
            if not sym:
                self.add_log("PREFLIGHT ERROR: no instrument selected. "
                             "Type a symbol or pick one from the search list.", "err")
                return
            import re as _re
            if not _re.match(r'^[A-Z0-9/]{2,12}$', sym):
                self.add_log(
                    f"PREFLIGHT ERROR: symbol '{sym}' looks invalid "
                    "(use letters/digits/slash, e.g. EURUSD or EUR/USD).", "err")
                return

        # ── 2. Timeframes ─────────────────────────────────────────────────
        if not self.selected_tfs:
            self.add_log("PREFLIGHT ERROR: select at least one timeframe.", "err")
            return

        # ── 3. Carpeta de salida ──────────────────────────────────────────
        out = Path(self.output_folder)
        try:
            out.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            self.add_log(f"PREFLIGHT ERROR: cannot create output folder "
                         f"'{self.output_folder}': {exc}", "err")
            return
        if not os.access(str(out), os.W_OK):
            self.add_log(f"PREFLIGHT ERROR: output folder is not writable: "
                         f"'{self.output_folder}'", "err")
            return

        # ── 4. Workers ────────────────────────────────────────────────────
        try:
            workers = int(self.workers_sel.get())
        except (ValueError, AttributeError):
            workers = 4

        # ── 5. Fechas ─────────────────────────────────────────────────────
        override_start_date = None
        raw_from = self.date_from.get().strip()
        try:
            datetime.strptime(raw_from, "%Y-%m-%d")
            override_start_date = raw_from
        except ValueError:
            if raw_from:
                self.add_log(
                    "PREFLIGHT WARN: DATE FROM no está en formato YYYY-MM-DD. "
                    "Se ignorará — el orchestrator usará el checkpoint.", "warn")

        date_to = None
        raw_to = self.date_to.get().strip()
        try:
            datetime.strptime(raw_to, "%Y-%m-%d")
            date_to = raw_to
        except ValueError:
            if raw_to:
                self.add_log(
                    "PREFLIGHT WARN: DATE TO no está en formato YYYY-MM-DD. "
                    "Se ignorará.", "warn")

        # ── 6. Si hay sesión pausada, borrar checkpoint (inicio desde cero) ─
        if self.paused:
            self.add_log("DOWNLOAD pressed on paused session — clearing checkpoint "
                         "and starting fresh.", "warn")
            ck_path = out / "progress.json"
            if ck_path.exists():
                try:
                    ck_path.unlink()
                except Exception as exc:
                    self.add_log(f"Could not clear checkpoint: {exc}", "warn")
            self.paused = False

        # ── Todo OK: lanzar worker con valores pre-leídos ─────────────────
        tfs_snapshot = list(self.selected_tfs)
        self._last_dl_args = (sym, workers, tfs_snapshot, override_start_date, date_to)

        self.running = True
        self.paused = False
        self._stop_event.clear()

        # Habilitar botón pausa
        self._pause_btn.configure(
            text="|| PAUSE", state="normal", text_color=T.AMBER)

        threading.Thread(
            target=self._download_worker,
            args=(sym, workers, tfs_snapshot, override_start_date, date_to),
            daemon=True,
        ).start()

    def _download_worker(self, sym: str, workers: int, tfs: list,
                         override_start_date: str | None = None,
                         date_to: str | None = None):
        """
        Real download worker.
        Recibe sym, workers y tfs ya validados y leídos desde el hilo principal.
        Cualquier excepción no capturada se reporta en el log de la app (no stderr).
        """
        try:
            self._download_worker_inner(sym, workers, tfs, override_start_date, date_to)
        except Exception as exc:
            import traceback
            tb = traceback.format_exc()
            self._ui(lambda e=str(exc), t=tb: self.add_log(
                f"DOWNLOAD WORKER CRASH: {e}\n{t}", "err"))
            self.running = False
        except BaseException:           # FIX BUG-5: captura KeyboardInterrupt/SystemExit
            self.running = False
            raise

    def _download_worker_inner(self, sym: str, workers: int, tfs: list,
                               override_start_date: str | None = None,
                               date_to: str | None = None):
        import config as _cfg
        from github_scraper import GitHubScraper, Instrument

        fmt = self.current_fmt

        self._ui(lambda: self.ib_sym.configure(text=sym))
        self._ui(lambda: self.sym_display.configure(text=sym))
        self._ui(lambda: self.ib_workers.configure(text=str(workers)))
        self._ui(lambda: self.stat_done.configure(text="0"))
        self._ui(lambda: self.stat_pend.configure(text="?"))
        self._ui(lambda: self.stat_err.configure(text="0"))
        self._ui(lambda: self._set_progress(0, 1))  # reset bar

        self._ui(lambda: self.add_log(
            f"DOWNLOAD INIT  symbol:{sym}  tfs:[{' '.join(sorted(tfs))}]"
            f"  workers:{workers}  format:{fmt.upper()}"
            + (f"  from:{override_start_date}" if override_start_date else ""),
            "info", "hi"))

        # ── 1. Fetch instrument metadata from GitHub ──────────────────────
        self._ui(lambda: self.add_log("Fetching instrument list from GitHub...", "info"))
        try:
            scraper = GitHubScraper()
            all_instruments = scraper.scrape()
        except Exception as exc:
            self._ui(lambda: self.add_log(f"GitHub scrape failed: {exc}", "err"))
            self.running = False
            return

        self._ui(lambda: self.add_log(
            f"  {len(all_instruments)} instruments found", "ok"))

        # ── 2. Filter instruments by scope ────────────────────────────────
        if self.scope_mode == "single":
            # Find the instrument matching the entered symbol
            instruments = [i for i in all_instruments if i.symbol == sym]
            if not instruments:
                self._ui(lambda: self.add_log(
                    f"Symbol {sym} not found in Dukascopy universe. Check the name.", "err"))
                self.running = False
                return
            self._ui(lambda: self.add_log(
                f"SCOPE: SINGLE  [{sym}]", "info"))
        elif self.scope_mode == "all":
            instruments = all_instruments
            self._ui(lambda: self.add_log(
                f"SCOPE: ALL  [{len(instruments)} instruments]", "info"))
        else:  # custom
            wanted = {t.lower() for t in self.scope_tickers}
            instruments = [i for i in all_instruments if i.symbol.lower() in wanted]
            self._ui(lambda: self.add_log(
                f"SCOPE: CUSTOM  [{len(instruments)} matched / {len(self.scope_tickers)} requested]",
                "info" if instruments else "warn"))
            if not instruments:
                self._ui(lambda: self.add_log(
                    "No matching instruments found for custom list.", "err"))
                self.running = False
                return

        # ── 3. Callbacks for the orchestrator ─────────────────────────────
        _total_tasks = [0]   # mutable cell so closure can write

        def on_log(msg, typ):
            self._ui(lambda m=msg, t=typ: self.add_log(m, t))

        def on_progress(done, total):
            _total_tasks[0] = total
            self._ui(lambda d=done, t=total: self._set_progress(d, t))

        def on_task_done(task_label, failed, err_count):
            # stat_err always updated (BUG 1 FIX)
            self._ui(lambda ec=err_count: self.stat_err.configure(text=str(ec)))
            # Only log to terminal when there's something worth showing (BUG 7 FIX)
            if task_label is not None:
                if failed:
                    self._ui(lambda lbl=task_label: self.add_log(
                        f"  FAIL  {lbl}", "err"))
                else:
                    self._ui(lambda lbl=task_label: self.add_log(
                        f"  {lbl}  ->  done", "ok"))

        def on_finalize_step(sym_f, tf_f, i, n):
            self._ui(lambda s=sym_f, t=tf_f, ii=i, nn=n: self.add_log(
                f"  [{ii}/{nn}] finalize {s}/{t}", "info"))

        def on_done(interrupted, paused):
            self.running = False
            def _finish(iv=interrupted, pv=paused):
                if pv:
                    self.add_log("Download PAUSED — checkpoint saved. Press RESUME to continue.", "warn")
                    self.paused = True
                    self._pause_btn.configure(text="> RESUME", state="normal", text_color=T.GREEN)
                elif iv:
                    self.add_log("Download interrupted — checkpoint saved.", "warn")
                    self._pause_btn.configure(state="disabled", text_color=T.BORDER)
                else:
                    self.add_log("DOWNLOAD COMPLETE  all chunks consolidated.", "ok", "hi")
                    self._render_blocks(self.TOTAL_BLOCKS, False)
                    self.prog_pct.configure(text="100%")
                    self._pause_btn.configure(state="disabled", text_color=T.BORDER)
                self.running = False
            self._ui(_finish)

        # ── 4. Run ────────────────────────────────────────────────────────
        orch = _GUIOrchestrator(
            output_path  = self.output_folder,
            max_workers  = workers,
            max_retries  = _cfg.MAX_RETRIES,
            storage_fmt  = fmt,
            on_log       = on_log,
            on_progress  = on_progress,
            on_task_done = on_task_done,
            on_finalize_step = on_finalize_step,
            on_done      = on_done,
            stop_event   = self._stop_event,
        )
        orch.run(instruments, tfs, override_start_date=override_start_date,
                 date_to=date_to)

    def run_reconstruct(self):
        if self.running:
            self.add_log("Pipeline running — wait for it to finish.", "warn")
            return
        if not self.selected_tfs:
            self.add_log("PREFLIGHT ERROR: select at least one timeframe.", "err")
            return

        # Pre-flight: carpeta de salida
        out = Path(self.output_folder)
        if not out.exists():
            self.add_log(
                f"PREFLIGHT ERROR: output folder does not exist: '{self.output_folder}'. "
                "Select a valid folder before reconstructing.", "err")
            return

        sym = self.sym_entry.get().strip().upper() or "EURUSD"
        tfs_sorted = sorted(self.selected_tfs)
        self._reconstruct_args = (sym, tfs_sorted)   # passed to worker via attribute

        # ── Confirmation Dialog ───────────────────────────────────────────
        lines = [
            f">> RECONSTRUCT  {sym}",
            "",
            f"  Timeframes : {', '.join(tfs_sorted)}",
            f"  Folder     : {self.output_folder}",
            "",
            "This will read every existing Parquet chunk for the selected",
            "symbol/timeframes, deduplicate rows, sort by timestamp, and",
            "write the consolidated file back to disk.",
            "",
            "! No network calls. No data will be downloaded.",
            "! Existing Parquet files will be overwritten in-place.",
        ]
        confirmed = _confirm_dialog(self, "RECONSTRUCT CONFIRMATION", lines, T.AMBER)
        if not confirmed:
            self.add_log("RECONSTRUCT cancelled by operator.", "info")
            return

        self.running = True
        threading.Thread(target=self._reconstruct_worker, daemon=True).start()

    def _reconstruct_worker(self):
        try:
            self._reconstruct_worker_inner()
        except Exception as exc:
            import traceback
            tb = traceback.format_exc()
            self._ui(lambda e=str(exc), t=tb: self.add_log(
                f"RECONSTRUCT CRASH: {e}\n{t}", "err"))
            self.running = False

    def _reconstruct_worker_inner(self):
        """
        sym y tfs ya se leyeron en run_reconstruct (hilo principal) y se
        guardaron en self._reconstruct_args antes de lanzar el thread.
        """
        from parquet_writer import ParquetWriter

        sym, tfs = self._reconstruct_args
        self._ui(lambda: self.add_log(
            f"RECONSTRUCT INIT  symbol:{sym}  tfs:[{' '.join(sorted(tfs))}]",
            "info", "hi"))

        writer = ParquetWriter(Path(self.output_folder))
        for tf in sorted(tfs):
            self._ui(lambda t=tf: self.add_log(
                f"  finalize {sym}/{t}  dedup + sort", "info"))
            try:
                writer.finalize(sym, tf)
                self._ui(lambda t=tf: self.add_log(
                    f"  {sym}/{t}  consolidated OK", "ok"))
                self.ckpt += 1
                self._ui(lambda: self.m_ckpt.configure(text=str(self.ckpt)))
            except Exception as exc:
                self._ui(lambda t=tf, e=exc: self.add_log(
                    f"  {sym}/{t}  error: {e}", "err"))

        self._ui(lambda: self.add_log(
            f"RECONSTRUCT COMPLETE  {sym}  {len(tfs)} timeframes processed", "ok"))
        self.running = False

    def run_migrate(self):
        if self.running:
            self.add_log("Pipeline running — wait for it to finish.", "warn")
            return

        # Pre-flight: carpeta de salida
        out = Path(self.output_folder)
        if not out.exists():
            self.add_log(
                f"PREFLIGHT ERROR: output folder does not exist: '{self.output_folder}'. "
                "Select a valid folder before migrating.", "err")
            return

        # Contar CSVs disponibles para informar al usuario
        csv_count = len(list(out.rglob("*.csv")))

        # ── Confirmation Dialog ───────────────────────────────────────────
        lines = [
            ">> MIGRATE  CSV  ->  PARQUET",
            "",
            f"  Folder : {self.output_folder}",
            f"  Found  : {csv_count} CSV file(s)",
            "",
            "This will scan ALL CSV files in the output folder, convert",
            "them to Parquet format using the int32/int64 schema, and",
            "write the new .parquet files alongside the originals.",
            "",
            "! The original CSV files are NOT deleted automatically.",
            "! csv_writer.py will be marked [DEPRECATED] after migration.",
        ]
        if csv_count == 0:
            lines.append("! WARNING: no CSV files found — migration may do nothing.")

        confirmed = _confirm_dialog(self, "MIGRATE CONFIRMATION", lines, T.RED)
        if not confirmed:
            self.add_log("MIGRATE cancelled by operator.", "info")
            return

        self.running = True
        threading.Thread(target=self._migrate_worker, daemon=True).start()

    def _migrate_worker(self):
        try:
            self._migrate_worker_inner()
        except Exception as exc:
            import traceback
            tb = traceback.format_exc()
            self._ui(lambda e=str(exc), t=tb: self.add_log(
                f"MIGRATE CRASH: {e}\n{t}", "err"))
            self.running = False

    def _migrate_worker_inner(self):
        """
        Runs migrate_csv_to_parquet for the output folder.
        If migrate_csv_to_parquet.py is not importable, logs and exits.
        """
        self._ui(lambda: self.add_log(
            "MIGRATE CSV->PARQUET  scan_csv()  applying int32/int64 schema...",
            "warn", "hi"))
        try:
            from migrate_csv_to_parquet import migrate_all
            out = Path(self.output_folder)
            migrate_all(out, lambda msg: self._ui(
                lambda m=msg: self.add_log(m, "info")))
            self._ui(lambda: self.add_log(
                "MIGRATE COMPLETE  csv_writer.py marked [DEPRECATED]", "ok"))
        except ImportError:
            self._ui(lambda: self.add_log(
                "migrate_csv_to_parquet not found — run manually via CLI", "warn"))
        except Exception as exc:
            self._ui(lambda: self.add_log(f"MIGRATE ERROR: {exc}", "err"))
        self.running = False

    # ===================================================================== #
    # CLOCK / DATES / BOOT
    # ===================================================================== #
    def _tick_clock(self):
        """Local machine time displayed in topbar."""
        n = datetime.now()   # local time, not UTC
        self.clock_lbl.configure(text=n.strftime("%H:%M:%S"))
        months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL",
                  "AUG", "SEP", "OCT", "NOV", "DEC"]
        self.date_lbl.configure(text=f"{n.day:02d} {months[n.month - 1]} {n.year}")
        self.after(1000, self._tick_clock)

    def _init_dates(self):
        from datetime import timedelta
        t = datetime.now()
        fr = t - timedelta(days=30)
        self.date_to.insert(0, t.strftime("%Y-%m-%d"))
        self.date_from.insert(0, fr.strftime("%Y-%m-%d"))

    def _boot_logs(self):
        _ui_handler = _UILogHandler(self.add_log)
        logging.getLogger().addHandler(_ui_handler)

        n = len(self._instr_meta)
        self.add_log(f"DKSC Pipeline Control  ready", "ok", "hi")
        self.add_log(f"Instrument universe loaded  {n} instruments",
                     "ok" if n else "warn")
        # Show backend root so operator can verify imports will resolve
        if _PROJECT_ROOT is not None:
            self.add_log(f"BACKEND ROOT  {_PROJECT_ROOT}", "info")
        else:
            self.add_log(
                "BACKEND ROOT not found — config.py missing in parent dirs. "
                "Downloads will fail.", "err")
        # Show checkpoint state from default output folder
        self._show_checkpoint_state()

    def _on_close(self):
        self.feed.stop()
        self.destroy()


if __name__ == "__main__":
    app = DKSCTerminal()
    app.mainloop()