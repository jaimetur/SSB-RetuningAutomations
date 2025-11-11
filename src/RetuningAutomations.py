#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main launcher with GUI/CLI to run one of:
  1) Pre/Post Relations Consistency Check (PrePostRelations)
  2) Create Excel from Logs (stub)
  3) Clean-Up (stub)

Behavior:
- If run with NO args, opens a single Tkinter window to choose module,
  input folder, and (optionally) frequencies (with defaults).
- If run with CLI args, behaves headless (unless you omit required fields, then
  will try GUI unless --no-gui).
"""

import argparse
import os
import sys
import time  # high-resolution timing for module execution
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List
import textwrap
import importlib
import configparser
import traceback
from pathlib import Path
import inspect
from tkinter import messagebox

# Import our different Classes
from src.modules.ConsistencyChecks import ConsistencyChecks
from src.modules.ConfigurationAudit import ConfigurationAudit
from src.modules.InitialCleanUp import InitialCleanUp
from src.modules.FinalCleanUp import FinalCleanUp

# ================================ VERSIONING ================================ #

TOOL_NAME           = "RetuningAutomations"
TOOL_VERSION        = "0.2.6"
TOOL_DATE           = "2025-11-12"
TOOL_NAME_VERSION   = f"{TOOL_NAME}_v{TOOL_VERSION}"
COPYRIGHT_TEXT      = "(c) 2025 - Jaime Tur (jaime.tur@ericsson.com)"
TOOL_DESCRIPTION    = textwrap.dedent(f"""
{TOOL_NAME_VERSION} - {TOOL_DATE}
Multi-Platform/Multi-Arch tool designed to Automate some process during SSB Retuning
©️ 2025 by Jaime Tur (jaime.tur@ericsson.com)
""")

# ================================ DEFAULTS ================================= #
# Input Folder
# INPUT_FOLDER = r"c:\Users\ejaitur\OneDrive - Ericsson\SSB Retuning Project Sharepoint - Scripts\OutputStep0\RetuneAutomations\ToyCells\PA7"
INPUT_FOLDER = ""  # empty by default if not defined

# Frequencies (single Pre/Post used by ConsistencyChecks)
DEFAULT_FREQ_PRE = "648672"
DEFAULT_FREQ_POST = "647328"

# Global selectable list for filtering summary columns in ConfigurationAudit
# NOTE: User can edit/extend this list. It supports multi-selection in GUI.
NETWORK_FREQUENCIES: List[str] = [
    "174970","176410","176430","176910","177150","392410","393410","394500","394590","432970","647328","648672","650004","653952","2071667","2071739","2073333","2074999","2076665","2078331","2079997","2081663","2083329"
]

# TABLES_ORDER defines the desired priority of table sheet ordering.
# Sheets whose MO name is not listed here will be placed after the listed ones.
TABLES_ORDER = []

# Module names
MODULE_NAMES = [
    "1. Configuration Audit (Logs Parser)",
    "2. Consistency Check (Pre/Post Comparison)",
    "3. Initial Clean-Up (During Maintenance Window)",
    "4. Final Clean-Up (After Retune is completed)",
]

# ============================== PERSISTENT CONFIG =========================== #
# We store config under user's home to avoid write-permission issues with PyInstaller/Nuitka.
CONFIG_DIR  = Path.home() / ".retuning_automations"
CONFIG_PATH = CONFIG_DIR / "config.cfg"
CONFIG_SECTION = "general"
CONFIG_KEY_LAST_INPUT = "last_input_dir"
CONFIG_KEY_FREQ_FILTERS = "summary_freq_filters"  # comma-separated persistence for filters

# ============================== LOGGING SYSTEM ============================== #
class LoggerDual:
    """
    Simple dual logger that mirrors stdout prints to both console and a log file.
    Replaces sys.stdout so every print() goes to both outputs automatically.
    """
    def __init__(self, log_file_path: str):
        self.terminal = sys.stdout
        self.log = open(log_file_path, "a", encoding="utf-8")

    def write(self, message: str):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        """Required for compatibility with Python's stdout flush behavior."""
        self.terminal.flush()
        self.log.flush()

# ============================ OPTIONAL TKINTER UI =========================== #
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception:
    tk = None
    ttk = None
    filedialog = None
    messagebox = None


@dataclass
class GuiResult:
    module: str
    input_dir: str
    freq_pre: str
    freq_post: str
    freq_filters_csv: str  # comma-separated list of frequency filters for summary sheets


def _normalize_csv_list(text: str) -> str:
    """Normalize a comma-separated text into 'a,b,c' without extra spaces/empties."""
    if not text:
        return ""
    items = [t.strip() for t in text.split(",")]
    items = [t for t in items if t]  # drop empties
    return ",".join(items)


def gui_config_dialog(
    default_input: str = "",
    default_pre: str = DEFAULT_FREQ_PRE,
    default_post: str = DEFAULT_FREQ_POST,
    default_filters_csv: str = "",
) -> Optional[GuiResult]:
    """
    Opens a single modal window with:
      - Combobox (module)
      - Input folder (entry + Browse)
      - Freq Pre (entry)
      - Freq Post (entry)
      - Multi-select list for Summary Frequency Filters (persisted)
      - Run / Cancel
    Returns GuiResult or None if cancelled/unavailable.
    """
    if tk is None or ttk is None or filedialog is None:
        return None

    root = tk.Tk()
    root.title("Select module to run and configuration")
    root.resizable(False, False)

    # Center window
    try:
        root.update_idletasks()
        w, h = 770, 430
        sw = root.winfo_screenwidth()
        sh = root.winfo_screenheight()
        x = (sw // 2) - (w // 2)
        y = (sh // 3) - (h // 2)
        root.geometry(f"{w}x{h}+{x}+{y}")
    except Exception:
        pass

    # --- Vars
    module_var = tk.StringVar(value=MODULE_NAMES[0])
    input_var = tk.StringVar(value=default_input or "")
    pre_var = tk.StringVar(value=default_pre or "")
    post_var = tk.StringVar(value=default_post or "")
    selected_csv_var = tk.StringVar(value=_normalize_csv_list(default_filters_csv))
    result: Optional[GuiResult] = None

    # --- Layout
    pad = {'padx': 10, 'pady': 6}
    pad_tight = {'padx': 4, 'pady': 2}
    frm = ttk.Frame(root, padding=12)
    frm.pack(fill="both", expand=True)

    # Row 0: Module
    ttk.Label(frm, text="Module to run:").grid(row=0, column=0, sticky="w", **pad)
    cmb = ttk.Combobox(frm, textvariable=module_var, values=MODULE_NAMES, state="readonly", width=42)
    cmb.grid(row=0, column=1, columnspan=2, sticky="ew", **pad)

    # Row 1: Input folder
    ttk.Label(frm, text="Input folder:").grid(row=1, column=0, sticky="w", **pad)
    ent_input = ttk.Entry(frm, textvariable=input_var, width=48)
    ent_input.grid(row=1, column=1, sticky="ew", **pad)

    def browse():
        path = filedialog.askdirectory(title="Select input folder", initialdir=input_var.get() or os.getcwd())
        if path:
            input_var.set(path)

    ttk.Button(frm, text="Browse…", command=browse).grid(row=1, column=2, sticky="ew", **pad)

    # Row 2-3: Pre/Post single frequencies (ConsistencyChecks)
    ttk.Label(frm, text="Frequency (Pre):").grid(row=2, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=pre_var, width=22).grid(row=2, column=1, sticky="w", **pad)
    ttk.Label(frm, text="Frequency (Post):").grid(row=3, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=post_var, width=22).grid(row=3, column=1, sticky="w", **pad)

    # Row 4+: Multi-select for Summary Filters (ConfigurationAudit)
    ttk.Separator(frm).grid(row=4, column=0, columnspan=3, sticky="ew", **pad)
    ttk.Label(frm, text="Summary Filters (for pivot columns in Configuration Audit):").grid(row=5, column=0, columnspan=3, sticky="w", **pad)

    # Left: multi-select list of NETWORK_FREQUENCIES with vertical scrollbar
    list_frame = ttk.Frame(frm)
    # list_frame.grid(row=6, column=0, columnspan=1, sticky="nsw", **pad)
    list_frame.grid(row=6, column=0, columnspan=1, sticky="nsw", **pad_tight)
    ttk.Label(list_frame, text="Available frequencies:").pack(anchor="w")

    # Frame that holds the Listbox and Scrollbar
    lb_container = ttk.Frame(list_frame)
    lb_container.pack(fill="both", expand=True)

    # Scrollbar
    scrollbar = ttk.Scrollbar(lb_container, orient="vertical")
    scrollbar.pack(side="right", fill="y")

    # Listbox linked to the scrollbar
    lb = tk.Listbox(
        lb_container,
        selectmode="extended",
        height=10,
        width=24,
        exportselection=False,
        yscrollcommand=scrollbar.set
    )
    for freq in NETWORK_FREQUENCIES:
        lb.insert("end", freq)
    lb.pack(side="left", fill="both", expand=True)

    scrollbar.config(command=lb.yview)

    btns_frame = ttk.Frame(frm)
    # btns_frame.grid(row=6, column=1, sticky="n", **pad)
    btns_frame.grid(row=6, column=1, sticky="n", **pad_tight)

    # Right: entry showing selected (comma-separated)
    right_frame = ttk.Frame(frm)
    # right_frame.grid(row=6, column=2, sticky="nsew", **pad)
    right_frame.grid(row=6, column=2, sticky="nsew", **pad_tight)
    ttk.Label(right_frame, text="Frequencies Filter (Empty = No Filter):").grid(row=0, column=0, sticky="w")
    ent_selected = ttk.Entry(right_frame, textvariable=selected_csv_var, width=36)
    ent_selected.grid(row=1, column=0, sticky="ew")

    def _current_selected_set() -> List[str]:
        return [s.strip() for s in _normalize_csv_list(selected_csv_var.get()).split(",") if s.strip()]

    def add_selected():
        # Append highlighted items from listbox into the CSV entry (unique)
        chosen = [lb.get(i) for i in lb.curselection()]
        pool = set(_current_selected_set())
        for c in chosen:
            pool.add(c)
        selected_csv_var.set(",".join(sorted(pool)))

    def remove_selected():
        # Remove highlighted items from listbox from the CSV entry
        chosen = set([lb.get(i) for i in lb.curselection()])
        pool = [x for x in _current_selected_set() if x not in chosen]
        selected_csv_var.set(",".join(pool))

    def select_all():
        lb.select_set(0, "end")
        add_selected()

    def clear_filters():
        """Clear only the summary frequency filters (no input dir / pre/post)."""
        lb.selection_clear(0, "end")
        selected_csv_var.set("")

    ttk.Button(btns_frame, text="Add →", command=add_selected).pack(pady=4, fill="x")
    ttk.Button(btns_frame, text="← Remove", command=remove_selected).pack(pady=4, fill="x")
    ttk.Button(btns_frame, text="Select all", command=select_all).pack(pady=4, fill="x")
    ttk.Button(btns_frame, text="Clear Filter", command=clear_filters).pack(pady=4, fill="x")

    # Row 7: Buttons
    btns = ttk.Frame(frm)
    btns.grid(row=7, column=0, columnspan=3, sticky="e", **pad)

    def on_run():
        nonlocal result
        sel_module = module_var.get().strip()
        sel_input = input_var.get().strip()
        if not sel_input:
            messagebox.showerror("Missing input", "Please select an input folder.")
            return
        result = GuiResult(
            module=sel_module,
            input_dir=sel_input,
            freq_pre=pre_var.get().strip(),
            freq_post=post_var.get().strip(),
            freq_filters_csv=_normalize_csv_list(selected_csv_var.get()),
        )
        root.destroy()

    def on_cancel():
        nonlocal result
        result = None
        root.destroy()

    ttk.Button(btns, text="Cancel", command=on_cancel).pack(side="right", padx=6)
    ttk.Button(btns, text="Run", command=on_run).pack(side="right")
    root.bind("<Return>", lambda e: on_run())
    root.bind("<Escape>", lambda e: on_cancel())
    root.mainloop()
    return result


# ================================ CLI PARSER ================================ #

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launcher Retuning Automations Tool with GUI fallback.")
    parser.add_argument(
        "--module",
        choices=["configuration-audit", "consistency-check", "initial-cleanup", "final-cleanup"],
        help="Module to run: configuration-audit|consistency-check|initial-cleanup|final-cleanup. If omitted, GUI appears (unless --no-gui)."
    )
    parser.add_argument("-i", "--input", help="Input folder to process")
    parser.add_argument("--freq-pre", help="Frequency before refarming (Pre)")
    parser.add_argument("--freq-post", help="Frequency after refarming (Post)")
    parser.add_argument(
        "--freq-filters",
        help="Comma-separated list of frequencies to filter pivot columns in Configuration Audit (substring match per column header)."
    )
    parser.add_argument("--no-gui", action="store_true", help="Disable GUI prompts (require CLI args)")
    return parser.parse_args()


# ============================== RUNNERS (TASKS) ============================= #

def run_configuration_audit(input_dir: str, freq_filters_csv: str = "") -> None:
    module_name = "[Configuration Audit (Log Parser)]"
    print(f"{module_name} Running…")
    print(f"{module_name} Input folder: '{input_dir}'")
    if freq_filters_csv:
        print(f"{module_name} Summary column filters: {freq_filters_csv}")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"
    app = ConfigurationAudit()

    # We pass the filters to ConfigurationAudit if its run() accepts it. If not, we call old signature.
    kwargs = dict(module_name=module_name, versioned_suffix=versioned_suffix, tables_order=TABLES_ORDER)
    if freq_filters_csv:
        # New optional argument expected by updated ConfigurationAudit
        kwargs["filter_frequencies"] = [x.strip() for x in freq_filters_csv.split(",") if x.strip()]

    try:
        # Try new signature first
        out = app.run(input_dir, **kwargs)
    except TypeError:
        # Fallback to legacy signature (no filtering supported by class yet)
        print(f"{module_name} [WARN] Installed ConfigurationAudit does not support 'filter_frequencies'. Running without filters.")
        out = app.run(input_dir, module_name=module_name, versioned_suffix=versioned_suffix, tables_order=TABLES_ORDER)

    if out:
        print(f"{module_name} Done → '{out}'")
    else:
        print(f"{module_name}  No logs found or nothing written.")


def run_consistency_checks(input_dir: str, freq_pre: Optional[str], freq_post: Optional[str]) -> None:
    module_name = "[Consistency Checks (Pre/Post Comparison)]"
    print(f"{module_name} Running…")
    print(f"{module_name} Input folder: '{input_dir}'")

    # --- Detect presence of Pre/Post folders early and exit if missing ---
    pre_found, post_found = False, False
    try:
        for entry in os.scandir(input_dir):
            if not entry.is_dir():
                continue
            tag = ConsistencyChecks._detect_prepost(entry.name)
            if tag == "Pre":
                pre_found = True
            elif tag == "Post":
                post_found = True
    except FileNotFoundError:
        # Si el input_dir no existe, conserva tu comportamiento actual más abajo
        pass

    if not (pre_found and post_found):
        missing = []
        if not pre_found:
            missing.append("Pre")
        if not post_found:
            missing.append("Post")
        msg = (
            f"Missing required folder(s): {', '.join(missing)}\n\n"
            "No processing will be performed. Please select a folder that contains both Pre and Post folders."
        )
        try:
            messagebox.showwarning("Missing Pre/Post folders", msg)
        except Exception:
            print(f"{module_name} [WARNING] {msg}")
        return
    # -------------------------------------------------------------------------------

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"
    output_dir = os.path.join(input_dir, f"CellRelationConsistencyChecks_{versioned_suffix}")

    app = ConsistencyChecks()
    app.loadPrePost(input_dir)

    results = None
    if freq_pre and freq_post:
        results = app.comparePrePost(freq_pre, freq_post, module_name)
    else:
        print(f"{module_name} [INFO] Frequencies not provided. Comparison will be skipped; only tables will be saved.")

    app.save_outputs_excel(output_dir, results, versioned_suffix=versioned_suffix)

    print(f"\n{module_name} Outputs saved to: '{output_dir}'")
    if results:
        print(f"{module_name} Wrote CellRelation.xlsx and CellRelationDiscrepancies.xlsx (with Summary and details).")
    else:
        print(f"{module_name} Wrote CellRelation.xlsx (all tables). No comparison Excel because frequencies were not provided.")


def run_initial_cleanup(input_dir: str, *_args) -> None:
    module_name = "[Initial Clean-Up]"
    print(f"{module_name} Running…")
    print(f"{module_name} Input folder: '{input_dir}'")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    app = InitialCleanUp()
    out = app.run(input_dir, module_name=module_name, versioned_suffix=versioned_suffix)

    if out:
        print(f"{module_name} Done → '{out}'")
    else:
        # print(f"{module_name} No logs found or nothing written.")
        print(f"{module_name} Module logic not yet implemented (under development). Exiting...")


def run_final_cleanup(input_dir: str, *_args) -> None:
    module_name = "[Final Clean-Up]"
    print(f"{module_name} Running…")
    print(f"{module_name} Input folder: '{input_dir}'")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    app = FinalCleanUp()
    out = app.run(input_dir, module_name=module_name, versioned_suffix=versioned_suffix)

    if out:
        print(f"{module_name} Done → '{out}'")
    else:
        # print(f"{module_name} No logs found or nothing written.")
        print(f"{module_name} Module logic not yet implemented (under development). Exiting...")


def resolve_module_callable(name: str):
    name = (name or "").strip().lower()
    if name in ("audit", MODULE_NAMES[0].lower(), "configuration-audit"):
        return run_configuration_audit
    if name in ("consistency-check", MODULE_NAMES[1].lower()):
        return run_consistency_checks
    if name in ("initial-cleanup", MODULE_NAMES[2].lower()):
        return run_initial_cleanup
    if name in ("final-cleanup", MODULE_NAMES[3].lower(), "final-cleanup"):
        return run_final_cleanup
    return None


def load_last_input_dir_from_config() -> str:
    """Load last used input directory from config file. Returns empty string if missing."""
    try:
        if not CONFIG_PATH.exists():
            return ""
        parser = configparser.ConfigParser()
        parser.read(CONFIG_PATH, encoding="utf-8")
        return parser.get(CONFIG_SECTION, CONFIG_KEY_LAST_INPUT, fallback="").strip()
    except Exception:
        # Fail-safe: do not block the tool if config is corrupt
        return ""


def load_last_filters_from_config() -> str:
    """Load last used frequency filters (CSV) from config file. Returns empty string if missing."""
    try:
        if not CONFIG_PATH.exists():
            return ""
        parser = configparser.ConfigParser()
        parser.read(CONFIG_PATH, encoding="utf-8")
        return parser.get(CONFIG_SECTION, CONFIG_KEY_FREQ_FILTERS, fallback="").strip()
    except Exception:
        return ""


def save_last_input_dir_to_config(input_dir: str) -> None:
    """Persist last used input directory to config file (create/update as needed)."""
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        parser = configparser.ConfigParser()
        if CONFIG_PATH.exists():
            parser.read(CONFIG_PATH, encoding="utf-8")
        if CONFIG_SECTION not in parser:
            parser[CONFIG_SECTION] = {}
        parser[CONFIG_SECTION][CONFIG_KEY_LAST_INPUT] = input_dir or ""
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            parser.write(f)
    except Exception:
        # Silent fail on write; we do not want to break main flow due to IO
        pass


def save_last_filters_to_config(filters_csv: str) -> None:
    """Persist last used frequency filters (CSV) to config file."""
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        parser = configparser.ConfigParser()
        if CONFIG_PATH.exists():
            parser.read(CONFIG_PATH, encoding="utf-8")
        if CONFIG_SECTION not in parser:
            parser[CONFIG_SECTION] = {}
        parser[CONFIG_SECTION][CONFIG_KEY_FREQ_FILTERS] = _normalize_csv_list(filters_csv)
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            parser.write(f)
    except Exception:
        pass


def _format_duration_hms(seconds: float) -> str:
    """Return duration as H:MM:SS.mmm (milliseconds precision)."""
    ms = int((seconds - int(seconds)) * 1000)
    total_seconds = int(seconds)
    hours, rem = divmod(total_seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}.{ms:03d}"


def execute_module(module_fn, input_dir: str, freq_pre: str, freq_post: str, freq_filters_csv: str = "") -> None:
    """Execute the selected module with the proper signature (timed)."""
    # Timing starts here
    start_ts = time.perf_counter()
    # Friendly label for logs: use function name if no better label
    label = getattr(module_fn, "__name__", "module")

    try:
        # Normalize signatures here so caller code stays simple.
        if module_fn is run_consistency_checks:
            module_fn(input_dir, freq_pre, freq_post)
        elif module_fn is run_configuration_audit:
            module_fn(input_dir, freq_filters_csv=freq_filters_csv)
        elif module_fn is run_initial_cleanup:
            module_fn(input_dir, freq_pre, freq_post)
        elif module_fn is run_final_cleanup:
            module_fn(input_dir, freq_pre, freq_post)
        else:
            # Fallback for custom callables keeping compatibility
            # Try to pass filters if function supports it
            sig = inspect.signature(module_fn)
            if "freq_filters_csv" in sig.parameters:
                module_fn(input_dir, freq_pre, freq_post, freq_filters_csv=freq_filters_csv)
            else:
                module_fn(input_dir, freq_pre, freq_post)
    finally:
        # Always print elapsed time even if the module raised an exception
        elapsed = time.perf_counter() - start_ts
        print(f"[Timer] {label} finished in {_format_duration_hms(elapsed)}")


def ask_reopen_launcher() -> bool:
    """Ask the user if the launcher should reopen after a module finishes.
    Returns True to reopen, False to exit.
    """
    # If Tk is not available (or disabled), default to not reopening.
    if messagebox is None:
        return False
    try:
        return messagebox.askyesno(
            "Finished",
            "The selected task has finished.\nDo you want to open the launcher again?"
        )
    except Exception:
        # In case of headless/console-only contexts, do not loop.
        return False


def log_module_exception(module_label: str, exc: BaseException) -> None:
    """Pretty-print a module exception to stdout (and therefore to the log)."""
    print("\n" + "=" * 80)
    print(f"[ERROR] An exception occurred while executing {module_label}:")
    print("-" * 80)
    print(str(exc))
    print("-" * 80)
    print("Traceback (most recent call last):")
    print(traceback.format_exc().rstrip())
    print("=" * 80 + "\n")
    # Optional GUI popup (short message) if Tk is available
    if messagebox is not None:
        try:
            messagebox.showerror(
                "Execution error",
                f"An exception occurred while executing {module_label}.\n\n{exc}"
            )
        except Exception:
            pass

# ================================== MAIN =================================== #

def main():
    os.system('cls' if os.name == 'nt' else 'clear')

    # --- Initialize log file inside ./Logs folder ---
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_filename = f"RetuningAutomation_{timestamp}_v{TOOL_VERSION}.log"

    # Detect the base directory of the running script or compiled binary
    try:
        base_dir = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))
    except Exception:
        base_dir = os.getcwd()

    logs_dir = os.path.join(base_dir, "Logs")
    os.makedirs(logs_dir, exist_ok=True)

    log_path = os.path.join(logs_dir, log_filename)

    # Replace stdout/stderr and with our dual logger
    sys.stdout = LoggerDual(log_path)
    sys.stderr = sys.stdout  # <- add this
    print(f"[Logger] Output will also be written to: {log_path}")
    print("")

    # Load Tool while splash image is shown (only for Windows)
    print("")
    print("Loading Tool...")
    # Remove Splash image from Pyinstaller
    if '_PYI_SPLASH_IPC' in os.environ and importlib.util.find_spec("pyi_splash"):
        import pyi_splash
        pyi_splash.update_text('UI Loaded ...')
        pyi_splash.close()

    # Remove Splash image from Nuitka
    if "NUITKA_ONEFILE_PARENT" in os.environ:
        import tempfile
        splash_filename = os.path.join(
            tempfile.gettempdir(),
            "onefile_%d_splash_feedback.tmp" % int(os.environ["NUITKA_ONEFILE_PARENT"]),
        )
        with open(splash_filename, "wb") as f:
            f.write(b"READY")

        if os.path.exists(splash_filename):
            os.unlink(splash_filename)
    print("Tool loaded!")
    print(TOOL_DESCRIPTION)
    print("")
    print(f"[Config] Using config file: {CONFIG_PATH}")
    print("")

    args = parse_args()

    # Determine default input folder and default filters (persist across runs)
    persisted_last = load_last_input_dir_from_config()
    default_input = args.input or persisted_last or INPUT_FOLDER or ""
    persisted_filters = load_last_filters_from_config()
    default_filters_csv = _normalize_csv_list(args.freq_filters or persisted_filters)

    default_pre = args.freq_pre or DEFAULT_FREQ_PRE
    default_post = args.freq_post or DEFAULT_FREQ_POST

    # CASE A: CLI module specified
    if args.module:
        module_fn = resolve_module_callable(args.module)
        if module_fn is None:
            raise SystemExit(f"Unknown module: {args.module}")

        input_dir = args.input or ""
        freq_pre = args.freq_pre or DEFAULT_FREQ_PRE
        freq_post = args.freq_post or DEFAULT_FREQ_POST
        freq_filters_csv = default_filters_csv  # already normalized from args/config

        # If input is missing and GUI is allowed, show GUI, execute, and optionally loop
        if not input_dir and not args.no_gui and tk is not None:
            while True:
                sel = gui_config_dialog(default_input="", default_pre=freq_pre, default_post=freq_post, default_filters_csv=freq_filters_csv)
                if sel is None:
                    raise SystemExit("Cancelled.")
                input_dir = sel.input_dir
                freq_pre = sel.freq_pre
                freq_post = sel.freq_post
                freq_filters_csv = sel.freq_filters_csv

                # Persist last used input dir and filters
                save_last_input_dir_to_config(input_dir)
                save_last_filters_to_config(freq_filters_csv)

                try:
                    execute_module(module_fn, input_dir, freq_pre, freq_post, freq_filters_csv=freq_filters_csv)
                except Exception as e:
                    log_module_exception(sel.module, e)

                # Ask if user wants to reopen the launcher; if not, exit loop
                if not ask_reopen_launcher():
                    break
                # Reset input_dir to force showing the GUI again on next iteration
                input_dir = ""
            return

        # Pure headless CLI execution (no GUI fallback or input provided)
        if not input_dir:
            raise SystemExit("Input folder not provided.")
        # Persist last used input dir and filters (headless CLI)
        save_last_input_dir_to_config(input_dir)
        save_last_filters_to_config(freq_filters_csv)
        execute_module(module_fn, input_dir, freq_pre, freq_post, freq_filters_csv=freq_filters_csv)
        return

    # CASE B: No module specified -> GUI (if available)
    if not args.no_gui and tk is not None:
        # Loop to keep reopening the launcher after each module finishes
        while True:
            sel = gui_config_dialog(
                default_input=default_input,
                default_pre=default_pre,
                default_post=default_post,
                default_filters_csv=default_filters_csv
            )
            if sel is None:
                raise SystemExit("Cancelled.")

            module_fn = resolve_module_callable(sel.module)
            if module_fn is None:
                raise SystemExit(f"Unknown module selected: {sel.module}")

            # Persist last used input dir and filters
            save_last_input_dir_to_config(sel.input_dir)
            save_last_filters_to_config(sel.freq_filters_csv)

            try:
                execute_module(module_fn, sel.input_dir, sel.freq_pre, sel.freq_post, freq_filters_csv=sel.freq_filters_csv)
            except Exception as e:
                # Log nicely and keep the app alive
                log_module_exception(sel.module, e)

            # Ask if user wants to reopen the launcher; if not, exit loop
            if not ask_reopen_launcher():
                break
        return

    # CASE C: Headless (no GUI)
    if not args.input:
        raise SystemExit("Input folder not provided and GUI disabled/unavailable. Use -i/--input.")
    # Persist last used input dir and filters (headless no-GUI path)
    save_last_input_dir_to_config(args.input)
    save_last_filters_to_config(default_filters_csv)
    try:
        # Default to Consistency Checks in headless path (same behavior as before)
        run_consistency_checks(args.input, args.freq_pre or DEFAULT_FREQ_PRE, args.freq_post or DEFAULT_FREQ_POST)
    except Exception as e:
        log_module_exception("consistency-check", e)
        return

    # Log closing
    print("\n[Logger] Execution finished.")
    # Restore stdout and close log file
    if isinstance(sys.stdout, LoggerDual):
        sys.stdout.log.close()
        sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()
