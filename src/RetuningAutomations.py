#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Main launcher with GUI/CLI to run one of:
  1) Pre/Post Relations Consistency Check (PrePostRelations)
  2) Create Excel from Logs (stub)
  3) Clean-Up (stub)

Behavior:
- If run with NO args, opens a single Tkinter window to choose module,
  input folder(s), and (optionally) frequencies (with defaults).
- If run with CLI args, behaves headless (unless you omit required fields, then
  will try GUI unless --no-gui).

NEW:
- When the user selects the SECOND item in the module combobox (index 1),
  the GUI switches to a dual-input layout asking for two folders:
    • Pre input folder
    • Post input folder
  The launcher will pass BOTH folders to the module call.
- For any other module, the GUI shows the classic single input folder.
- CLI now accepts --input-pre and --input-post for the consistency-check module.

NEW (ARFCN lists):
- Allowed N77 SSB and Allowed N77 ARFCN can now be configured from CLI
  (via --allowed-ssb-n77 / --allowed-n77-arfcn) or from GUI (two CSV fields).
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

# Import our different Classes
from src.modules.ConsistencyChecks import ConsistencyChecks
from src.modules.ConfigurationAudit import ConfigurationAudit
from src.modules.InitialCleanUp import InitialCleanUp
from src.modules.FinalCleanUp import FinalCleanUp

# ================================ VERSIONING ================================ #

TOOL_NAME           = "RetuningAutomations"
TOOL_VERSION        = "0.2.8"
TOOL_DATE           = "2025-11-17"
TOOL_NAME_VERSION   = f"{TOOL_NAME}_v{TOOL_VERSION}"
COPYRIGHT_TEXT      = "(c) 2025 - Jaime Tur (jaime.tur@ericsson.com)"
TOOL_DESCRIPTION    = textwrap.dedent(f"""
{TOOL_NAME_VERSION} - {TOOL_DATE}
Multi-Platform/Multi-Arch tool designed to Automate some process during SSB Retuning
©️ 2025 by Jaime Tur (jaime.tur@ericsson.com)
""")

# ================================ DEFAULTS ================================= #
# Input Folder(s)
INPUT_FOLDER = ""  # single-input default if not defined
INPUT_FOLDER_PRE = ""   # default Pre folder for dual-input GUI (empty by default)
INPUT_FOLDER_POST = ""  # default Post folder for dual-input GUI (empty by default)

# Frequencies (single Pre/Post used by ConsistencyChecks)
DEFAULT_FREQ_PRE = "648672"
DEFAULT_FREQ_POST = "647328"

# Default ARFCN lists (CSV) for ConfigurationAudit
# NOTE: These are used if the user does not provide custom lists via CLI/GUI.
DEFAULT_ALLOWED_N77_SSB_CSV = "648672,653952"
DEFAULT_ALLOWED_N77_ARFCN_CSV = "654652,655324,655984,656656"

# Global selectable list for filtering summary columns in ConfigurationAudit
# NOTE: User can edit/extend this list. It supports multi-selection in GUI.
NETWORK_FREQUENCIES: List[str] = [
    "174970","176410","176430","176910","177150","392410","393410","394500","394590","432970",
    "647328","648672","650004","650006","653952",
    "2071667","2071739","2073333","2074999","2076665","2078331","2079997","2081663","2083329"
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
CONFIG_KEY_LAST_INPUT_PRE = "last_input_dir_pre"
CONFIG_KEY_LAST_INPUT_POST = "last_input_dir_post"
CONFIG_KEY_FREQ_FILTERS = "summary_freq_filters"  # comma-separated persistence for filters
CONFIG_KEY_ALLOWED_N77_SSB = "allowed_n77_ssb_csv"       # NEW: persist N77 SSB list
CONFIG_KEY_ALLOWED_N77_ARFCN = "allowed_n77_arfcn_csv" # NEW: persist N77 ARFCN list


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
    # Single-input mode
    input_dir: str
    # Dual-input mode
    input_pre_dir: str
    input_post_dir: str
    # Common frequencies
    freq_pre: str
    freq_post: str
    # Summary filters for ConfigurationAudit
    freq_filters_csv: str  # comma-separated list of frequency filters for summary sheets
    # NEW: SSB/ARFCN lists for ConfigurationAudit
    allowed_n77_ssb_csv: str
    allowed_n77_arfcn_csv: str


def _normalize_csv_list(text: str) -> str:
    """Normalize a comma-separated text into 'a,b,c' without extra spaces/empties."""
    if not text:
        return ""
    items = [t.strip() for t in text.split(",")]
    items = [t for t in items if t]  # drop empties
    return ",".join(items)


def _is_consistency_module(selected_text: str) -> bool:
    """
    Return True if the selected module is the SECOND item (index 1),
    regardless of its display name. We check position to stay robust
    even if the label changes in the future.
    """
    try:
        idx = MODULE_NAMES.index(selected_text)
        return idx == 1
    except ValueError:
        # If not found (custom text?), fallback to comparing lowercased forms
        lowered = selected_text.strip().lower()
        return lowered.startswith("2.") or "consistency" in lowered


def gui_config_dialog(
    default_input: str = "",
    default_pre: str = DEFAULT_FREQ_PRE,
    default_post: str = DEFAULT_FREQ_POST,
    default_filters_csv: str = "",
    default_input_pre: str = "",
    default_input_post: str = "",
    default_allowed_n77_ssb_csv: str = DEFAULT_ALLOWED_N77_SSB_CSV,
    default_allowed_n77_arfcn_csv: str = DEFAULT_ALLOWED_N77_ARFCN_CSV,
) -> Optional[GuiResult]:
    """
    Opens a single modal window with:
      - Combobox (module)
      - Either:
         • Single input folder (entry + Browse), or
         • Dual input folders (Pre + Post, each with Browse)
        The layout auto-switches when the SECOND module is selected.
      - Freq Pre (entry)
      - Freq Post (entry)
      - Multi-select list for Summary Frequency Filters (persisted)
      - CSV fields for:
         • Allowed N77 SSB  list
         • Allowed N77 ARFCN list
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
        w, h = 620, 580
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
    input_pre_var = tk.StringVar(value=default_input_pre or "")
    input_post_var = tk.StringVar(value=default_input_post or "")
    pre_var = tk.StringVar(value=default_pre or "")
    post_var = tk.StringVar(value=default_post or "")
    selected_csv_var = tk.StringVar(value=_normalize_csv_list(default_filters_csv))
    allowed_n77_ssb_var = tk.StringVar(value=_normalize_csv_list(default_allowed_n77_ssb_csv))
    allowed_n77_arfcn_var = tk.StringVar(value=_normalize_csv_list(default_allowed_n77_arfcn_csv))
    result: Optional[GuiResult] = None

    # --- Layout
    pad = {'padx': 10, 'pady': 6}
    pad_tight = {'padx': 4, 'pady': 2}
    frm = ttk.Frame(root, padding=12)
    frm.pack(fill="both", expand=True)

    # Row 0: Module
    ttk.Label(frm, text="Module to run:").grid(row=0, column=0, sticky="w", **pad)
    cmb = ttk.Combobox(frm, textvariable=module_var, values=MODULE_NAMES, state="readonly", width=50)
    cmb.grid(row=0, column=1, columnspan=2, sticky="ew", **pad)

    # --- Single-input frame (shown for modules other than #2)
    single_frame = ttk.Frame(frm)
    ttk.Label(single_frame, text="Input folder:").grid(row=0, column=0, sticky="w", **pad)
    ent_input = ttk.Entry(single_frame, textvariable=input_var, width=58)
    ent_input.grid(row=0, column=1, sticky="ew", **pad)

    def browse_single():
        path = filedialog.askdirectory(title="Select input folder", initialdir=input_var.get() or os.getcwd())
        if path:
            input_var.set(path)

    ttk.Button(single_frame, text="Browse…", command=browse_single).grid(row=0, column=2, sticky="ew", **pad)

    # --- Dual-input frame (shown only for module #2)
    dual_frame = ttk.Frame(frm)

    ttk.Label(dual_frame, text="Pre input folder:").grid(row=0, column=0, sticky="w", **pad)
    ent_pre = ttk.Entry(dual_frame, textvariable=input_pre_var, width=58)
    ent_pre.grid(row=0, column=1, sticky="ew", **pad)

    def browse_pre():
        path = filedialog.askdirectory(title="Select PRE input folder", initialdir=input_pre_var.get() or os.getcwd())
        if path:
            input_pre_var.set(path)

    ttk.Button(dual_frame, text="Browse…", command=browse_pre).grid(row=0, column=2, sticky="ew", **pad)

    ttk.Label(dual_frame, text="Post input folder:").grid(row=1, column=0, sticky="w", **pad)
    ent_post = ttk.Entry(dual_frame, textvariable=input_post_var, width=58)
    ent_post.grid(row=1, column=1, sticky="ew", **pad)

    def browse_post():
        path = filedialog.askdirectory(title="Select POST input folder", initialdir=input_post_var.get() or os.getcwd())
        if path:
            input_post_var.set(path)

    ttk.Button(dual_frame, text="Browse…", command=browse_post).grid(row=1, column=2, sticky="ew", **pad)

    # Initially show single or dual depending on default selection
    def _refresh_input_mode(*_e):
        # Hide both, then show the appropriate one
        single_frame.grid_forget()
        dual_frame.grid_forget()
        if _is_consistency_module(module_var.get()):
            dual_frame.grid(row=1, column=0, columnspan=3, sticky="ew")
        else:
            single_frame.grid(row=1, column=0, columnspan=3, sticky="ew")

    cmb.bind("<<ComboboxSelected>>", _refresh_input_mode)
    _refresh_input_mode()

    # Row 2-3: Pre/Post single frequencies (ConsistencyChecks)
    ttk.Label(frm, text="Frequency (Pre):").grid(row=2, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=pre_var, width=22).grid(row=2, column=1, sticky="w", **pad)
    ttk.Label(frm, text="Frequency (Post):").grid(row=3, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=post_var, width=22).grid(row=3, column=1, sticky="w", **pad)

    # Row 4+: Multi-select for Summary Filters (ConfigurationAudit)
    ttk.Separator(frm).grid(row=4, column=0, columnspan=3, sticky="ew", **pad)
    ttk.Label(frm, text="Summary Filters (for pivot columns in Configuration Audit):").grid(
        row=5, column=0, columnspan=3, sticky="w", **pad
    )

    # Left: multi-select list of NETWORK_FREQUENCIES with vertical scrollbar
    list_frame = ttk.Frame(frm)
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

    # Middle: buttons for selection management
    btns_frame = ttk.Frame(frm)
    btns_frame.grid(row=6, column=1, sticky="n", **pad_tight)

    # Right: entry showing selected (comma-separated)
    right_frame = ttk.Frame(frm)
    right_frame.grid(row=6, column=2, sticky="nsew", **pad_tight)
    ttk.Label(right_frame, text="Frequencies Filter (Empty = No Filter):").grid(row=0, column=0, sticky="w")
    ent_selected = ttk.Entry(right_frame, textvariable=selected_csv_var, width=40)
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

    # NEW: ARFCN lists for ConfigurationAudit
    ttk.Separator(frm).grid(row=7, column=0, columnspan=3, sticky="ew", **pad)
    ttk.Label(frm, text="Allowed ARFCN sets for Configuration Audit (CSV):").grid(
        row=8, column=0, columnspan=3, sticky="w", **pad
    )

    ttk.Label(frm, text="Allowed N77 SSB (csv):").grid(row=9, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=allowed_n77_ssb_var, width=40).grid(
        row=9, column=1, columnspan=2, sticky="ew", **pad
    )

    ttk.Label(frm, text="Allowed N77 ARFCN (csv):").grid(row=10, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=allowed_n77_arfcn_var, width=40).grid(
        row=10, column=1, columnspan=2, sticky="ew", **pad
    )

    # Row 11: Buttons
    btns = ttk.Frame(frm)
    btns.grid(row=11, column=0, columnspan=3, sticky="e", **pad)

    def on_run():
        nonlocal result
        sel_module = module_var.get().strip()

        # Normalize ARFCN CSV inputs
        normalized_allowed_n77_ssb = _normalize_csv_list(allowed_n77_ssb_var.get())
        normalized_allowed_n77_arfcn = _normalize_csv_list(allowed_n77_arfcn_var.get())

        # Validate inputs depending on the selected module
        if _is_consistency_module(sel_module):
            sel_input_pre = input_pre_var.get().strip()
            sel_input_post = input_post_var.get().strip()
            if not sel_input_pre or not sel_input_post:
                messagebox.showerror("Missing input", "Please select both Pre and Post input folders.")
                return
            result = GuiResult(
                module=sel_module,
                input_dir="",  # not used in dual-input mode
                input_pre_dir=sel_input_pre,
                input_post_dir=sel_input_post,
                freq_pre=pre_var.get().strip(),
                freq_post=post_var.get().strip(),
                freq_filters_csv=_normalize_csv_list(selected_csv_var.get()),
                allowed_n77_ssb_csv=normalized_allowed_n77_ssb,
                allowed_n77_arfcn_csv=normalized_allowed_n77_arfcn,
            )
        else:
            sel_input = input_var.get().strip()
            if not sel_input:
                messagebox.showerror("Missing input", "Please select an input folder.")
                return
            result = GuiResult(
                module=sel_module,
                input_dir=sel_input,
                input_pre_dir="",
                input_post_dir="",
                freq_pre=pre_var.get().strip(),
                freq_post=post_var.get().strip(),
                freq_filters_csv=_normalize_csv_list(selected_csv_var.get()),
                allowed_n77_ssb_csv=normalized_allowed_n77_ssb,
                allowed_n77_arfcn_csv=normalized_allowed_n77_arfcn,
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
    # Single-input (most modules)
    parser.add_argument("-i", "--input", help="Input folder to process (single-input modules)")
    # Dual-input (consistency-check)
    parser.add_argument("--input-pre", help="PRE input folder (only for consistency-check)")
    parser.add_argument("--input-post", help="POST input folder (only for consistency-check)")

    parser.add_argument("--freq-pre", help="Frequency before refarming (Pre)")
    parser.add_argument("--freq-post", help="Frequency after refarming (Post)")
    parser.add_argument(
        "--freq-filters",
        help="Comma-separated list of frequencies to filter pivot columns in Configuration Audit (substring match per column header)."
    )

    # NEW: ARFCN list options for ConfigurationAudit
    parser.add_argument(
        "--allowed-n77-ssb",
        help="Comma-separated ARFCN list for N77 SSB allowed values (Configuration Audit).",
    )
    parser.add_argument(
        "--allowed-n77-arfcn",
        help="Comma-separated ARFCN list for N77 ARFCN allowed values (Configuration Audit).",
    )

    parser.add_argument("--no-gui", action="store_true", help="Disable GUI prompts (require CLI args)")
    return parser.parse_args()


# ============================== RUNNERS (TASKS) ============================= #

def _parse_arfcn_csv_to_set(
    csv_text: Optional[str],
    default_values: List[int],
    label: str,
) -> set:
    """
    Helper to parse a CSV string into a set of integers.

    - If csv_text is empty or all values are invalid, fall back to default_values.
    - Logs warnings for invalid tokens.
    """
    values: List[int] = []
    if csv_text:
        for token in csv_text.split(","):
            tok = token.strip()
            if not tok:
                continue
            try:
                values.append(int(tok))
            except ValueError:
                print(f"[Configuration Audit] [WARN] Ignoring invalid ARFCN '{tok}' in {label} list.")

    if not values:
        # Fallback to defaults
        return set(default_values)

    return set(values)


def run_configuration_audit(
    input_dir: str,
    freq_filters_csv: str = "",
    freq_pre: Optional[str] = None,
    freq_post: Optional[str] = None,
    allowed_n77_ssb_csv: Optional[str] = None,
    allowed_n77_arfcn_csv: Optional[str] = None,
) -> None:
    module_name = "[Configuration Audit (Log Parser)]"
    print(f"{module_name} Running…")
    print(f"{module_name} Input folder: '{input_dir}'")
    if freq_filters_csv:
        print(f"{module_name} Summary column filters: {freq_filters_csv}")

    # Determine ARFCN-related parameters for ConfigurationAudit using GUI/CLI frequencies
    # This allows the user to change new/old ARFCN values without touching the class internals.
    try:
        old_arfcn = int(freq_pre) if freq_pre else int(DEFAULT_FREQ_PRE)
    except ValueError:
        old_arfcn = int(DEFAULT_FREQ_PRE)

    try:
        new_arfcn = int(freq_post) if freq_post else int(DEFAULT_FREQ_POST)
    except ValueError:
        new_arfcn = int(DEFAULT_FREQ_POST)

    # Build allowed sets from CSV (or fall back to defaults that include new_arfcn).
    # Default behavior:
    #   - allowed_n77_ssb: {new_arfcn, 653952}
    #   - allowed_n77_arfcn: {654652, 655324, 655984, 656656}
    default_n77_ssb_list = [new_arfcn, 653952]
    default_n77_list = [654652, 655324, 655984, 656656]

    allowed_n77_ssb = _parse_arfcn_csv_to_set(
        csv_text=allowed_n77_ssb_csv,
        default_values=default_n77_ssb_list,
        label="Allowed N77 SSB",
    )
    allowed_n77_arfcn = _parse_arfcn_csv_to_set(
        csv_text=allowed_n77_arfcn_csv,
        default_values=default_n77_list,
        label="Allowed N77 ARFCN",
    )

    print(f"{module_name} Using old ARFCN = {old_arfcn} --> new ARFCN = {new_arfcn}")
    print(f"{module_name} Allowed N77 SSB set = {sorted(allowed_n77_ssb)}")
    print(f"{module_name} Allowed N77 ARFCN set = {sorted(allowed_n77_arfcn)}")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    # Create ConfigurationAudit instance with ARFCN parameters coming from launcher
    app = ConfigurationAudit(
        old_arfcn=old_arfcn,
        new_arfcn=new_arfcn,
        allowed_n77_ssb=allowed_n77_ssb,
        allowed_n77_arfcn=allowed_n77_arfcn,
    )

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


def run_consistency_checks(input_pre_dir: Optional[str], input_post_dir: Optional[str],
                           freq_pre: Optional[str], freq_post: Optional[str]) -> None:
    """
    Updated runner to support DUAL INPUT mode.
    - If both input_pre_dir and input_post_dir are provided, attempt to load Pre/Post from separate folders.
    - If not, preserve legacy behavior (single parent folder expected with 'Pre' and 'Post' children).
    """
    module_name = "[Consistency Checks (Pre/Post Comparison)]"
    print(f"{module_name} Running…")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    app = ConsistencyChecks()

    # Dual-input new path
    if input_pre_dir and input_post_dir:
        print(f"{module_name} PRE folder:  '{input_pre_dir}'")
        print(f"{module_name} POST folder: '{input_post_dir}'")

        # Try a modern signature first: loadPrePost(pre_dir, post_dir)
        loaded = False
        try:
            # If the class supports two-args loader, this will work
            app.loadPrePost(input_pre_dir, input_post_dir)  # NEW signature support
            loaded = True
        except TypeError:
            # Fall back to custom method name if available
            try:
                app.loadPrePostFromFolders(input_pre_dir, input_post_dir)  # Alternate name
                loaded = True
            except Exception:
                loaded = False

        if not loaded:
            # Do not try legacy common-parent fallback anymore; we explicitly require dual-input support
            print(f"{module_name} [ERROR] ConsistencyChecks class does not support dual folders (Pre/Post).")
            print(f"{module_name}         Please update ConsistencyChecks.loadPrePost(pre_dir, post_dir) to enable dual-input mode.")
            return

        # Output base is ALWAYS the POST folder in dual-input mode
        output_dir = os.path.join(input_post_dir, f"CellRelationConsistencyChecks_{versioned_suffix}")

    else:
        # Legacy single-input behavior preserved: expect a parent with Pre/Post inside
        input_dir = input_pre_dir or ""  # using first param slot as "single"
        print(f"{module_name} Input folder: '{input_dir}'")

        # Early presence check (legacy)
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

        app.loadPrePost(input_dir)
        output_dir = os.path.join(input_dir, f"CellRelationConsistencyChecks_{versioned_suffix}")

    results = None

    if freq_pre and freq_post:
        try:
            # Modern comparison that may accept dual context transparently
            results = app.comparePrePost(freq_pre, freq_post, module_name)
        except TypeError:
            # If the signature differs in your class, adapt here as needed
            results = app.comparePrePost(freq_pre, freq_post)
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


# =============================== PERSISTENCE ================================ #

def _read_cfg() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    if CONFIG_PATH.exists():
        parser.read(CONFIG_PATH, encoding="utf-8")
    return parser

def load_last_input_dir_from_config() -> str:
    """Load last used input directory for single-input modules."""
    try:
        if not CONFIG_PATH.exists():
            return ""
        parser = _read_cfg()
        return parser.get(CONFIG_SECTION, CONFIG_KEY_LAST_INPUT, fallback="").strip()
    except Exception:
        return ""


def save_last_input_dir_to_config(input_dir: str) -> None:
    """Persist last used input directory to config file (single-input)."""
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        parser = _read_cfg()
        _ensure_cfg_section(parser)
        parser[CONFIG_SECTION][CONFIG_KEY_LAST_INPUT] = input_dir or ""
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            parser.write(f)
    except Exception:
        pass

def load_last_dual_from_config() -> tuple[str, str]:
    """Load last used PRE/POST input directories for dual-input module."""
    try:
        if not CONFIG_PATH.exists():
            return ("", "")
        parser = _read_cfg()
        pre = parser.get(CONFIG_SECTION, CONFIG_KEY_LAST_INPUT_PRE, fallback="").strip()
        post = parser.get(CONFIG_SECTION, CONFIG_KEY_LAST_INPUT_POST, fallback="").strip()
        return (pre, post)
    except Exception:
        return ("", "")


def save_last_dual_to_config(pre_dir: str, post_dir: str) -> None:
    """Persist last used PRE/POST input directories (dual-input)."""
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        parser = _read_cfg()
        _ensure_cfg_section(parser)
        parser[CONFIG_SECTION][CONFIG_KEY_LAST_INPUT_PRE] = pre_dir or ""
        parser[CONFIG_SECTION][CONFIG_KEY_LAST_INPUT_POST] = post_dir or ""
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            parser.write(f)
    except Exception:
        pass


def load_last_filters_from_config() -> str:
    """Load last used frequency filters (CSV) from config file. Returns empty string if missing."""
    try:
        if not CONFIG_PATH.exists():
            return ""
        parser = _read_cfg()
        return parser.get(CONFIG_SECTION, CONFIG_KEY_FREQ_FILTERS, fallback="").strip()
    except Exception:
        return ""


def save_last_filters_to_config(filters_csv: str) -> None:
    """Persist last used frequency filters (CSV) to config file."""
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        parser = _read_cfg()
        _ensure_cfg_section(parser)
        parser[CONFIG_SECTION][CONFIG_KEY_FREQ_FILTERS] = _normalize_csv_list(filters_csv)
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            parser.write(f)
    except Exception:
        pass


def load_last_allowed_lists_from_config() -> tuple[str, str]:
    """
    Load last used Allowed N77 SSB and Allowed N77 ARFCN CSV lists
    from config file. Returns a tuple (allowed_n77_ssb_n77, allowed_n77_arfcn_csv).
    If missing, returns empty strings.
    """
    try:
        if not CONFIG_PATH.exists():
            return ("", "")
        parser = _read_cfg()
        allowed_n77_ssb = parser.get(CONFIG_SECTION, CONFIG_KEY_ALLOWED_N77_SSB, fallback="").strip()
        allowed_n77_arfcn = parser.get(CONFIG_SECTION, CONFIG_KEY_ALLOWED_N77_ARFCN, fallback="").strip()
        return (allowed_n77_ssb, allowed_n77_arfcn)
    except Exception:
        return ("", "")


def save_last_allowed_lists_to_config(allowed_n77_ssb_csv: str, allowed_n77_arfcn_csv: str) -> None:
    """
    Persist last used Allowed N77 SSB and Allowed N77 ARFCN CSV lists
    to config file.
    """
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        parser = _read_cfg()
        _ensure_cfg_section(parser)
        parser[CONFIG_SECTION][CONFIG_KEY_ALLOWED_N77_SSB] = _normalize_csv_list(allowed_n77_ssb_csv)
        parser[CONFIG_SECTION][CONFIG_KEY_ALLOWED_N77_ARFCN] = _normalize_csv_list(allowed_n77_arfcn_csv)
        with CONFIG_PATH.open("w", encoding="utf-8") as f:
            parser.write(f)
    except Exception:
        # Never break the tool just because config persistence fails
        pass

def _ensure_cfg_section(parser: configparser.ConfigParser) -> None:
    if CONFIG_SECTION not in parser:
        parser[CONFIG_SECTION] = {}

# =============================== EXECUTION CORE ============================= #

def _format_duration_hms(seconds: float) -> str:
    """Return duration as H:MM:SS.mmm (milliseconds precision)."""
    ms = int((seconds - int(seconds)) * 1000)
    total_seconds = int(seconds)
    hours, rem = divmod(total_seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}.{ms:03d}"


def execute_module(module_fn,
                   input_dir: str,
                   freq_pre: str,
                   freq_post: str,
                   freq_filters_csv: str = "",
                   input_pre_dir: str = "",
                   input_post_dir: str = "",
                   allowed_n77_ssb_csv: str = "",
                   allowed_n77_arfcn_csv: str = "") -> None:
    """
    Execute the selected module with the proper signature (timed).
    - For run_consistency_checks: prefer dual-input if both input_pre_dir/post_dir are provided.
    - For run_configuration_audit: single input + optional filters and ARFCN config.
    - For cleanup modules: single input.
    """
    start_ts = time.perf_counter()
    label = getattr(module_fn, "__name__", "module")

    try:
        if module_fn is run_consistency_checks:
            # Dual-input preferred if provided
            if input_pre_dir and input_post_dir:
                module_fn(input_pre_dir, input_post_dir, freq_pre, freq_post)
            else:
                # Backwards compatibility: use single input_dir (legacy layout)
                module_fn(input_dir, None, freq_pre, freq_post)
        elif module_fn is run_configuration_audit:
            # Pass GUI/CLI frequencies and ARFCN lists down so ConfigurationAudit can use them
            module_fn(
                input_dir,
                freq_filters_csv=freq_filters_csv,
                freq_pre=freq_pre,
                freq_post=freq_post,
                allowed_n77_ssb_csv=allowed_n77_ssb_csv,
                allowed_n77_arfcn_csv=allowed_n77_arfcn_csv,
            )
        elif module_fn is run_initial_cleanup:
            module_fn(input_dir, freq_pre, freq_post)
        elif module_fn is run_final_cleanup:
            module_fn(input_dir, freq_pre, freq_post)
        else:
            # Generic fallback for custom callables
            sig = inspect.signature(module_fn)
            params = sig.parameters
            if "input_pre_dir" in params and "input_post_dir" in params:
                # Modules that support dual-input signature can optionally accept extra config
                kwargs = {}
                if "freq_pre" in params:
                    kwargs["freq_pre"] = freq_pre
                if "freq_post" in params:
                    kwargs["freq_post"] = freq_post
                if "freq_filters_csv" in params:
                    kwargs["freq_filters_csv"] = freq_filters_csv
                if "allowed_n77_ssb_csv" in params:
                    kwargs["allowed_n77_ssb_csv"] = allowed_n77_ssb_csv
                if "allowed_n77_arfcn_csv" in params:
                    kwargs["allowed_n77_arfcn_csv"] = allowed_n77_arfcn_csv
                module_fn(input_pre_dir=input_pre_dir, input_post_dir=input_post_dir, **kwargs)
            elif "freq_filters_csv" in params or "allowed_n77_ssb_csv" in params or "allowed_n77_arfcn_csv" in params:
                # Single-input modules with optional extra configuration
                kwargs = {}
                if "freq_filters_csv" in params:
                    kwargs["freq_filters_csv"] = freq_filters_csv
                if "allowed_n77_ssb_csv" in params:
                    kwargs["allowed_ssb_n77_csv"] = allowed_n77_ssb_csv
                if "allowed_n77_arfcn_csv" in params:
                    kwargs["allowed_n77_arfcn_csv"] = allowed_n77_arfcn_csv
                module_fn(input_dir, freq_pre, freq_post, **kwargs)
            else:
                module_fn(input_dir, freq_pre, freq_post)
    finally:
        elapsed = time.perf_counter() - start_ts
        print(f"[Timer] {label} finished in {_format_duration_hms(elapsed)}")


def ask_reopen_launcher() -> bool:
    """Ask the user if the launcher should reopen after a module finishes.
    Returns True to reopen, False to exit.
    """
    if messagebox is None:
        return False
    try:
        return messagebox.askyesno(
            "Finished",
            "The selected task has finished.\nDo you want to open the launcher again?"
        )
    except Exception:
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

    # Replace stdout/stderr with our dual logger
    sys.stdout = LoggerDual(log_path)
    sys.stderr = sys.stdout
    print(f"[Logger] Output will also be written to: {log_path}\n")

    # Load Tool while splash image is shown (only for Windows)
    print("\nLoading Tool...")
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
    print(f"\n[Config] Using config file: {CONFIG_PATH}\n")

    args = parse_args()

    # Determine default input(s) and filters (persist across runs)
    persisted_last_single = load_last_input_dir_from_config()
    persisted_pre, persisted_post = load_last_dual_from_config()

    persisted_allowed_n77_ssb, persisted_allowed_n77_arfcn = load_last_allowed_lists_from_config()


    default_input = args.input or persisted_last_single or INPUT_FOLDER or ""
    default_input_pre = args.input_pre or persisted_pre or INPUT_FOLDER_PRE or ""
    default_input_post = args.input_post or persisted_post or INPUT_FOLDER_POST or ""

    persisted_filters = load_last_filters_from_config()
    default_filters_csv = _normalize_csv_list(args.freq_filters or persisted_filters)

    default_pre = args.freq_pre or DEFAULT_FREQ_PRE
    default_post = args.freq_post or DEFAULT_FREQ_POST

    # Defaults for ARFCN lists (CLI overrides persisted values, which override global defaults)
    default_allowed_n77_ssb_csv = _normalize_csv_list(
        args.allowed_n77_ssb or persisted_allowed_n77_ssb or DEFAULT_ALLOWED_N77_SSB_CSV
    )
    default_allowed_n77_arfcn_csv = _normalize_csv_list(
        args.allowed_n77_arfcn or persisted_allowed_n77_arfcn or DEFAULT_ALLOWED_N77_ARFCN_CSV
    )

    # CASE A: CLI module specified
    if args.module:
        module_fn = resolve_module_callable(args.module)
        if module_fn is None:
            raise SystemExit(f"Unknown module: {args.module}")

        # Consistency-check can use dual-input from CLI
        if module_fn is run_consistency_checks:
            input_pre_dir = args.input_pre or default_input_pre
            input_post_dir = args.input_post or default_input_post

            # If missing and GUI allowed, show GUI with dual layout
            if (not input_pre_dir or not input_post_dir) and not args.no_gui and tk is not None:
                while True:
                    sel = gui_config_dialog(
                        default_input="",  # single not used here
                        default_pre=default_pre,
                        default_post=default_post,
                        default_filters_csv=default_filters_csv,
                        default_input_pre=input_pre_dir,
                        default_input_post=input_post_dir,
                        default_allowed_n77_ssb_csv=default_allowed_n77_ssb_csv,
                        default_allowed_n77_arfcn_csv=default_allowed_n77_arfcn_csv,
                    )
                    if sel is None:
                        raise SystemExit("Cancelled.")
                    input_pre_dir = sel.input_pre_dir
                    input_post_dir = sel.input_post_dir
                    freq_pre = sel.freq_pre
                    freq_post = sel.freq_post
                    freq_filters_csv = sel.freq_filters_csv
                    default_allowed_n77_ssb_csv = sel.allowed_n77_ssb_csv
                    default_allowed_n77_arfcn_csv = sel.allowed_n77_arfcn_csv

                    # Persist last used inputs/filters
                    # Persist last used inputs/filters/allowed lists
                    save_last_dual_to_config(input_pre_dir, input_post_dir)
                    save_last_filters_to_config(freq_filters_csv)
                    save_last_allowed_lists_to_config(
                        default_allowed_n77_ssb_csv,
                        default_allowed_n77_arfcn_csv,
                    )

                    try:
                        execute_module(
                            module_fn,
                            input_dir="",  # unused in dual mode
                            freq_pre=freq_pre,
                            freq_post=freq_post,
                            freq_filters_csv=freq_filters_csv,
                            input_pre_dir=input_pre_dir,
                            input_post_dir=input_post_dir,
                            allowed_n77_ssb_csv=default_allowed_n77_ssb_csv,
                            allowed_n77_arfcn_csv=default_allowed_n77_arfcn_csv,
                        )
                    except Exception as e:
                        log_module_exception(sel.module, e)

                    if not ask_reopen_launcher():
                        break
                return

            # Pure headless CLI (dual-input required)
            if not (input_pre_dir and input_post_dir):
                raise SystemExit("Both --input-pre and --input-post must be provided for consistency-check in headless mode.")
            save_last_dual_to_config(input_pre_dir, input_post_dir)
            save_last_filters_to_config(default_filters_csv)
            save_last_allowed_lists_to_config(
                default_allowed_n77_ssb_csv,
                default_allowed_n77_arfcn_csv,
            )
            execute_module(
                module_fn,
                input_dir="",  # unused in dual mode
                freq_pre=default_pre,
                freq_post=default_post,
                freq_filters_csv=default_filters_csv,
                input_pre_dir=input_pre_dir,
                input_post_dir=input_post_dir,
                allowed_n77_ssb_csv=default_allowed_n77_ssb_csv,
                allowed_n77_arfcn_csv=default_allowed_n77_arfcn_csv,
            )
            return

        # Other modules (single-input)
        input_dir = args.input or default_input
        freq_pre = default_pre
        freq_post = default_post
        freq_filters_csv = default_filters_csv
        allowed_n77_ssb_csv = default_allowed_n77_ssb_csv
        allowed_n77_arfcn_csv = default_allowed_n77_arfcn_csv

        if not input_dir and not args.no_gui and tk is not None:
            while True:
                sel = gui_config_dialog(
                    default_input=default_input,
                    default_pre=freq_pre,
                    default_post=freq_post,
                    default_filters_csv=freq_filters_csv,
                    default_input_pre=default_input_pre,
                    default_input_post=default_input_post,
                    default_allowed_n77_ssb_csv=default_allowed_n77_ssb_csv,
                    default_allowed_n77_arfcn_csv=default_allowed_n77_arfcn_csv,
                )
                if sel is None:
                    raise SystemExit("Cancelled.")
                # Persist last used inputs/filters
                save_last_input_dir_to_config(sel.input_dir)
                save_last_filters_to_config(sel.freq_filters_csv)
                save_last_allowed_lists_to_config(
                    sel.allowed_n77_ssb_csv,
                    sel.allowed_n77_arfcn_csv,
                )
                default_input = sel.input_dir
                freq_pre = sel.freq_pre
                freq_post = sel.freq_post
                freq_filters_csv = sel.freq_filters_csv
                default_allowed_n77_ssb_csv = sel.allowed_n77_ssb_csv
                default_allowed_n77_arfcn_csv = sel.allowed_n77_arfcn_csv

                try:
                    execute_module(
                        module_fn,
                        input_dir=sel.input_dir,
                        freq_pre=sel.freq_pre,
                        freq_post=sel.freq_post,
                        freq_filters_csv=sel.freq_filters_csv,
                        input_pre_dir=sel.input_pre_dir,
                        input_post_dir=sel.input_post_dir,
                        allowed_n77_ssb_csv=sel.allowed_n77_ssb_csv,
                        allowed_n77_arfcn_csv=sel.allowed_n77_arfcn_csv,
                    )
                except Exception as e:
                    log_module_exception(sel.module, e)

                if not ask_reopen_launcher():
                    break
            return

        # Headless single-input path
        if not input_dir:
            raise SystemExit("Input folder not provided.")
        save_last_input_dir_to_config(input_dir)
        save_last_filters_to_config(freq_filters_csv)
        save_last_allowed_lists_to_config(
            default_allowed_n77_ssb_csv,
            default_allowed_n77_arfcn_csv,
        )
        execute_module(
            module_fn,
            input_dir=input_dir,
            freq_pre=freq_pre,
            freq_post=freq_post,
            freq_filters_csv=freq_filters_csv,
            allowed_n77_ssb_csv=allowed_n77_ssb_csv,
            allowed_n77_arfcn_csv=allowed_n77_arfcn_csv,
        )
        return

    # CASE B: No module specified -> GUI (if available)
    if not args.no_gui and tk is not None:
        while True:
            # Use last in-memory defaults so they persist across module runs
            sel = gui_config_dialog(
                default_input=default_input,
                default_pre=default_pre,
                default_post=default_post,
                default_filters_csv=default_filters_csv,
                default_input_pre=default_input_pre,
                default_input_post=default_input_post,
                default_allowed_n77_ssb_csv=default_allowed_n77_ssb_csv,
                default_allowed_n77_arfcn_csv=default_allowed_n77_arfcn_csv,
            )
            if sel is None:
                raise SystemExit("Cancelled.")

            module_fn = resolve_module_callable(sel.module)
            if module_fn is None:
                raise SystemExit(f"Unknown module selected: {sel.module}")

            # Persist inputs appropriately (config file + in-memory defaults)
            if _is_consistency_module(sel.module):
                save_last_dual_to_config(sel.input_pre_dir, sel.input_post_dir)
                input_dir = ""  # unused in dual mode

                # Update in-memory defaults for next iterations
                default_input_pre = sel.input_pre_dir
                default_input_post = sel.input_post_dir
            else:
                save_last_input_dir_to_config(sel.input_dir)
                input_dir = sel.input_dir

                # Update in-memory default for single-input modules
                default_input = sel.input_dir

                # Persist filters, frequencies and allowed lists (config file + in-memory defaults)
                save_last_filters_to_config(sel.freq_filters_csv)
                save_last_allowed_lists_to_config(
                    sel.allowed_n77_ssb_csv,
                    sel.allowed_n77_arfcn_csv,
                )
                default_filters_csv = sel.freq_filters_csv
                default_pre = sel.freq_pre
                default_post = sel.freq_post
                default_allowed_n77_ssb_csv = sel.allowed_n77_ssb_csv
                default_allowed_n77_arfcn_csv = sel.allowed_n77_arfcn_csv

            try:
                execute_module(
                    module_fn,
                    input_dir=input_dir,
                    freq_pre=sel.freq_pre,
                    freq_post=sel.freq_post,
                    freq_filters_csv=sel.freq_filters_csv,
                    input_pre_dir=sel.input_pre_dir,
                    input_post_dir=sel.input_post_dir,
                    allowed_n77_ssb_csv=sel.allowed_n77_ssb_csv,
                    allowed_n77_arfcn_csv=sel.allowed_n77_arfcn_csv,
                )
            except Exception as e:
                log_module_exception(sel.module, e)

            if not ask_reopen_launcher():
                break
        return

    # CASE C: Headless (no GUI)
    # Default to Consistency Checks or require single-input depending on args
    raise SystemExit("Headless start without --module is not supported. Please provide --module and input(s).")

    # (No code beyond this point)
    # Log closing and restoration would happen at the natural process exit.


if __name__ == "__main__":
    main()
