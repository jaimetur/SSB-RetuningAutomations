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
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import textwrap

# Import our different Classes
from PrePostRelations import PrePostRelations
from CreateExcelFromLogs import CreateExcelFromLogs
from CleanUp import CleanUp

# ================================ VERSIONING ================================ #

TOOL_NAME           = "RetuningAutomations"
TOOL_VERSION        = "0.2.1"
TOOL_DATE           = "2025-11-06"
TOOL_NAME_VERSION   = f"{TOOL_NAME}_v{TOOL_VERSION}"
COPYRIGHT_TEXT      = "(c) 2025 - Jaime Tur (jaime.tur@ericsson.com)"
TOOL_DESCRIPTION    = textwrap.dedent(f"""
{TOOL_NAME_VERSION} - {TOOL_DATE}
Multi-Platform/Multi-Arch tool designed to Automate some process during SSB Retuning
©️ 2025 by Jaime Tur (jaime.tur@ericsson.com)
"""
                                      )
# ================================ DEFAULTS ================================= #

DEFAULT_FREQ_PRE = "648672"
DEFAULT_FREQ_POST = "647328"

# Optional hardcoded constant (can be defined manually)
try:
    INPUT_FOLDER = r"c:\Users\ejaitur\OneDrive - Ericsson\SSB Retuning Project Sharepoint - Scripts\OutputStep0\Output\ToyCells\PA6"
except NameError:
    INPUT_FOLDER = ""  # empty by default if not defined


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


MODULE_OPTIONS = [
    "1. Create Excel from Logs",
    "2. Pre/Post Relations Consistency Check",
    "3. Clean-Up",
]


def gui_config_dialog(
    default_input: str = "",
    default_pre: str = DEFAULT_FREQ_PRE,
    default_post: str = DEFAULT_FREQ_POST,
) -> Optional[GuiResult]:
    """
    Opens a single modal window with:
      - Combobox (module)
      - Input folder (entry + Browse)
      - Freq Pre (entry)
      - Freq Post (entry)
      - Run / Cancel
    Returns GuiResult or None if cancelled/unavailable.
    """
    if tk is None or ttk is None or filedialog is None:
        return None

    root = tk.Tk()
    root.title("Select task and configuration")
    root.resizable(False, False)

    # Center window
    try:
        root.update_idletasks()
        w, h = 520, 240
        sw = root.winfo_screenwidth()
        sh = root.winfo_screenheight()
        x = (sw // 2) - (w // 2)
        y = (sh // 3) - (h // 2)
        root.geometry(f"{w}x{h}+{x}+{y}")
    except Exception:
        pass

    # --- Vars
    module_var = tk.StringVar(value=MODULE_OPTIONS[0])
    input_var = tk.StringVar(value=default_input or "")
    pre_var = tk.StringVar(value=default_pre or "")
    post_var = tk.StringVar(value=default_post or "")
    result: Optional[GuiResult] = None

    # --- Layout
    pad = {'padx': 10, 'pady': 6}
    frm = ttk.Frame(root, padding=12)
    frm.pack(fill="both", expand=True)

    # Row 0: Module
    ttk.Label(frm, text="Module to run:").grid(row=0, column=0, sticky="w", **pad)
    cmb = ttk.Combobox(frm, textvariable=module_var, values=MODULE_OPTIONS, state="readonly", width=36)
    cmb.grid(row=0, column=1, columnspan=2, sticky="ew", **pad)

    # Row 1: Input folder
    ttk.Label(frm, text="Input folder:").grid(row=1, column=0, sticky="w", **pad)
    ent_input = ttk.Entry(frm, textvariable=input_var, width=42)
    ent_input.grid(row=1, column=1, sticky="ew", **pad)

    def browse():
        path = filedialog.askdirectory(title="Select input folder", initialdir=input_var.get() or os.getcwd())
        if path:
            input_var.set(path)

    ttk.Button(frm, text="Browse…", command=browse).grid(row=1, column=2, sticky="ew", **pad)

    # Row 2-3: Frequencies
    ttk.Label(frm, text="Frequency (Pre):").grid(row=2, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=pre_var, width=18).grid(row=2, column=1, sticky="w", **pad)
    ttk.Label(frm, text="Frequency (Post):").grid(row=3, column=0, sticky="w", **pad)
    ttk.Entry(frm, textvariable=post_var, width=18).grid(row=3, column=1, sticky="w", **pad)

    # Row 4: Buttons
    btns = ttk.Frame(frm)
    btns.grid(row=4, column=0, columnspan=3, sticky="e", **pad)

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
    parser = argparse.ArgumentParser(description="Launcher for Pre/Post tools with GUI fallback.")
    parser.add_argument(
        "--module",
        choices=["excel", "prepost", "cleanup"],
        help="Module to run: prepost|excel|cleanup. If omitted, GUI appears (unless --no-gui)."
    )
    parser.add_argument("-i", "--input", help="Input folder to process")
    parser.add_argument("--freq-pre", help="Frequency before refarming (Pre)")
    parser.add_argument("--freq-post", help="Frequency after refarming (Post)")
    parser.add_argument("--no-gui", action="store_true", help="Disable GUI prompts (require CLI args)")
    return parser.parse_args()


# ============================== RUNNERS (TASKS) ============================= #

def run_excel_from_logs(input_dir: str) -> None:
    module_name = "[Create Excel from Logs]"
    print(f"{module_name} Running…")
    print(f"{module_name} Input folder: '{input_dir}'")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"
    app = CreateExcelFromLogs()
    out = app.run(input_dir, module_name=module_name, versioned_suffix=versioned_suffix)

    if out:
        print(f"{module_name} Done → '{out}'")
    else:
        print(f"{module_name}  No logs found or nothing written.")


def run_prepost(input_dir: str, freq_pre: Optional[str], freq_post: Optional[str]) -> None:
    module_name = "[Pre/Post Relations Consistency Checks]"
    print(f"{module_name} Running…")
    print(f"{module_name} Input folder: '{input_dir}'")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"
    output_dir = os.path.join(input_dir, f"CellRelationConsistencyChecks_{versioned_suffix}")

    app = PrePostRelations()
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


def run_cleanup(input_dir: str, *_args) -> None:
    module_name = "[Clean-Up]"
    print(f"{module_name} Running…")
    print(f"{module_name} Input folder: '{input_dir}'")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    versioned_suffix = f"{timestamp}_v{TOOL_VERSION}"

    app = CleanUp()
    out = app.run(input_dir, module_name=module_name, versioned_suffix=versioned_suffix)

    if out:
        print(f"{module_name} Done → '{out}'")
    else:
        # print(f"{module_name} No logs found or nothing written.")
        print(f"{module_name} Module logic not yet implemented (under development). Exiting...")


def resolve_module_callable(name: str):
    name = (name or "").strip().lower()
    if name in ("excel", "1. create excel from logs"):
        return run_excel_from_logs
    if name in ("prepost", "2. pre/post relations consistency check"):
        return run_prepost
    if name in ("cleanup", "3. clean-up", "clean up"):
        return run_cleanup
    return None


# ================================== MAIN =================================== #

def main():
    args = parse_args()

    # Determine default input folder
    default_input = args.input or INPUT_FOLDER or ""
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
        if not input_dir and not args.no_gui and tk is not None:
            sel = gui_config_dialog(default_input="", default_pre=freq_pre, default_post=freq_post)
            if sel is None:
                raise SystemExit("Cancelled.")
            input_dir = sel.input_dir
            freq_pre = sel.freq_pre
            freq_post = sel.freq_post
        if not input_dir:
            raise SystemExit("Input folder not provided.")
        if module_fn is run_excel_from_logs:
            run_excel_from_logs(input_dir, freq_pre, freq_post)
        elif module_fn is run_prepost:
            run_prepost(input_dir, freq_pre, freq_post)
        elif module_fn is run_cleanup:
            run_cleanup(input_dir, freq_pre, freq_post)
        else:
            module_fn(input_dir, freq_pre, freq_post)
        return

    # CASE B: No module specified -> GUI (if available)
    if not args.no_gui and tk is not None:
        sel = gui_config_dialog(default_input=default_input, default_pre=default_pre, default_post=default_post)
        if sel is None:
            raise SystemExit("Cancelled.")
        module_fn = resolve_module_callable(sel.module)
        if module_fn is None:
            raise SystemExit(f"Unknown module selected: {sel.module}")
        if module_fn is run_excel_from_logs:
            run_excel_from_logs(sel.input_dir)
        elif module_fn is run_prepost:
            run_prepost(sel.input_dir, sel.freq_pre, sel.freq_post)
        elif module_fn is run_cleanup:
            run_cleanup(sel.input_dir, sel.freq_pre, sel.freq_post)
        else:
            module_fn(sel.input_dir, sel.freq_pre, sel.freq_post)
        return

    # CASE C: Headless (no GUI)
    if not args.input:
        raise SystemExit("Input folder not provided and GUI disabled/unavailable. Use -i/--input.")
    run_prepost(args.input, args.freq_pre or DEFAULT_FREQ_PRE, args.freq_post or DEFAULT_FREQ_POST)


if __name__ == "__main__":
    print (TOOL_DESCRIPTION)
    main()
