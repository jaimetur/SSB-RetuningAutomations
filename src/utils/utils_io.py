# -*- coding: utf-8 -*-
import configparser
import os
import traceback
from typing import List, Optional, Tuple

# ============================ OPTIONAL TKINTER UI =========================== #
try:
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
except Exception:
    tk = None
    ttk = None
    filedialog = None
    messagebox = None
# ============================ IO / TEXT ============================

ENCODINGS_TRY = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "utf-8"]


def read_text_with_encoding(path: str) -> Tuple[List[str], Optional[str]]:
    # <<< Ensure Windows long path compatibility >>>
    path_long = to_long_path(path)

    for enc in ENCODINGS_TRY:
        try:
            with open(path_long, "r", encoding=enc, errors="strict") as f:
                return [ln.rstrip("\n") for ln in f], enc
        except Exception:
            continue
    with open(path_long, "r", encoding="utf-8", errors="replace") as f:
        return [ln.rstrip("\n") for ln in f], None


def read_text_lines(path: str) -> Optional[List[str]]:
    try:
        lines, _ = read_text_with_encoding(path)
        return lines
    except Exception:
        return None


def try_read_text_file_with_encoding(path: str) -> Tuple[List[str], Optional[str]]:
    """
    Robust text reader that tries several encodings and returns (lines, encoding_used).
    If it falls back to 'replace' mode, returns (lines, None) to signal that encoding is uncertain.
    """
    # <<< Ensure Windows long path compatibility >>>
    path_long = to_long_path(path)

    encodings = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "utf-8"]
    for enc in encodings:
        try:
            with open(path_long, "r", encoding=enc, errors="strict") as f:
                return [ln.rstrip("\n") for ln in f], enc
        except Exception:
            continue
    # last permissive attempt
    with open(path_long, "r", encoding="utf-8", errors="replace") as f:
        return [ln.rstrip("\n") for ln in f], None


def try_read_text_file_lines(path: str) -> Optional[List[str]]:
    """
    Same as above but returns only the lines (used by PrePostRelations.loaders).
    """
    try:
        lines, _ = try_read_text_file_with_encoding(path)
        return lines
    except Exception:
        return None


def find_log_files(folder: str) -> List[str]:
    """
    Return a sorted list of *.log / *.logs / *.txt files found in 'folder'.
    """
    files: List[str] = []

    # <<< Ensure Windows long path compatibility >>>
    folder_long = to_long_path(folder)

    for name in os.listdir(folder_long):
        lower = name.lower()
        if lower.endswith((".log", ".logs", ".txt")):
            p = os.path.join(folder_long, name)
            if os.path.isfile(p):
                files.append(p)
    files.sort()
    return files


def read_text_file(path: str) -> Tuple[List[str], Optional[str]]:
    """
    Thin wrapper around read_text_with_encoding to keep current behavior.
    Returns (lines, encoding_used).
    """
    return read_text_with_encoding(path)


def normalize_csv_list(text: str) -> str:
    """Normalize a comma-separated text into 'a,b,c' without extra spaces/empties."""
    if not text:
        return ""
    items = [t.strip() for t in text.split(",")]
    items = [t for t in items if t]
    return ",".join(items)


def parse_arfcn_csv_to_set(
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
        return set(default_values)

    return set(values)


def ensure_cfg_section(config_section, parser: configparser.ConfigParser) -> None:
    if config_section not in parser:
        parser[config_section] = {}


def read_cfg(config_path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    if config_path.exists():
        parser.read(config_path, encoding="utf-8")
    return parser


def load_cfg_values(config_path, config_section, cfg_field_map, *fields: str) -> dict:
    """
    Load multiple logical fields defined in cfg_field_map.
    Returns a dict {logical_name: value_str} with "" as fallback.
    """
    values = {f: "" for f in fields}
    if not config_path.exists():
        return values

    parser = read_cfg(config_path)
    if config_section not in parser:
        return values

    section = parser[config_section]
    for logical in fields:
        cfg_key = cfg_field_map.get(logical)
        if not cfg_key:
            continue
        values[logical] = section.get(cfg_key, "").strip()
    return values


def save_cfg_values(config_dir, config_path, config_section, cfg_field_map, **kwargs: str) -> None:
    """
    Generates multiple logical fields at once.
    - Applies normalize_csv_list to CSV fields.
    - Don't break execution if something goes wrong.
    """
    if not kwargs:
        return

    try:
        config_dir.mkdir(parents=True, exist_ok=True)
        parser = read_cfg(config_path)
        ensure_cfg_section(config_section, parser)
        section = parser[config_section]

        csv_fields = {"freq_filters", "allowed_n77_ssb", "allowed_n77_arfcn"}

        for logical, value in kwargs.items():
            cfg_key = cfg_field_map.get(logical)
            if not cfg_key:
                continue
            val = value or ""
            if logical in csv_fields:
                val = normalize_csv_list(val)
            section[cfg_key] = val

        with config_path.open("w", encoding="utf-8") as f:
            parser.write(f)
    except Exception:
        # Nunca romper solo por fallo de persistencia
        pass


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


def to_long_path(path: str) -> str:
    r"""
    Convert a normal Windows path to a long-path format with \\?\ prefix.

    Rules:
      - If path already starts with \\?\ it is returned unchanged.
      - If path is a UNC path (\\server\share\...), it becomes \\?\UNC\server\share\...
      - Otherwise, it becomes \\?\C:\... (absolute local path).

    On non-Windows platforms, the path is returned unchanged.
    """
    import os
    if os.name != "nt":
        return path

    if not path:
        return path

    # Normalize to absolute path and backslashes
    abs_path = os.path.abspath(path)
    abs_path = abs_path.replace("/", "\\")

    # Already in long-path form
    if abs_path.startswith("\\\\?\\"):
        return abs_path

    # UNC path: \\server\share\...
    if abs_path.startswith("\\\\"):
        # Strip leading \\ and prefix with \\?\UNC\
        return "\\\\?\\UNC\\" + abs_path.lstrip("\\")
    else:
        # Local drive path: C:\...
        return "\\\\?\\" + abs_path
