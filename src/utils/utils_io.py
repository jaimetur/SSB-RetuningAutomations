# -*- coding: utf-8 -*-
import configparser
import os
import traceback
from typing import List, Optional, Tuple

from src.RetuningAutomations import CONFIG_PATH, CONFIG_SECTION, CFG_FIELD_MAP, CONFIG_DIR, messagebox

# ============================ IO / TEXT ============================

ENCODINGS_TRY = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "utf-8"]


def read_text_with_encoding(path: str) -> Tuple[List[str], Optional[str]]:
    for enc in ENCODINGS_TRY:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return [ln.rstrip("\n") for ln in f], enc
        except Exception:
            continue
    with open(path, "r", encoding="utf-8", errors="replace") as f:
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
    encodings = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "utf-8"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return [ln.rstrip("\n") for ln in f], enc
        except Exception:
            continue
    # last permissive attempt
    with open(path, "r", encoding="utf-8", errors="replace") as f:
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
    for name in os.listdir(folder):
        lower = name.lower()
        if lower.endswith((".log", ".logs", ".txt")):
            p = os.path.join(folder, name)
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


def read_cfg() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    if CONFIG_PATH.exists():
        parser.read(CONFIG_PATH, encoding="utf-8")
    return parser

def ensure_cfg_section(parser: configparser.ConfigParser) -> None:
    if CONFIG_SECTION not in parser:
        parser[CONFIG_SECTION] = {}

def load_cfg_values(*fields: str) -> dict:
    """
    Load multiple logical fields defined in CFG_FIELD_MAP.
    Returns a dict {logical_name: value_str} with "" as fallback.
    """
    values = {f: "" for f in fields}
    if not CONFIG_PATH.exists():
        return values

    parser = read_cfg()
    if CONFIG_SECTION not in parser:
        return values

    section = parser[CONFIG_SECTION]
    for logical in fields:
        cfg_key = CFG_FIELD_MAP.get(logical)
        if not cfg_key:
            continue
        values[logical] = section.get(cfg_key, "").strip()
    return values


def save_cfg_values(**kwargs: str) -> None:
    """
    Generates multiple logical fields at once.
    - Applies normalize_csv_list to CSV fields.
    - Don't break execution if something goes wrong.
    """
    if not kwargs:
        return

    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        parser = read_cfg()
        ensure_cfg_section(parser)
        section = parser[CONFIG_SECTION]

        csv_fields = {"freq_filters", "allowed_n77_ssb", "allowed_n77_arfcn"}

        for logical, value in kwargs.items():
            cfg_key = CFG_FIELD_MAP.get(logical)
            if not cfg_key:
                continue
            val = value or ""
            if logical in csv_fields:
                val = normalize_csv_list(val)
            section[cfg_key] = val

        with CONFIG_PATH.open("w", encoding="utf-8") as f:
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

