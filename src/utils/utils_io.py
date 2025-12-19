# -*- coding: utf-8 -*-
import configparser
import os
import re
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple, Dict

from src.utils.utils_parsing import normalize_csv_list

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

@dataclass
class Step0RunInfo:
    """Represents a single Step0 run under the logs root."""
    name: str               # folder name (e.g. 20251203_0730_Step0)
    path: str               # full folder path
    date: str               # yyyymmdd
    time_hhmm: str          # hhmm in 24h format
    datetime_key: datetime  # yyyymmddhhmm for sorting


def detect_step0_folders(entry_name: str, base_folder: str) -> Optional[Step0RunInfo]:
    """
    Parse a folder name as a Step0 run.

    Rules:
    - The name MUST contain 'step0' (case-insensitive) anywhere in the string.
    - The name MUST start with:  yyyymmdd_<time>
      where <time> can be:
         * HHMM  (e.g. 0730, 2359)
         * H[H]am or H[H]pm  (e.g. 2am, 02am, 4pm, 11pm)

    Returns:
        Step0RunInfo if the name matches; otherwise None.
    """
    lower_name = entry_name.lower()
    if "step0" not in lower_name:
        return None

    # Match: YYYYMMDD_(HHMM | H[H](am|pm))
    m = re.match(
        r"^(?P<date>\d{8})_(?P<time>\d{4}|\d{1,2}(?:am|pm))",
        entry_name,
        flags=re.IGNORECASE
    )
    if not m:
        return None

    date_str = m.group("date")
    time_token = m.group("time").lower()

    try:
        if re.fullmatch(r"\d{4}", time_token):
            dt = datetime.strptime(date_str + time_token, "%Y%m%d%H%M")
        else:
            # 2am, 11pm, etc.
            m2 = re.match(r"(?P<hour>\d{1,2})(?P<ampm>am|pm)", time_token)
            if not m2:
                return None

            hour = int(m2.group("hour"))
            ampm = m2.group("ampm")

            if hour < 1 or hour > 12:
                return None

            if ampm == "am":
                hour_24 = 0 if hour == 12 else hour
            else:  # pm
                hour_24 = 12 if hour == 12 else hour + 12

            dt = datetime(
                year=int(date_str[:4]),
                month=int(date_str[4:6]),
                day=int(date_str[6:8]),
                hour=hour_24,
                minute=0
            )

            time_token = f"{dt.hour:02d}{dt.minute:02d}"

    except Exception:
        return None

    return Step0RunInfo(
        name=entry_name,
        path=os.path.join(base_folder, entry_name),
        date=date_str,
        time_hhmm=time_token,
        datetime_key=dt
    )


def detect_pre_post_subfolders(base_folder: str, BLACKLIST: tuple) -> Tuple[Optional[str], Optional[str], Dict[str, Tuple[str, str]]]:
    """
    Detect PRE/POST Step0 runs and, inside them, market subfolders.

    Step0 run detection
    -------------------
    - Direct subfolders of base_folder are scanned.
    - A subfolder is considered a "Step0 run" if:
        * detect_step0_folders() returns a Step0RunInfo, i.e.:
            - name contains 'Step0' (anywhere, case-insensitive), AND
            - name starts with 'yyyymmdd_<time>', with <time> in HHMM or H[H](am|pm).

    PRE/POST selection
    ------------------
    Among all detected Step0 runs:

        POST run:
            - The run with the greatest datetime_key (most recent date+time).

        PRE run:
            - If there is more than one run with the SAME date as POST:
                  PRE = the earliest run of that date (smallest datetime_key
                        among runs with that same date).
            - Otherwise:
                  PRE = the most recent run strictly BEFORE POST
                        (largest datetime_key < POST.datetime_key).

        If we cannot find BOTH PRE and POST, we return (None, None, {}).

    Market detection
    ----------------
    For the selected PRE and POST base folders:

        PRE_BASE  = folder of the PRE Step0 run.
        POST_BASE = folder of the POST Step0 run.

        - We look at DIRECT subfolders of PRE_BASE and POST_BASE.
        - For each subfolder, we extract candidate market tokens using
          _extract_market_tokens_from_name().

        Example tokens:
            'Indiana'                -> ['indiana']
            'Indiana_Step0_pre'      -> ['indiana']
            'step0_Indiana_batch_02' -> ['indiana']

        - A token that appears in BOTH PRE and POST sides is considered a market.
        - For each common token:
            * pre_dir  = first PRE subfolder containing that token.
            * post_dir = first POST subfolder containing that token.
            * The market label is taken from the POST folder basename
              (for nicer human-readable names).

        If no market tokens are found on either side, we create a single
        "GLOBAL" market pair using (PRE_BASE, POST_BASE).

    Returns
    -------
        base_pre_dir, base_post_dir, market_pairs

        where market_pairs is a dict:
            { market_label -> (pre_market_dir, post_market_dir) }
    """
    try:
        entries = [e for e in os.scandir(base_folder) if e.is_dir()]
    except FileNotFoundError:
        return None, None, {}

        # ---------------- STEP0 RUN DETECTION ---------------- #
    runs: List[Step0RunInfo] = []
    # Skip Step0 candidate folders whose name contains blacklist tokens
    for entry in entries:
        name_low = entry.name.lower()
        if any(tok in name_low for tok in BLACKLIST):
            continue
        step0_folder_parsed = detect_step0_folders(entry.name, base_folder)
        if step0_folder_parsed:
            runs.append(step0_folder_parsed)

    if len(runs) < 2:
        return None, None, {}

    runs.sort(key=lambda r: r.datetime_key)

    # POST = most recent
    post_run = runs[-1]

    # PRE selection
    same_day = [r for r in runs if r.date == post_run.date]
    if len(same_day) > 1:
        pre_run = same_day[0]
    else:
        earlier = [r for r in runs if r.datetime_key < post_run.datetime_key]
        if not earlier:
            return None, None, {}
        pre_run = earlier[-1]

    base_pre = pre_run.path
    base_post = post_run.path

    # ---------------- MARKET DETECTION ---------------- #

    def scan_side(root: str) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        try:
            for e in os.scandir(root):
                if not e.is_dir():
                    continue
                tokens = extract_tokens_dynamic(e.name)
                for tok in tokens:
                    mapping.setdefault(tok, []).append(e.path)
        except FileNotFoundError:
            pass
        return mapping

    pre_tokens = scan_side(base_pre)
    post_tokens = scan_side(base_post)

    common = set(pre_tokens.keys()) & set(post_tokens.keys())

    market_pairs: Dict[str, Tuple[str, str]] = {}

    if common:
        for tok in sorted(common):
            pre_dirs = sorted(pre_tokens[tok])
            post_dirs = sorted(post_tokens[tok])

            pre_dir = pre_dirs[0]
            post_dir = post_dirs[0]

            label = os.path.basename(post_dir)  # nicer for user
            market_pairs[label] = (pre_dir, post_dir)
    else:
        # fallback: treat PRE/POST base as a single GLOBAL pair
        market_pairs["GLOBAL"] = (base_pre, base_post)

    return base_pre, base_post, market_pairs


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

def folder_has_valid_logs(folder: str) -> bool:
    """
    Return True if 'folder' contains at least one .log / .logs / .txt file
    with a line starting by 'SubNetwork'.

    A "valid" log file is defined as one containing at least one line
    whose first non-BOM, non-whitespace characters start with 'SubNetwork'.

    This check is used to decide whether ConfigurationAudit should run
    inside a folder or whether the recursive search should continue.
    """
    try:
        folder_fs = to_long_path(folder)
        log_files = find_log_files(folder_fs)  # already filters .log/.logs/.txt
    except Exception:
        return False

    for fpath in log_files:
        try:
            fpath_fs = to_long_path(fpath)
            with open(fpath_fs, "r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    stripped = line.lstrip("\ufeff").lstrip()
                    if stripped.startswith("SubNetwork"):
                        return True
        except Exception:
            # If a file cannot be opened or read, skip and continue
            continue

    return False

def extract_tokens_dynamic(name: str) -> List[str]:
    """
    Extracts ALL non-numeric tokens found in a folder name.
    No blacklist. No assumptions.
    Tokens are lowercase alphanumeric sequences.

    Examples:
        "Indiana"                  -> ["indiana"]
        "Indiana_Step0_pre"        -> ["indiana", "step0", "pre"]
        "step0_Indiana_batch_02"   -> ["step0", "indiana", "batch"]
        "Westside_Simulated"       -> ["westside", "simulated"]
        "233_Westside"             -> ["westside"]    (233 ignored)

    Numeric-only tokens are discarded.
    """
    base = os.path.basename(name).lower()
    cleaned = re.sub(r"[^a-z0-9]", " ", base)
    tokens = [t for t in cleaned.split() if t and not t.isdigit()]
    return tokens


def read_text_file(path: str) -> Tuple[List[str], Optional[str]]:
    """
    Thin wrapper around read_text_with_encoding to keep current behavior.
    Returns (lines, encoding_used).
    """
    return read_text_with_encoding(path)


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

def pretty_path(path: str) -> str:
    """Remove Windows long-path prefix (\\?\\) for logging/display."""
    if os.name == "nt" and isinstance(path, str) and path.startswith("\\\\?\\"):
        return path[4:]
    return path

def write_compared_folders_file(output_dir: str, pre_dir: str, post_dir: str, filename: str = "FoldersCompared.txt") -> Optional[str]:
    """
    Write a small text file with the PRE/POST folders used for the comparison.

    File format (2 lines):
      Folder-Pre : <absolute path>
      Folder-Post: <absolute path>

    Returns the full path to the created file, or None if output_dir is missing.
    """
    if not output_dir:
        return None

    output_dir_fs = to_long_path(output_dir)
    os.makedirs(output_dir_fs, exist_ok=True)

    pre_path = pretty_path(to_long_path(pre_dir)) if pre_dir else ""
    post_path = pretty_path(to_long_path(post_dir)) if post_dir else ""

    out_path = os.path.join(output_dir_fs, filename)
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(f"Folder-Pre : {pre_path}\n")
        f.write(f"Folder-Post: {post_path}\n")

    return out_path
