# -*- coding: utf-8 -*-
# src/utils/utils_datetime.py

import re
import time
from datetime import datetime
from contextlib import contextmanager
from typing import Callable, Iterator, Iterable, Optional


# --- HELPERS FOR DATE DETECTION ---

def extract_date(folder_name: str) -> Optional[str]:
    """
    Try to find a valid date inside 'folder_name' in many human formats and return it as 'YYYYMMDD'.
    The detection is "intelligent": it validates year, month and day using datetime.strptime.

    Priority rules for ambiguities:
      1) Prefer interpretations that yield the current year.
      2) Prefer formats with 4-digit years (%Y) over 2-digit years (%y).
      3) Prefer candidates containing separators over compact blobs.

    Supported examples (non exhaustive):
      - 20250103, 2025-01-03, 2025_1_3, 2025.01.03
      - 01-03-2025, 1_3_25, 01.03.25
      - 03/01/2025 (both D/M/Y and M/D/Y are attempted)
      - Jan-03-2025, 03-Jan-25, January 3 2025, 3 January 25
      - 2025Jan03, 03Jan2025, 3January2025
      - With or without separators: '-', '_', '.', '/', ' '

    Two-digit years are mapped with a century window [1970..2069].
    Returns 'YYYYMMDD' on success, otherwise None.
    """

    # ------------------------- helpers -------------------------
    def try_parse_with_formats(candidate: str, fmts: Iterable[str]) -> list[tuple[datetime, str]]:
        """Return all successful parses as (datetime, format) to allow scoring selection."""
        results: list[tuple[datetime, str]] = []
        for fmt in fmts:
            try:
                dt = datetime.strptime(candidate, fmt)
                if 1900 <= dt.year <= 2100:
                    # Enforce 2-digit year window [1970..2069]
                    if "%y" in fmt and not (1970 <= dt.year <= 2069):
                        continue
                    results.append((dt, fmt))
            except ValueError:
                continue
        return results

    def normalize_output(dt: datetime) -> str:
        """Return canonical YYYY-MM-DD string."""
        return dt.strftime("%Y-%m-%d")

    # -------------------- candidate generation --------------------
    text = folder_name
    current_year = datetime.now().year
    current_year_str = str(current_year)
    current_year_2d = f"{current_year % 100:02d}"  # kept exactly as in original code, even if unused

    # Tokenize by non-alnum; keep alnum tokens
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", text) if t]

    candidates: set[str] = set()

    # 1) Individual tokens (useful for '2025Jan03' or numeric blobs)
    for t in tokens:
        if len(t) >= 4:
            candidates.add(t)
        if t.isdigit():
            candidates.add(t)

    # 2) Windows joined with/without separators
    seps = ["-", "_", ".", "/", " "]
    for win_size in (2, 3, 4):
        for i in range(0, len(tokens) - win_size + 1):
            window = tokens[i:i + win_size]
            joined_no_sep = "".join(window)
            if 4 <= len(joined_no_sep) <= 16:
                candidates.add(joined_no_sep)
            for sep in seps:
                joined = sep.join(window)
                if 4 <= len(joined) <= 20:
                    candidates.add(joined)

    # 3) Regex slices that look date-ish
    dateish_regexes = [
        r"\b\d{8}\b",                               # 20250103 / 01032025
        r"\b\d{6}\b",                               # 250103 / 111125
        r"\b\d{4}[-_/.\s]\d{1,2}[-_/.\s]\d{1,2}\b",
        r"\b\d{1,2}[-_/.\s]\d{1,2}[-_/.\s]\d{2,4}\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[-_/.\s]?\d{1,2}[-_/.\s]?\d{2,4}\b",
        r"\b\d{1,2}[-_/.\s]?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[-_/.\s]?\d{2,4}\b",
        r"\b\d{4}[-_/.\s]?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[-_/.\s]?\d{1,2}\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\d{1,2}\d{4}\b",  # Jan032025
        r"\b\d{1,2}(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\d{4}\b",  # 03Jan2025
    ]
    for rx in dateish_regexes:
        for m in re.finditer(rx, text, flags=re.IGNORECASE):
            candidates.add(m.group(0))

    if not candidates:
        return None

    # -------------------- formats to attempt --------------------
    # Keep a broad set; scoring will decide which parse wins.
    numeric_YMD = [
        "%Y%m%d", "%Y-%m-%d", "%Y_%m_%d", "%Y.%m.%d", "%Y/%m/%d", "%Y %m %d",
        "%y%m%d", "%y-%m-%d", "%y_%m_%d", "%y.%m.%d", "%y/%m/%d", "%y %m %d",
    ]
    numeric_MDY = [
        "%m%d%Y", "%m-%d-%Y", "%m_%d_%Y", "%m.%d.%Y", "%m/%d/%Y", "%m %d %Y",
        "%m%d%y", "%m-%d-%y", "%m_%d_%y", "%m.%d.%y", "%m/%d/%y", "%m %d %y",
    ]
    numeric_DMY = [
        "%d%m%Y", "%d-%m-%Y", "%d_%m_%Y", "%d.%m.%Y", "%d/%m/%Y", "%d %m %Y",
        "%d%m%y", "%d-%m-%y", "%d_%m_%y", "%d.%m.%y", "%d/%m/%y", "%d %m %y",
    ]
    month_name_variants = [
        # Day Month Year
        "%d-%b-%Y", "%d %b %Y", "%d_%b_%Y", "%d.%b.%Y", "%d/%b/%Y",
        "%d-%B-%Y", "%d %B %Y", "%d_%B_%Y", "%d.%B.%Y", "%d/%B/%Y",
        # Month Day Year
        "%b-%d-%Y", "%b %d %Y", "%b_%d_%Y", "%b.%d.%Y", "%b/%d/%Y",
        "%B-%d-%Y", "%B %d %Y", "%B_%d_%Y", "%B.%d.%Y", "%B/%d/%Y",
        # Year Month Day
        "%Y-%b-%d", "%Y %b %d", "%Y_%b_%d", "%Y.%b.%d", "%Y/%b/%d",
        "%Y-%B-%d", "%Y %B %d", "%Y_%B_%d", "%Y.%B.%d", "%Y/%B/%d",
        # Compact without separators like 03Jan2025 / Jan032025 / 2025Jan03
        "%d%b%Y", "%b%d%Y", "%Y%b%d",
        "%d%B%Y", "%B%d%Y", "%Y%B%d",
        # Two-digit year with names
        "%d-%b-%y", "%d %b %y", "%b %d %y", "%y %b %d", "%b%d%y", "%d%b%y",
        "%d-%B-%y", "%d %B %y", "%B %d %y", "%y %B %d", "%B%d%y", "%d%B%y",
    ]
    all_formats: list[str] = numeric_YMD + numeric_MDY + numeric_DMY + month_name_variants

    # -------------------- parsing with scoring --------------------
    # We will evaluate all successful parses per candidate and select the best by a score tuple.
    # Lower score is better.
    def score_parse(candidate: str, dt: datetime, fmt: str) -> tuple[int, int, int, int]:
        """Build a priority score: (year_match, four_digit_year, has_separators, candidate_len)."""
        year_match = 0 if dt.year == current_year else 1
        four_digit = 0 if "%Y" in fmt else 1
        has_seps = 0 if any(sep in candidate for sep in seps) else 1
        # Slight bias towards shorter candidates to avoid over-greedy matches
        return (year_match, four_digit, has_seps, len(candidate))

    best_dt: Optional[datetime] = None
    best_score: Optional[tuple[int, int, int, int]] = None

    # Try candidates that contain the current year first (fast path).
    # This improves cases like "...2025-11-11..." where we clearly want 2025.
    prioritized_candidates = sorted(
        candidates,
        key=lambda c: (current_year_str not in c, len(c))
    )

    for candidate in prioritized_candidates:
        parses = try_parse_with_formats(candidate, all_formats)
        if not parses:
            continue

        # Among all parses for this candidate, pick by score
        for dt, fmt in parses:
            # If the candidate is a pure 6-digit blob, prioritize interpretations that place %y at the END
            # (i.e., mmddyy or ddmmyy) because users often encode '...yy' as the last two digits.
            if candidate.isdigit() and len(candidate) == 6:
                if "%y" in fmt and fmt.endswith("%y"):
                    # small bonus: improve score artificially
                    sc = score_parse(candidate, dt, fmt)
                    sc = (sc[0], sc[1], sc[2], max(sc[3] - 1, 0))
                else:
                    sc = score_parse(candidate, dt, fmt)
            else:
                sc = score_parse(candidate, dt, fmt)

            # Pick the best-scoring parse globally
            if best_score is None or sc < best_score:
                best_score = sc
                best_dt = dt

            # Early exit if perfect score (current year + 4-digit + separators)
            if sc[:3] == (0, 0, 0):
                return normalize_output(dt)

    return normalize_output(best_dt) if best_dt else None


def format_duration_hms(seconds: float) -> str:
    """Return duration as H:MM:SS.mmm (milliseconds precision)."""
    ms = int((seconds - int(seconds)) * 1000)
    total_seconds = int(seconds)
    hours, rem = divmod(total_seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}.{ms:03d}"


def format_duration_hms(seconds: float) -> str:
    """
    Format a duration in seconds into HH:MM:SS (and milliseconds when useful).
    """
    if seconds is None:
        return "00:00:00"
    try:
        total_ms = int(round(float(seconds) * 1000.0))
    except Exception:
        return "00:00:00"
    ms = total_ms % 1000
    total_s = total_ms // 1000
    s = total_s % 60
    total_m = total_s // 60
    m = total_m % 60
    h = total_m // 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"
    return f"{m:02d}:{s:02d}.{ms:03d}"


@contextmanager
def log_phase_timer(
    phase_name: str,
    log_fn: Callable[[str], None],
    show_start: bool = True,
    show_end: bool = False,
    show_timing: bool = True,
    line_prefix: str = "",
    start_level: str = "INFO",
    end_level: str = "INFO",
    timing_level: str = "INFO",
) -> Iterator[None]:
    """
    Context manager to log phase boundaries and elapsed time.

    Behavior (configurable):
      - show_start=True prints only START line (no END unless show_end=True).
      - show_end=True prints END line.
      - show_timing=True prints the elapsed time line.

    All lines can be labeled as INFO/DEBUG/etc. via *_level params.
    """
    start = time.perf_counter()

    if show_start:
        log_fn(f"{line_prefix}[{start_level}] {phase_name} (START)")

    try:
        yield
    finally:
        elapsed = time.perf_counter() - start

        if show_end:
            log_fn(f"{line_prefix}[{end_level}] {phase_name} (END)")

        if show_timing:
            log_fn(f"{line_prefix}[{timing_level}] {phase_name} took {format_duration_hms(elapsed)} ({elapsed:.3f}s)")
