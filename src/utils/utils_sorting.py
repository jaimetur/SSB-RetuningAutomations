# -*- coding: utf-8 -*-

import os
import re
from typing import Tuple, Optional


# --- NEW HELPERS FOR NATURAL FILES SORTING ---

def split_stem_counter(stem: str) -> Tuple[str, Optional[int]]:
    """
    Split a filename stem into (base_stem, numeric_counter) when it ends with '(N)'.
    Example:
      'Data_Collection_MKT_188(10)' -> ('Data_Collection_MKT_188', 10)
      'Data_Collection_MKT_188'     -> ('Data_Collection_MKT_188', None)
    """
    m = re.search(r"\((\d+)\)\s*$", stem)
    if m:
        return stem[:m.start()].rstrip(), int(m.group(1))
    return stem, None


def natural_logfile_key(path: str) -> Tuple[str, int, int]:
    """
    Generate a natural sorting key for log/text files with '(N)' suffixes.
    Sorts files by:
      1) Base stem (without '(N)') in lowercase
      2) Files *without* counter first, then those *with* counter
      3) Numeric counter ascending
    Example desired order:
      file.txt, file(1).txt, file(2).txt, ..., file(10).txt, file(11).txt
    """
    base = os.path.basename(path)
    stem, _ = os.path.splitext(base)
    stem_base, counter = split_stem_counter(stem)
    has_counter_flag = 1 if counter is not None else 0
    return (stem_base.lower(), has_counter_flag, counter or 0)
