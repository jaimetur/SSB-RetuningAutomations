# -*- coding: utf-8 -*-

"""
Helpers to export Correction_Cmd  to text files.
"""

from __future__ import annotations

import os
import re
import pandas as pd
from typing import Dict, Optional, List, Tuple

from src.utils.utils_io import to_long_path, pretty_path

# ----------------------------------------------------------------------
#  INTERNAL HELPERS
# ----------------------------------------------------------------------
_DEL_LINE_RE = re.compile(r"^\s*del\b", re.IGNORECASE)

def _merge_blocks_hoist_header_footer(blocks: List[str], header_lines: int = 3, footer_lines: int = 1) -> str:
    """
    Merge multiple multi-line command blocks into a single script.

    Rules:
      - Hoist a common header of up to `header_lines` identical leading lines across all blocks (typically 3).
      - Hoist a common footer of up to `footer_lines` identical trailing lines across all blocks (typically 1).
      - Keep each block body in the same order as in Excel.
      - Insert ONE blank line before the first body line of EACH block (including the first),
        so blocks are visually separated like in Excel.
      - Do NOT try to merge phases by 'wait' (no optimization/reordering).
    """
    if not blocks:
        return ""

    normalized_blocks: List[List[str]] = []
    for b in blocks:
        if b is None:
            continue
        text = str(b).replace("\r\n", "\n").replace("\r", "\n").strip("\n")
        if not text.strip():
            continue
        lines = [ln.rstrip() for ln in text.split("\n") if ln.strip() != ""]
        if lines:
            normalized_blocks.append(lines)

    if not normalized_blocks:
        return ""

    # -----------------------------
    # Common header
    # -----------------------------
    min_len = min(len(x) for x in normalized_blocks)
    max_header = min(header_lines, min_len)
    common_header: List[str] = []

    for i in range(max_header):
        candidate = normalized_blocks[0][i].strip()
        if all(x[i].strip() == candidate for x in normalized_blocks[1:]):
            common_header.append(normalized_blocks[0][i].strip())
        else:
            break

    # -----------------------------
    # Common footer (up to footer_lines)
    # -----------------------------
    common_footer: List[str] = []
    max_footer = min(footer_lines, min_len - len(common_header)) if min_len > len(common_header) else 0

    for i in range(1, max_footer + 1):
        candidate = normalized_blocks[0][-i].strip()
        if all(x[-i].strip() == candidate for x in normalized_blocks[1:]):
            common_footer.insert(0, candidate)  # keep order
        else:
            break

    # -----------------------------
    # Build output
    # -----------------------------
    out_lines: List[str] = []
    out_lines.extend(common_header)

    def _ensure_blank_line():
        if out_lines and out_lines[-1] != "":
            out_lines.append("")
        elif not out_lines:
            out_lines.append("")

    for lines in normalized_blocks:
        body = lines[len(common_header):]

        if common_footer:
            # Remove matching footer length from this block if it ends with the common footer
            if len(body) >= len(common_footer) and [x.strip() for x in body[-len(common_footer):]] == common_footer:
                body = body[:-len(common_footer)]

        body = [ln.strip() for ln in body if ln.strip() != ""]
        if not body:
            continue

        _ensure_blank_line()
        out_lines.extend(body)

    if common_footer:
        if out_lines and out_lines[-1] == "":
            out_lines.pop()
        out_lines.extend(common_footer)

    while out_lines and out_lines[0] == "":
        out_lines.pop(0)
    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines).strip()



def _extract_del_lines_and_rest(cmd: object) -> Tuple[List[str], str]:
    """
    Split a command block into:
      - del_lines: consecutive 'del ...' lines at the very beginning of the block (ignoring blank lines)
      - rest: remaining text (may be empty)
    This is useful for discrepancy blocks where we may have:
      del ...
      crn ...
      ...
    """
    if cmd is None:
        return [], ""

    text = str(cmd)
    if not text.strip():
        return [], ""

    raw_lines = text.splitlines()

    # Skip leading empty lines
    i = 0
    n = len(raw_lines)
    while i < n and raw_lines[i].strip() == "":
        i += 1

    del_lines: List[str] = []

    # Collect consecutive del lines at the top
    j = i
    while j < n:
        line = raw_lines[j]
        if line.strip() == "":
            # Allow blank lines inside the initial del block (rare) - skip them
            j += 1
            continue
        if _DEL_LINE_RE.match(line):
            del_lines.append(line.strip())
            j += 1
            continue
        break

    # Rest is everything from j onwards
    rest = "\n".join(raw_lines[j:]).strip()

    return del_lines, rest


def _reorder_cmds_del_first(cmds: List[object]) -> List[str]:
    """
    Given a list of command blocks (strings), returns a new list of blocks where:
      - all extracted 'del ...' lines are placed first (one per line)
      - then blocks containing 'set External...' (kept as full blocks, not line-splitted)
      - followed by the remaining blocks (typically 'crn ...' creations and other scripts)
    """
    set_external_re = re.compile(r"^\s*set\s+External", re.IGNORECASE | re.MULTILINE)

    del_lines_all: List[str] = []
    set_external_blocks: List[str] = []
    rest_blocks: List[str] = []

    for c in cmds:
        del_lines, rest = _extract_del_lines_and_rest(c)
        if del_lines:
            del_lines_all.extend(del_lines)
        if rest:
            if set_external_re.search(rest):
                set_external_blocks.append(rest)
            else:
                rest_blocks.append(rest)

    out: List[str] = []
    if del_lines_all:
        out.extend(del_lines_all)
    if set_external_blocks:
        out.extend(set_external_blocks)
    if rest_blocks:
        out.extend(rest_blocks)

    return out



# ----------------------------------------------------------------------
#  EXPORT CORRECTION COMMNANDS TO TEXT FILES
# ----------------------------------------------------------------------
def export_correction_cmd_texts(output_dir: str, dfs_by_category: Dict[str, pd.DataFrame], base_folder_name: str = "Correction_Cmd") -> int:
    """
    Export Correction_Cmd values to text files grouped by NodeId and category.

    For each category (e.g. GU_missing, NR_new), one file per NodeId is created in:
      <output_dir>/Correction_Cmd/<GroupFolder>/<NR|GU>/<NodeId>_<Category>.txt

    Additionally:
      - All 'del ...' commands are moved to the top of each node file.
    """

    def _detect_layer_from_category(category: str) -> str:
        s = str(category or "").strip().upper()
        if s.startswith("NR"):
            return "NR"
        if s.startswith("GU"):
            return "GU"

        # Fallbacks if naming is not strictly prefix-based
        if "NR_" in s or "_NR" in s or "NR-" in s:
            return "NR"
        if "GU_" in s or "_GU" in s or "GU-" in s:
            return "GU"
        return ""

    base_dir = os.path.join(output_dir, base_folder_name)
    os.makedirs(base_dir, exist_ok=True)

    # Renamed folders
    new_dir = os.path.join(base_dir, "NewRelations")
    missing_dir = os.path.join(base_dir, "MissingRelations")
    discrepancies_dir = os.path.join(base_dir, "RelationsDiscrepancies")
    os.makedirs(new_dir, exist_ok=True)
    os.makedirs(missing_dir, exist_ok=True)
    os.makedirs(discrepancies_dir, exist_ok=True)

    # Subfolders to distinguish NR vs GU
    for parent in (new_dir, missing_dir, discrepancies_dir):
        os.makedirs(os.path.join(parent, "NR"), exist_ok=True)
        os.makedirs(os.path.join(parent, "GU"), exist_ok=True)

    total_files = 0

    for category, df in dfs_by_category.items():
        if df is None or df.empty:
            continue
        if "NodeId" not in df.columns or "Correction_Cmd" not in df.columns:
            continue

        work = df.copy()
        work["NodeId"] = work["NodeId"].astype(str).str.strip()
        work["Correction_Cmd"] = work["Correction_Cmd"].astype(str)

        category_lower = str(category).lower()

        # Choose the "group" folder (New/Missing/Discrepancies/Other)
        if "new" in category_lower:
            parent_dir = new_dir
        elif "missing" in category_lower:
            parent_dir = missing_dir
        elif "disc" in category_lower:
            parent_dir = discrepancies_dir
        else:
            parent_dir = base_dir

        # If it's one of the 3 main folders, route into NR/GU subfolder when possible
        target_dir = parent_dir
        if parent_dir in (new_dir, missing_dir, discrepancies_dir):
            layer = _detect_layer_from_category(category)
            if layer in ("NR", "GU"):
                target_dir = os.path.join(parent_dir, layer)

        os.makedirs(target_dir, exist_ok=True)

        for node_id, group in work.groupby("NodeId"):
            node_str = str(node_id).strip()
            if not node_str:
                continue

            raw_cmds = [cmd for cmd in group["Correction_Cmd"] if str(cmd).strip()]
            if not raw_cmds:
                continue

            # NEW: put all del first
            ordered_blocks = _reorder_cmds_del_first(raw_cmds)
            if not ordered_blocks:
                continue

            # Write:
            # - del lines as single lines
            # - rest blocks separated by blank line
            del_lines = [b for b in ordered_blocks if _DEL_LINE_RE.match(b)]
            rest_blocks = [b for b in ordered_blocks if not _DEL_LINE_RE.match(b)]

            pieces: List[str] = []
            if del_lines:
                pieces.append("\n".join(del_lines).strip())
            if rest_blocks:
                pieces.append("\n\n".join(rest_blocks).strip())

            final_text = "\n\n".join([p for p in pieces if p.strip()]).strip()
            if not final_text:
                continue

            file_name = f"{node_str}_{category}.txt"
            file_path = os.path.join(target_dir, file_name)
            file_path_long = to_long_path(file_path)

            with open(file_path_long, "w", encoding="utf-8") as f:
                f.write(final_text)

            total_files += 1

    print(f"\n[Correction Commands] Generated {total_files} Correction_Cmd files in: '{pretty_path(base_dir)}'")
    return total_files


# ----------------------------- EXTERNAL/TERMPOINTS COMMANDS ----------------------------- #
def export_external_and_termpoint_commands(audit_post_excel: str, output_dir: str, base_folder_name: str = "Correction_Cmd") -> int:
    """
    Export correction commands coming from POST Configuration Audit Excel:
      - ExternalNRCellCU (SSB-Post)
      - ExternalNRCellCU (Unknown)
      - ExternalGUtranCell (SSB-Post)
      - ExternalGUtranCell (Unknown)
      - TermPointToGNB
      - TermPointToGNodeB

    Files are written under:
      <output_dir>/Correction_Cmd/ExternalNRCellCU/{SSB-Post,Unknown}
      <output_dir>/Correction_Cmd/ExternalGUtranCell/{SSB-Post,Unknown}
      <output_dir>/Correction_Cmd/TermPointToGNB
      <output_dir>/Correction_Cmd/TermPointToGNodeB/{SSB-Post,Unknown}

    One text file per NodeId is generated (grouped like other Correction_Cmd exports).

    Additionally:
      - All 'del ...' commands are moved to the top of each node file.
    """

    def _read_sheet_case_insensitive(audit_excel: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """
        Read a sheet using case-insensitive matching. Returns None if not found or on error.
        """
        if not audit_excel or not os.path.isfile(audit_excel):
            return None

        try:
            xl = pd.ExcelFile(audit_excel)
            sheet_map = {s.lower(): s for s in xl.sheet_names}
            real_sheet = sheet_map.get(str(sheet_name).lower())
            if not real_sheet:
                return None
            return xl.parse(real_sheet)
        except Exception:
            return None

    def _export_grouped_commands_from_sheet(
            audit_excel: str,
            sheet_name: str,
            output_dir: str,
            command_column: str = "Correction_Cmd",
            node_column: str = "NodeId",
            filter_column: Optional[str] = None,
            filter_values: Optional[list[str]] = None,
            filename_suffix: Optional[str] = None,
    ) -> int:
        """
        Export Correction_Cmd grouped by NodeId from a given sheet into output_dir.
        Returns how many files were generated.

        Behavior:
          - 'del ...' lines are moved to the top of each node file.
          - For External* and TermPoint* blocks, we keep per-block order exactly as in Excel,
            but hoist ONLY the first 3 lines and the last line once per node file.
            (No special single-instance handling for 'wait' or 'lt all' after wait.)
        """
        df = _read_sheet_case_insensitive(audit_excel, sheet_name)
        if df is None or df.empty:
            return 0

        # Allow backward compatibility with old column name
        if command_column not in df.columns and "Correction Command" in df.columns:
            command_column = "Correction Command"

        if command_column not in df.columns:
            return 0
        if node_column not in df.columns:
            return 0

        # IMPORTANT: if caller requested a filter but column is missing, do not export to avoid mixing targets
        if filter_column and filter_values is not None and filter_column not in df.columns:
            return 0

        if filter_column and filter_column in df.columns and filter_values is not None:
            allowed = {str(v).strip() for v in filter_values}
            df = df[df[filter_column].astype(str).str.strip().isin(allowed)]

        if df.empty:
            return 0

        os.makedirs(output_dir, exist_ok=True)

        work = df.copy()
        work[node_column] = work[node_column].astype(str).str.strip()

        suffix = filename_suffix if filename_suffix else str(sheet_name).strip()
        generated_files = 0

        for node_id, group in work.groupby(node_column):
            node_str = str(node_id).strip()
            if not node_str:
                continue

            # IMPORTANT: do NOT cast the whole column to str before dropna(), otherwise NaN becomes "nan"
            raw_series = group[command_column]
            raw_series = raw_series[raw_series.notna()]

            cmds = (
                raw_series
                .astype(str)
                .map(str.strip)
                .loc[lambda s: (s != "") & (s.str.lower() != "nan") & (s.str.lower() != "none")]
                .tolist()
            )

            if not cmds:
                continue

            # del first (and keep rest blocks after)
            ordered_blocks = _reorder_cmds_del_first(cmds)
            if not ordered_blocks:
                continue

            del_lines = [b for b in ordered_blocks if _DEL_LINE_RE.match(b)]
            rest_blocks = [b for b in ordered_blocks if not _DEL_LINE_RE.match(b)]

            # NEW: HOIST only the first 3 lines and the last line ONCE per node
            merged_rest = _merge_blocks_hoist_header_footer(rest_blocks, header_lines=3, footer_lines=1) if rest_blocks else ""

            pieces: List[str] = []
            if del_lines:
                pieces.append("\n".join(del_lines).strip())
            if merged_rest.strip():
                pieces.append(merged_rest.strip())

            merged_script = "\n\n".join([p for p in pieces if p.strip()]).strip()
            if not merged_script:
                continue

            file_name = f"{node_str}_{suffix}.txt"
            out_path = os.path.join(output_dir, file_name)
            out_path_long = to_long_path(out_path)

            with open(out_path_long, "w", encoding="utf-8") as f:
                f.write(merged_script)

            generated_files += 1

        return generated_files


    if not audit_post_excel or not os.path.isfile(audit_post_excel):
        return 0

    base_dir = os.path.join(output_dir, base_folder_name)

    ext_nr_base = os.path.join(base_dir, "ExternalNRCellCU")
    ext_nr_ssbpost_dir = os.path.join(ext_nr_base, "SSB-Post")
    ext_nr_unknown_dir = os.path.join(ext_nr_base, "Unknown")

    ext_gu_base = os.path.join(base_dir, "ExternalGUtranCell")
    ext_gu_ssbpost_dir = os.path.join(ext_gu_base, "SSB-Post")
    ext_gu_unknown_dir = os.path.join(ext_gu_base, "Unknown")

    tp_gnb_dir = os.path.join(base_dir, "TermPointToGNB")
    tp_gnb_ssbpost_dir = os.path.join(tp_gnb_dir, "SSBPost")
    tp_gnb_unknown_dir = os.path.join(tp_gnb_dir, "Unknown")

    tp_gnodeb_base = os.path.join(base_dir, "TermPointToGNodeB")
    tp_gnodeb_ssbpost_dir = os.path.join(tp_gnodeb_base, "SSB-Post")
    tp_gnodeb_unknown_dir = os.path.join(tp_gnodeb_base, "Unknown")

    os.makedirs(ext_nr_ssbpost_dir, exist_ok=True)
    os.makedirs(ext_nr_unknown_dir, exist_ok=True)

    os.makedirs(ext_gu_ssbpost_dir, exist_ok=True)
    os.makedirs(ext_gu_unknown_dir, exist_ok=True)

    os.makedirs(tp_gnb_ssbpost_dir, exist_ok=True)
    os.makedirs(tp_gnb_unknown_dir, exist_ok=True)

    os.makedirs(tp_gnodeb_ssbpost_dir, exist_ok=True)
    os.makedirs(tp_gnodeb_unknown_dir, exist_ok=True)

    generated = 0

    # -----------------------------
    # ExternalNRCellCU - SSB-Post / Unknown
    # - Export to two subfolders inside ExternalNRCellCU
    # - If a NodeId has both targets, grouping is done within each filtered subset
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="ExternalNRCellCU",
        output_dir=ext_nr_ssbpost_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["SSB-Post"],
        filename_suffix="ExternalNRCellCU",
    )
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="ExternalNRCellCU",
        output_dir=ext_nr_unknown_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["Unknown", "Unkwnow"],
        filename_suffix="ExternalNRCellCU",
    )

    # -----------------------------
    # ExternalGUtranCell - SSB-Post / Unknown
    # - Export to two subfolders inside ExternalGUtranCell
    # - If a NodeId has both targets, grouping is done within each filtered subset
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="ExternalGUtranCell",
        output_dir=ext_gu_ssbpost_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["SSB-Post"],
        filename_suffix="ExternalGUtranCell",
    )
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="ExternalGUtranCell",
        output_dir=ext_gu_unknown_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["Unknown", "Unkwnow"],
        filename_suffix="ExternalGUtranCell",
    )

    # -----------------------------
    # TermPointToGNodeB - SSB-Post / Unknown  (Bullets 2 & 3 from new requirements slide)
    # - Bullet 2: export to two subfolders
    # - Bullet 3: if a NodeId has both targets, grouping is done within each filtered subset
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="TermPointToGNodeB",
        output_dir=tp_gnodeb_ssbpost_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["SSB-Post"],
        filename_suffix="TermPointToGNodeB",
    )
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="TermPointToGNodeB",
        output_dir=tp_gnodeb_unknown_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["Unknown", "Unkwnow"],
        filename_suffix="TermPointToGNodeB",
    )

    # -----------------------------
    # TermPointToGNB - SSBPost / Unknown (same behavior as ExternalGUtranCell)
    # - Export to two subfolders inside TermPointToGNB
    # - If a NodeId has both targets, grouping is done within each filtered subset
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="TermPointToGNB",
        output_dir=tp_gnb_ssbpost_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["SSB-Post"],
        filename_suffix="TermPointToGNB")
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="TermPointToGNB",
        output_dir=tp_gnb_unknown_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["Unknown", "Unkwnow"],
        filename_suffix="TermPointToGNB")

    if generated:
        print(f"[Correction Commands] Generated {generated} extra Correction_Cmd files from POST Configuration Audit in: '{pretty_path(base_dir)}'")

    return generated
