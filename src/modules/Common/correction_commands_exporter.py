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

def _safe_filename_component(value: object, fallback: str = "item", max_len: int = 120) -> str:
    """
    Convert any value into a Windows-safe filename component:
      - remove invalid characters <>:"/\\|?*
      - remove ASCII control chars
      - strip trailing dots/spaces
      - avoid reserved device names (CON, PRN, AUX, NUL, COM1.., LPT1..)
    """
    s = str(value) if value is not None else ""
    s = s.strip()

    if not s:
        s = fallback

    invalid = '<>:"/\\|?*'
    s = "".join("_" if (ch in invalid or ord(ch) < 32) else ch for ch in s)
    s = s.rstrip(" .")

    reserved = {
        "con", "prn", "aux", "nul",
        "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
        "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
    }
    if s.lower() in reserved:
        s = f"_{s}_"

    if len(s) > max_len:
        s = s[:max_len].rstrip(" .")

    return s or fallback


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
def export_relations_commands(output_dir: str, dfs_by_category: Dict[str, pd.DataFrame], base_folder_name: str = "Correction_Cmd", export_to_zip: bool = True, module_name: str = "", market_tag: str = "GLOBAL") -> int:
    """
    Export Correction_Cmd values to text files grouped by NodeId and category.

    For each category (e.g. GU_missing, NR_new), one file per NodeId is created under:
      <output_dir>/<base_folder_name>/(NewRelations|MissingRelations|RelationsDiscrepancies)/(NR|GU)/<NodeId>_<Category>.txt

    By default, outputs are written into a ZIP (same behavior as ConfigurationAudit exporters).
    """
    import zipfile

    def _detect_layer_from_category(category: str) -> str:
        s = str(category or "").strip().upper()
        if s.startswith("NR"):
            return "NR"
        if s.startswith("GU"):
            return "GU"
        if "NR_" in s or "_NR" in s or "NR-" in s:
            return "NR"
        if "GU_" in s or "_GU" in s or "GU-" in s:
            return "GU"
        return ""

    base_dir = os.path.join(output_dir, base_folder_name)
    os.makedirs(base_dir, exist_ok=True)

    zip_file = None
    zip_path = None

    try:
        if export_to_zip:
            zip_path = os.path.join(base_dir, "Relations_CorrectionCmds.zip")
            zip_path_long = to_long_path(zip_path)
            zip_file = zipfile.ZipFile(zip_path_long, "w", compression=zipfile.ZIP_DEFLATED)

        def _write_text(target_dir: str, file_name: str, text: str) -> None:
            if export_to_zip and zip_file is not None:
                target_dir_clean = str(target_dir).replace("\\", "/").strip("/")
                arcname = f"{target_dir_clean}/{file_name}" if target_dir_clean else f"{file_name}"
                zip_file.writestr(arcname.replace("\\", "/"), text)
                return

            disk_dir = os.path.join(base_dir, target_dir)
            os.makedirs(disk_dir, exist_ok=True)
            file_path = os.path.join(disk_dir, file_name)
            file_path_long = to_long_path(file_path)
            with open(file_path_long, "w", encoding="utf-8") as f:
                f.write(text)

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

            if "new" in category_lower:
                group_folder = "NewRelations"
            elif "missing" in category_lower:
                group_folder = "MissingRelations"
            elif "disc" in category_lower:
                group_folder = "RelationsDiscrepancies"
            else:
                group_folder = ""

            layer = _detect_layer_from_category(category)
            if group_folder and layer in ("NR", "GU"):
                rel_dir = f"{group_folder}/{layer}"
            elif group_folder:
                rel_dir = f"{group_folder}"
            else:
                rel_dir = ""

            for node_id, group in work.groupby("NodeId"):
                node_str = str(node_id).strip()
                if not node_str:
                    continue

                raw_cmds = [cmd for cmd in group["Correction_Cmd"] if str(cmd).strip()]
                if not raw_cmds:
                    continue

                ordered_blocks = _reorder_cmds_del_first(raw_cmds)
                if not ordered_blocks:
                    continue

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

                node_str_safe = _safe_filename_component(node_str, fallback="node")
                cat_safe = _safe_filename_component(category, fallback="cmd")
                file_name = f"{node_str_safe}_{cat_safe}.txt"

                target_dir = rel_dir if rel_dir else ""
                _write_text(target_dir, file_name, final_text)
                total_files += 1

        if total_files > 0:
            if export_to_zip and zip_file is not None:
                print(f"{module_name} {market_tag} [INFO] Generated {total_files} Correction Commands files in ZIP: '{pretty_path(zip_path or '')}'")
            else:
                print(f"{module_name} {market_tag} [INFO] Generated {total_files} Correction Commands files in: '{pretty_path(base_dir)}'")
        else:
            print(f"\n{module_name} {market_tag} [INFO] No relations Correction Commands files generated.")

        return total_files

    finally:
        try:
            if zip_file is not None:
                zip_file.close()
        except Exception:
            pass



# ----------------------------- EXTERNAL/TERMPOINTS COMMANDS ----------------------------- #
def export_external_and_termpoint_commands(audit_post_excel: str, output_dir: str, base_folder_name: str = "Correction_Cmd", sheet_dfs: Optional[dict[str, pd.DataFrame]] = None, export_to_zip: bool = True, module_name: str = "") -> int:
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

    base_dir = os.path.join(output_dir, base_folder_name)

    ext_nr_base = os.path.join(base_dir, "ExternalNRCellCU")
    ext_nr_ssbpost_dir = os.path.join(ext_nr_base, "SSB-Post")
    ext_nr_unknown_dir = os.path.join(ext_nr_base, "Unknown")

    ext_gu_base = os.path.join(base_dir, "ExternalGUtranCell")
    ext_gu_ssbpost_dir = os.path.join(ext_gu_base, "SSB-Post")
    ext_gu_unknown_dir = os.path.join(ext_gu_base, "Unknown")

    tp_gnb_dir = os.path.join(base_dir, "TermPointToGNB")
    tp_gnb_ssbpost_dir = os.path.join(tp_gnb_dir, "SSB-Post")
    tp_gnb_unknown_dir = os.path.join(tp_gnb_dir, "Unknown")

    tp_gnodeb_base = os.path.join(base_dir, "TermPointToGNodeB")
    tp_gnodeb_ssbpost_dir = os.path.join(tp_gnodeb_base, "SSB-Post")
    tp_gnodeb_unknown_dir = os.path.join(tp_gnodeb_base, "Unknown")

    zip_file = None
    zip_path = None

    try:
        # If we export to zip, create it inside the base_dir and avoid duplicating base folder inside the zip
        if export_to_zip:
            import zipfile
            os.makedirs(base_dir, exist_ok=True)
            zip_path = to_long_path(os.path.join(base_dir, "CellRelation_Externals_Termpoints.zip"))
            zip_file = zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED)

        # If we export to filesystem, ensure directories exist
        if not export_to_zip:
            os.makedirs(ext_nr_ssbpost_dir, exist_ok=True)
            os.makedirs(ext_nr_unknown_dir, exist_ok=True)
            os.makedirs(ext_gu_ssbpost_dir, exist_ok=True)
            os.makedirs(ext_gu_unknown_dir, exist_ok=True)
            os.makedirs(tp_gnb_ssbpost_dir, exist_ok=True)
            os.makedirs(tp_gnb_unknown_dir, exist_ok=True)
            os.makedirs(tp_gnodeb_ssbpost_dir, exist_ok=True)
            os.makedirs(tp_gnodeb_unknown_dir, exist_ok=True)

        # Prepare cached Excel reader only if needed (no in-memory DFs provided)
        xl_cached = None
        sheet_map_cached = {}
        if not (isinstance(sheet_dfs, dict) and sheet_dfs):
            try:
                xl_cached = pd.ExcelFile(audit_post_excel) if audit_post_excel and os.path.isfile(audit_post_excel) else None
                sheet_map_cached = {s.lower(): s for s in (xl_cached.sheet_names if xl_cached else [])}
            except Exception:
                xl_cached = None
                sheet_map_cached = {}

        def _read_sheet_case_insensitive(audit_excel: str, sheet_name: str) -> Optional[pd.DataFrame]:
            """
            Read a sheet using case-insensitive matching. Returns None if not found or on error.
            Uses a cached ExcelFile to avoid reopening/parsing the XLSX multiple times (major speedup).
            """
            if isinstance(sheet_dfs, dict) and sheet_dfs:
                for k, v in sheet_dfs.items():
                    if str(k).strip().lower() == str(sheet_name).strip().lower():
                        return v
                return None

            if xl_cached is None:
                return None

            try:
                real_sheet = sheet_map_cached.get(str(sheet_name).lower())
                if not real_sheet:
                    return None
                return xl_cached.parse(real_sheet)
            except Exception:
                return None

        def _write_node_output(target_dir: str, file_name: str, text: str) -> None:
            if export_to_zip and zip_file is not None:
                arcname = f"{target_dir}/{file_name}"
                zip_file.writestr(arcname, text + "\n")
                return
            out_path = to_long_path(os.path.join(base_dir, target_dir, file_name))
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(text)

        def _export_grouped_commands_from_sheet(audit_excel: str, sheet_name: str, output_dir: str, command_column: str = "Correction_Cmd", node_column: str = "NodeId", filter_column: Optional[str] = None, filter_values: Optional[list[str]] = None, filename_suffix: Optional[str] = None, hoist_header_lines: int = 3, hoist_footer_lines: int = 1, force_file_header: Optional[list[str]] = None, force_file_footer: Optional[list[str]] = None, strip_file_wrappers: bool = False) -> int:
            """
            Export Correction_Cmd grouped by NodeId from a given sheet into output_dir.
            Returns how many files were generated.

            Behavior:
              - 'del ...' lines are moved to the top of each node file.
              - For External* and TermPoint* blocks, we keep per-block order exactly as in Excel,
                but hoist ONLY the first N header lines and the last M footer lines once per node file (configurable per export call).
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

            work = df.copy()
            work[node_column] = work[node_column].astype(str).str.strip()

            suffix = filename_suffix if filename_suffix else str(sheet_name).strip()
            generated_files = 0

            # If exporting to zip, compute relative dir inside zip (no base folder duplication)
            rel_dir = os.path.relpath(output_dir, base_dir).replace("\\", "/")

            for node_id, group in work.groupby(node_column):
                node_str = str(node_id).strip()
                if not node_str:
                    continue

                # IMPORTANT: do NOT cast the whole column to str before dropna(), otherwise NaN becomes "nan"
                raw_series = group[command_column]
                raw_series = raw_series[raw_series.notna()]

                cmds = raw_series.astype(str).map(str.strip).loc[lambda s: (s != "") & (s.str.lower() != "nan") & (s.str.lower() != "none")].tolist()
                if not cmds:
                    continue

                if strip_file_wrappers:
                    def _strip_wrappers_block(s: str) -> str:
                        wrappers = {"confb+", "gs+", "lt all", "alt"}
                        block = str(s).replace("\r\n", "\n").replace("\r", "\n").strip("\n")
                        lines = [ln.rstrip() for ln in block.split("\n") if ln.strip() != ""]
                        while lines and lines[0].strip().lower() in wrappers:
                            lines.pop(0)
                        while lines and lines[-1].strip().lower() == "alt":
                            lines.pop()
                        return "\n".join(lines).strip()

                    cmds = [_strip_wrappers_block(c) for c in cmds]
                    cmds = [c for c in cmds if str(c).strip()]

                # del first (and keep rest blocks after)
                ordered_blocks = _reorder_cmds_del_first(cmds)
                if not ordered_blocks:
                    continue

                del_lines = [b for b in ordered_blocks if _DEL_LINE_RE.match(b)]
                rest_blocks = [b for b in ordered_blocks if not _DEL_LINE_RE.match(b)]

                # NEW: HOIST only the first 3 lines and the last line ONCE per node
                merged_rest = _merge_blocks_hoist_header_footer(rest_blocks, header_lines=hoist_header_lines, footer_lines=hoist_footer_lines) if (rest_blocks and (hoist_header_lines or hoist_footer_lines)) else ("\n\n".join([str(b).strip() for b in (rest_blocks or []) if str(b).strip()]).strip())

                pieces: List[str] = []
                if force_file_header:
                    pieces.append("\n".join([str(x).strip() for x in force_file_header if str(x).strip()]).strip())
                if del_lines:
                    pieces.append("\n".join(del_lines).strip())
                if str(merged_rest).strip():
                    pieces.append(str(merged_rest).strip())
                if force_file_footer:
                    pieces.append("\n".join([str(x).strip() for x in force_file_footer if str(x).strip()]).strip())

                merged_script = "\n\n".join([p for p in pieces if str(p).strip()]).strip()
                if not merged_script:
                    continue

                node_str_safe = _safe_filename_component(node_str, fallback="node")
                suffix_safe = _safe_filename_component(suffix, fallback="cmd")
                file_name = f"{node_str_safe}_{suffix_safe}.txt"

                _write_node_output(rel_dir, file_name, merged_script)
                generated_files += 1

            return generated_files

        if not (isinstance(sheet_dfs, dict) and sheet_dfs) and (not audit_post_excel or not os.path.isfile(audit_post_excel)):
            return 0

        total_files = 0

        # -----------------------------
        # ExternalNRCellCU - SSB-Post / Unknown
        # - Export to two subfolders inside ExternalNRCellCU
        # - If a NodeId has both targets, grouping is done within each filtered subset
        # -----------------------------
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="ExternalNRCellCU", output_dir=ext_nr_ssbpost_dir, command_column="Correction_Cmd", filter_column="GNodeB_SSB_Target", filter_values=["SSB-Post"], filename_suffix="ExternalNRCellCU", hoist_header_lines=4, hoist_footer_lines=1)
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="ExternalNRCellCU", output_dir=ext_nr_unknown_dir, command_column="Correction_Cmd", filter_column="GNodeB_SSB_Target", filter_values=["Unknown", "Unkwnow"], filename_suffix="ExternalNRCellCU", hoist_header_lines=4, hoist_footer_lines=1)

        # -----------------------------
        # ExternalGUtranCell - SSB-Post / Unknown
        # - Export to two subfolders inside ExternalGUtranCell
        # - If a NodeId has both targets, grouping is done within each filtered subset
        # -----------------------------
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="ExternalGUtranCell", output_dir=ext_gu_ssbpost_dir, command_column="Correction_Cmd", filter_column="GNodeB_SSB_Target", filter_values=["SSB-Post"], filename_suffix="ExternalGUtranCell", hoist_header_lines=0, hoist_footer_lines=0, force_file_header=["confb+", "gs+", "lt all", "alt"], force_file_footer=["alt"], strip_file_wrappers=True)
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="ExternalGUtranCell", output_dir=ext_gu_unknown_dir, command_column="Correction_Cmd", filter_column="GNodeB_SSB_Target", filter_values=["Unknown", "Unkwnow"], filename_suffix="ExternalGUtranCell", hoist_header_lines=0, hoist_footer_lines=0, force_file_header=["confb+", "gs+", "lt all", "alt"], force_file_footer=["alt"], strip_file_wrappers=True)

        # -----------------------------
        # TermPointToGNodeB - SSB-Post / Unknown  (Bullets 2 & 3 from new requirements slide)
        # - Bullet 2: export to two subfolders
        # - Bullet 3: if a NodeId has both targets, grouping is done within each filtered subset
        # -----------------------------
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="TermPointToGNodeB", output_dir=tp_gnodeb_ssbpost_dir, command_column="Correction_Cmd", filter_column="GNodeB_SSB_Target", filter_values=["SSB-Post"], filename_suffix="TermPointToGNodeB")
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="TermPointToGNodeB", output_dir=tp_gnodeb_unknown_dir, command_column="Correction_Cmd", filter_column="GNodeB_SSB_Target", filter_values=["Unknown", "Unkwnow"], filename_suffix="TermPointToGNodeB")

        # -----------------------------
        # TermPointToGNB - SSBPost / Unknown (same behavior as ExternalGUtranCell)
        # - Export to two subfolders inside TermPointToGNB
        # - If a NodeId has both targets, grouping is done within each filtered subset
        # -----------------------------
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="TermPointToGNB", output_dir=tp_gnb_ssbpost_dir, command_column="Correction_Cmd", filter_column="GNodeB_SSB_Target", filter_values=["SSB-Post"], filename_suffix="TermPointToGNB")
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="TermPointToGNB", output_dir=tp_gnb_unknown_dir, command_column="Correction_Cmd", filter_column="GNodeB_SSB_Target", filter_values=["Unknown", "Unkwnow"], filename_suffix="TermPointToGNB")

        # -----------------------------
        # NRCellRelation / GUtranCellRelation
        # - Export into the same ZIP/folder as Externals & TermPoints (NOT Other_MOs.zip)
        # - Add confb+/gs+/lt all/alt once per file at the top, and a final alt once per file at the bottom
        # -----------------------------
        nr_rel_dir = os.path.join(base_dir, "NRCellRelation")
        gu_rel_dir = os.path.join(base_dir, "GUtranCellRelation")

        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="NRCellRelation", output_dir=nr_rel_dir, command_column="Correction_Cmd", filename_suffix="NRCellRelation", hoist_header_lines=0, hoist_footer_lines=0, force_file_header=["confb+", "gs+", "lt all", "alt"], force_file_footer=["alt"], strip_file_wrappers=True)
        total_files += _export_grouped_commands_from_sheet(audit_excel=audit_post_excel, sheet_name="GUtranCellRelation", output_dir=gu_rel_dir, command_column="Correction_Cmd", filename_suffix="GUtranCellRelation", hoist_header_lines=0, hoist_footer_lines=0, force_file_header=["confb+", "gs+", "lt all", "alt"], force_file_footer=["alt"], strip_file_wrappers=True)

        if total_files >0:
            if export_to_zip and zip_file is not None:
                print(f"{module_name} [INFO] Generated {total_files} Termpoints/Externals Correction Commands files from Configuration Audit in ZIP: '{pretty_path(zip_path or '')}'")
            else:
                print(f"{module_name} [INFO] Generated {total_files} Termpoints/Externals Correction Commands files from Configuration Audit in: '{pretty_path(base_dir)}'")
        else:
            print(f"{module_name} [INFO] No Termpoints/Externals Correction Commands files generated from Configuration Audit.")

        return total_files

    finally:
        try:
            if zip_file is not None:
                zip_file.close()
        except Exception:
            pass



def export_all_sheets_with_correction_commands(audit_post_excel: str, output_dir: str, base_folder_name: str = "Correction_Cmd", exclude_sheets: Optional[set[str]] = None, sheet_dfs: Optional[dict[str, pd.DataFrame]] = None, export_to_zip: bool = True, module_name: str = "") -> int:
    """
    Export Correction_Cmd values from ANY sheet containing a 'Correction_Cmd' column.
    Intended for ConfigurationAudit (NRCellRelation, GUtranCellRelation, etc.).

    Folder layout (export_to_zip=False):
      <output_dir>/Correction_Cmd/<SheetName>/<NodeId>_<SheetName>.txt

    ZIP layout (export_to_zip=True):
      <output_dir>/Correction_Cmd/Correction_Cmd.zip
      Entries:
        <SheetName>/<NodeId>_<SheetName>.txt

    Optimization:
      - If sheet_dfs is provided, use those DataFrames directly (no Excel re-read).
      - If sheet_dfs is None, fallback to reading from Excel (legacy behavior).
      - ZIP output reduces filesystem latency (especially on synced drives like Google Drive).
    """
    exclude = {s.strip().lower() for s in (exclude_sheets or set())}

    base_dir = os.path.join(output_dir, base_folder_name)
    os.makedirs(base_dir, exist_ok=True)

    total_files = 0
    zip_path = None
    zip_file = None

    # Determine sheet sources
    dfs_source: dict[str, pd.DataFrame] = {}
    if isinstance(sheet_dfs, dict) and sheet_dfs:
        dfs_source = sheet_dfs
    else:
        if not audit_post_excel or not os.path.isfile(audit_post_excel):
            return 0
        try:
            xl = pd.ExcelFile(audit_post_excel)
            for sheet in list(xl.sheet_names):
                try:
                    df = xl.parse(sheet)
                except Exception:
                    continue
                if df is None or df.empty:
                    continue
                dfs_source[str(sheet)] = df
        except Exception:
            return 0

    try:
        if export_to_zip:
            import zipfile
            zip_path = to_long_path(os.path.join(base_dir, "Other_MOs.zip"))
            zip_file = zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED)

        for sheet, df in dfs_source.items():
            if str(sheet).strip().lower() in exclude:
                continue

            if df is None or df.empty:
                continue
            if "Correction_Cmd" not in df.columns or "NodeId" not in df.columns:
                continue

            work = df.copy()
            work["NodeId"] = work["NodeId"].astype(str).str.strip()

            raw_series = work["Correction_Cmd"]
            raw_series = raw_series[raw_series.notna()]
            if raw_series.empty:
                continue

            sheet_name = str(sheet).strip()
            sheet_dir = os.path.join(base_dir, sheet_name)
            if not export_to_zip:
                os.makedirs(sheet_dir, exist_ok=True)

            # Group and export
            for node_id, group in work.groupby("NodeId"):
                node_str = str(node_id).strip()
                if not node_str:
                    continue

                raw_cmds = [cmd for cmd in group["Correction_Cmd"] if str(cmd).strip()]
                if not raw_cmds:
                    continue

                ordered_blocks = _reorder_cmds_del_first(raw_cmds)
                if not ordered_blocks:
                    continue

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

                node_str_safe = _safe_filename_component(node_str, fallback="node")
                sheet_safe = _safe_filename_component(sheet_name, fallback="sheet")
                file_name = f"{node_str_safe}_{sheet_safe}.txt"

                if export_to_zip and zip_file is not None:
                    arcname = f"{sheet_name}/{file_name}"
                    zip_file.writestr(arcname, final_text + "\n")
                else:
                    file_path = to_long_path(os.path.join(sheet_dir, file_name))
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(final_text)

                total_files += 1

        if total_files>0:
            if export_to_zip and zip_path:
                print(f"{module_name} [INFO] Generated {total_files} Other_MOs (sheet-based) Correction Commands files from Configuration Audit in ZIP: '{pretty_path(zip_path)}'")
            else:
                print(f"{module_name} [INFO] Generated {total_files} Other_MOs (sheet-based) Correction Commands files from Configuration Audit in: '{pretty_path(base_dir)}'")
        else:
            print(f"{module_name} [INFO] No Other_MOs (sheet-based) Correction Commands files generated from Configuration Audit.")

        return total_files

    finally:
        try:
            if zip_file is not None:
                zip_file.close()
        except Exception:
            pass
