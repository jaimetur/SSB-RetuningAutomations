# -*- coding: utf-8 -*-

"""
Helpers to export Correction_Cmd  to text files.
"""

from __future__ import annotations

import os
import pandas as pd
from typing import Dict, Optional

from src.utils.utils_io import to_long_path, pretty_path
from src.utils.utils_parsing import merge_command_blocks_for_node


# ----------------------------------------------------------------------
#  EXPORT CORRECTION COMMNANDS TO TEXT FILES
# ----------------------------------------------------------------------
def export_correction_cmd_texts(output_dir: str, dfs_by_category: Dict[str, pd.DataFrame]) -> int:
    """
    Export Correction_Cmd values to text files grouped by NodeId and category.

    For each category (e.g. GU_missing, NR_new), one file per NodeId is created in:
      <output_dir>/Correction_Cmd/<GroupFolder>/<NR|GU>/<NodeId>_<Category>.txt
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

    base_dir = os.path.join(output_dir, "Correction_Cmd")
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

            cmds = [cmd for cmd in group["Correction_Cmd"] if cmd.strip()]
            if not cmds:
                continue

            file_name = f"{node_str}_{category}.txt"
            file_path = os.path.join(target_dir, file_name)
            file_path_long = to_long_path(file_path)

            with open(file_path_long, "w", encoding="utf-8") as f:
                f.write("\n\n".join(cmds))

            total_files += 1

    print(f"\n[Consistency Checks (Pre/Post Comparison)] Generated {total_files} Correction_Cmd files in: '{pretty_path(base_dir)}'")
    return total_files


# ----------------------------- EXTERNAL/TERMPOINTS COMMANDS ----------------------------- #
def export_external_and_termpoint_commands(
    audit_post_excel: str,
    output_dir: str,
) -> int:
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
      <output_dir>/Correction_Cmd/TermPointToGNodeB

    One text file per NodeId is generated (grouped like other Correction_Cmd exports).

    Returns the number of generated files.
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

            file_name = f"{node_str}_{suffix}.txt"
            out_path = os.path.join(output_dir, file_name)
            out_path_long = to_long_path(out_path)

            merged_script = merge_command_blocks_for_node(cmds)
            if not merged_script.strip():
                continue

            with open(out_path_long, "w", encoding="utf-8") as f:
                f.write(merged_script)

            generated_files += 1

        return generated_files

    if not audit_post_excel or not os.path.isfile(audit_post_excel):
        return 0

    base_dir = os.path.join(output_dir, "Correction_Cmd")

    ext_nr_base = os.path.join(base_dir, "ExternalNRCellCU")
    ext_gu_base = os.path.join(base_dir, "ExternalGUtranCell")
    tp_gnb_dir = os.path.join(base_dir, "TermPointToGNB")
    tp_gnodeb_dir = os.path.join(base_dir, "TermPointToGNodeB")

    ext_nr_ssbpost_dir = os.path.join(ext_nr_base, "SSB-Post")
    ext_nr_unknown_dir = os.path.join(ext_nr_base, "Unknown")
    ext_gu_ssbpost_dir = os.path.join(ext_gu_base, "SSB-Post")
    ext_gu_unknown_dir = os.path.join(ext_gu_base, "Unknown")

    os.makedirs(ext_nr_ssbpost_dir, exist_ok=True)
    os.makedirs(ext_nr_unknown_dir, exist_ok=True)
    os.makedirs(ext_gu_ssbpost_dir, exist_ok=True)
    os.makedirs(ext_gu_unknown_dir, exist_ok=True)
    os.makedirs(tp_gnb_dir, exist_ok=True)
    os.makedirs(tp_gnodeb_dir, exist_ok=True)

    generated = 0

    # -----------------------------
    # ExternalNRCellCU - SSB-Post / Unknown
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
    # TermPointToGNodeB
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="TermPointToGNodeB",
        output_dir=tp_gnodeb_dir,
        command_column="Correction_Cmd",
        filename_suffix="TermPointToGNodeB",
    )

    # -----------------------------
    # TermPointToGNB
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="TermPointToGNB",
        output_dir=tp_gnb_dir,
        command_column="Correction_Cmd",
        filename_suffix="TermPointToGNB",
    )

    if generated:
        print(f"[Consistency Checks (Pre/Post Comparison)] Generated {generated} extra Correction_Cmd files from POST Configuration Audit in: '{pretty_path(base_dir)}'")

    return generated
