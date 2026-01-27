from __future__ import annotations

import os
import re
from typing import Optional

import pandas as pd

from src.utils.utils_io import to_long_path

# ----------------------------- LOAD NODES FROM SUMMARY EXCEL ----------------------------- #
def load_nodes_names_and_id_from_summary_audit(
    audit_excel: Optional[object],
    stage: Optional[str] = "Pre",
    print_lists: Optional[bool] = False,
    module_name: Optional[str] = "",
) -> tuple[set[str], set[str]]:
    """
    Read a ConfigurationAudit SummaryAudit dataset (from a DataFrame, list of rows, or an Excel path)
    and extract:
      - nodes_id: numeric identifiers (leading digits of the node name)
      - nodes_names: full node names as they appear in ExtraInfo.

    The function looks for SummaryAudit rows matching:
      - Category == 'NRCellDU'
      - Metric contains: 'NR nodes with N77 SSB in <stage>-Retune allowed list'

    Then it parses the 'ExtraInfo' field assuming it contains a comma-separated list of node names.

    For each node name token, it:
      - stores the full token in nodes_names
      - extracts the leading numeric identifier (digits at the beginning) and stores it in nodes_id

    The function also prints the detected full node names and numeric identifiers for visibility.

    Parameters:
      audit_excel:
        - pd.DataFrame: already loaded SummaryAudit-like table
        - list: list of rows/dicts convertible to DataFrame (e.g. generated via add_row)
        - str: path to an Excel file containing a 'SummaryAudit' sheet
      stage:
        label used to match the Metric string (e.g. 'Pre' or 'Post')
      module_name:
        optional prefix used in log/print messages
    """

    nodes_id: set[str] = set()
    nodes_names: set[str] = set()

    if audit_excel is None:
        return nodes_id, nodes_names

    df: Optional[pd.DataFrame] = None

    # --- Case 1: audit_excel is already a DataFrame ---
    if isinstance(audit_excel, pd.DataFrame):
        df = audit_excel.copy()

    # --- Case 2: audit_excel is a list of rows (generated via add_row) ---
    elif isinstance(audit_excel, list):
        if not audit_excel:
            return nodes_id, nodes_names
        try:
            df = pd.DataFrame(audit_excel)
        except Exception as e:
            print(f"{module_name} [WARNING] Could not convert list to DataFrame: {e}. Skipping node exclusion based on SummaryAudit.")
            return nodes_id, nodes_names

    # --- Case 3: audit_excel is a path or string ---
    elif isinstance(audit_excel, str):
        try:
            audit_path = to_long_path(audit_excel)
        except Exception:
            audit_path = audit_excel

        if not os.path.isfile(audit_path):
            print(f"{module_name} [WARNING] POST audit Excel not found: '{audit_excel}'. Skipping node exclusion based on SummaryAudit.")
            return nodes_id, nodes_names

        try:
            df = pd.read_excel(audit_path, sheet_name="SummaryAudit")
        except Exception as e:
            print(f"{module_name} [WARNING] Could not read 'SummaryAudit' sheet from POST audit Excel: {e}. Skipping node exclusion based on SummaryAudit.")
            return nodes_id, nodes_names

    # --- Unsupported type ---
    else:
        print(f"{module_name} [WARNING] Unsupported type for audit_excel: {type(audit_excel)}. Expected DataFrame, list, or path string.")
        return nodes_id, nodes_names

    required_cols = {"Category", "SubCategory", "Metric", "ExtraInfo"}
    if not required_cols.issubset(df.columns):
        print(f"{module_name} [WARNING] 'SummaryAudit' data does not contain required columns {required_cols}. Skipping node exclusion based on SummaryAudit.")
        return nodes_id, nodes_names

    sub = df.copy()
    sub["Category"] = sub["Category"].astype(str).str.strip()
    sub["Metric"] = sub["Metric"].astype(str)
    sub["ExtraInfo"] = sub["ExtraInfo"].astype(str)

    mask = (sub["Category"] == "NRCellDU") & sub["Metric"].str.contains(
        f"NR nodes with N77 SSB in {stage}-Retune allowed list",
        case=False,
        na=False,
    )
    rows = sub.loc[mask]
    if rows.empty:
        return nodes_id, nodes_names

    pattern_id = re.compile(r"^\s*(\d+)")

    for extra in rows["ExtraInfo"]:
        if not extra:
            continue
        parts = [p.strip() for p in str(extra).split(",") if p.strip()]
        for token in parts:
            nodes_names.add(token.strip())
            m = pattern_id.match(token)
            if m:
                nodes_id.add(m.group(1))

    if print_lists:
        print(f"{module_name} [INFO] Nodes with {stage}-SSB (complete node names): {sorted(nodes_names)}")
        print(f"{module_name} [INFO] Nodes with {stage}-SSB (numeric identifiers): {sorted(nodes_id)}")

    return nodes_id, nodes_names

