from __future__ import annotations

import os
import re
from typing import Optional

import pandas as pd

from src.utils.utils_io import to_long_path

# ----------------------------- LOAD NODES FROM SUMMARY EXCEL ----------------------------- #
def load_nodes_names_and_id_from_summary_audit(audit_excel: Optional[object], stage: Optional[str] = "Pre", module_name: Optional[str] = "") -> set[str]:
    """
    Read SummaryAudit sheet from POST Configuration Audit and extract node numeric identifiers
    for nodes that have not completed the retuning.

    It looks for rows with:
      - SubCategory == 'NRCellDU'
      - Metric containing 'NR nodes with N77 SSB in Pre-Retune allowed list'
    and then parses the ExtraInfo field assuming it contains a comma-separated list of node names.

    From each node name it extracts the leading numeric identifier (digits at the beginning).
    Additionally, it prints the full node names that are being considered as "no retuning" nodes.
    """
    nodes: set[str] = set()
    if audit_excel is None:
        return nodes

    df: Optional[pd.DataFrame] = None

    # --- Case 1: audit_excel is already a DataFrame ---
    if isinstance(audit_excel, pd.DataFrame):
        df = audit_excel.copy()

    # --- Case 2: audit_excel is a list of rows (generated via add_row) ---
    elif isinstance(audit_excel, list):
        if not audit_excel:
            return nodes
        try:
            df = pd.DataFrame(audit_excel)
        except Exception as e:
            print(f"{module_name} [WARNING] Could not convert list to DataFrame: {e}. Skipping node exclusion based on SummaryAudit.")
            return nodes

    # --- Case 3: audit_excel is a path or string ---
    elif isinstance(audit_excel, str):
        try:
            audit_path = to_long_path(audit_excel)
        except Exception:
            audit_path = audit_excel

        if not os.path.isfile(audit_path):
            print(f"{module_name} [WARNING] POST audit Excel not found: '{audit_excel}'. Skipping node exclusion based on SummaryAudit.")
            return nodes

        try:
            df = pd.read_excel(audit_path, sheet_name="SummaryAudit")
        except Exception as e:
            print(f"{module_name} [WARNING] Could not read 'SummaryAudit' sheet from POST audit Excel: {e}. Skipping node exclusion based on SummaryAudit.")
            return nodes

    # --- Unsupported type ---
    else:
        print(f"{module_name} [WARNING] Unsupported type for audit_excel: {type(audit_excel)}. Expected DataFrame, list, or path string.")
        return nodes

    required_cols = {"Category", "SubCategory", "Metric", "ExtraInfo"}
    if not required_cols.issubset(df.columns):
        print(f"{module_name} [WARNING] 'SummaryAudit' data does not contain required columns {required_cols}. Skipping node exclusion based on SummaryAudit.")
        return nodes

    sub = df.copy()
    sub["Category"] = sub["Category"].astype(str).str.strip()
    sub["Metric"] = sub["Metric"].astype(str)
    sub["ExtraInfo"] = sub["ExtraInfo"].astype(str)

    mask = (sub["Category"] == "NRCellDU") & sub["Metric"].str.contains(f"NR nodes with N77 SSB in {stage}-Retune allowed list", case=False, na=False)
    rows = sub.loc[mask]
    if rows.empty:
        return nodes

    pattern_id = re.compile(r"^\s*(\d+)")
    full_node_names: set[str] = set()  # NEW: keep full node names (as they appear in ExtraInfo)

    for extra in rows["ExtraInfo"]:
        if not extra:
            continue
        parts = [p.strip() for p in str(extra).split(",") if p.strip()]
        for token in parts:
            m = pattern_id.match(token)
            if m:
                # Store numeric identifier
                nodes.add(m.group(1))
                # Store full node name as destination of the relation being discarded
                full_node_names.add(token.strip())

    print(f"{module_name} [INFO] Nodes with {stage}-SSB (complete node names): {sorted(full_node_names)}")
    print(f"{module_name} [INFO] Nodes with {stage}-SSB (numeric identifiers): {sorted(nodes)}")

    # if full_node_names:
    #     print(f"{module_name} [INFO] Nodes with {stage}-SSB (destination of relations to be skipped from Discrepancies): {sorted(full_node_names)}")
    # if nodes:
    #     print(f"{module_name} [INFO] Nodes with {stage}-SSB (numeric identifiers): {sorted(nodes)}")

    return nodes
