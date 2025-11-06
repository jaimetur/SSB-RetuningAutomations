# -*- coding: utf-8 -*-

import os
import re
from typing import List, Tuple, Optional, Dict

import pandas as pd


class CreateExcelFromLogs:
    """
    Genera un Excel en input_dir con una pestaña por cada fichero *.log / *.logs.

    Robustez:
      - Intenta múltiples codificaciones: utf-8-sig, utf-16, utf-16-le, utf-16-be, cp1252.
      - Delimitador preferente: TAB. Solo si no hay tabs en ninguna línea, intenta coma.
      - Si tampoco hay coma, separa por espacios.
      - Elimina filas totalmente vacías y líneas "N instance(s)".
      - Añade hoja 'Summary'.
      - Limita filas a ~1,048,576 (límite de Excel).

    Nombre de salida: LogsToExcel_YYYYmmdd-HHMMSS[_VERSION].xlsx
    """

    SUMMARY_RE = re.compile(r"^\s*\d+\s+instance\(s\)\s*$", re.IGNORECASE)

    def __init__(self):
        pass

    # ----------------------------- API pública ----------------------------- #
    def run(self, input_dir: str, module_name: Optional[str] = "", versioned_suffix: Optional[str] = None) -> str:
        if not os.path.isdir(input_dir):
            raise NotADirectoryError(f"Invalid directory: {input_dir}")

        log_files = self._find_log_files(input_dir)
        if not log_files:
            raise FileNotFoundError(f"No .log/.logs files found in: {input_dir}")

        excel_path = os.path.join(input_dir, f"LogsCombined_{versioned_suffix}.xlsx")

        summaries: List[Dict[str, object]] = []
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            used_sheet_names: set = set()

            for path in log_files:
                base = os.path.basename(path)
                sheet = self._unique_sheet_name(self._sanitize_sheet_name(os.path.splitext(base)[0]),
                                                used_sheet_names)
                used_sheet_names.add(sheet)

                lines, encoding_used = self._read_text_file(path)
                df, note = self._parse_log_lines(lines)
                if encoding_used:
                    note = (note + " | " if note else "") + f"encoding={encoding_used}"

                # Límite Excel
                max_rows_excel = 1_048_576
                if len(df) > max_rows_excel:
                    df = df.iloc[:max_rows_excel, :].copy()
                    note = (note + " | " if note else "") + f"Trimmed to {max_rows_excel} rows"

                df.to_excel(writer, sheet_name=sheet, index=False)

                summaries.append({
                    "File": base,
                    "Sheet": sheet,
                    "Rows": int(len(df)),
                    "Columns": int(df.shape[1]),
                    "Note": note or ""
                })

            pd.DataFrame(summaries).to_excel(writer, sheet_name="Summary", index=False)

        print(f"{module_name} Wrote Excel with {len(log_files)} sheet(s) in: '{excel_path}'")
        return excel_path

    # ---------------------------- búsqueda de files ----------------------- #
    def _find_log_files(self, folder: str) -> List[str]:
        files = []
        for name in os.listdir(folder):
            lower = name.lower()
            if lower.endswith(".log") or lower.endswith(".logs"):
                p = os.path.join(folder, name)
                if os.path.isfile(p):
                    files.append(p)
        files.sort()
        return files

    # ---------------------- lectura robusta de texto ---------------------- #
    def _read_text_file(self, path: str) -> Tuple[List[str], Optional[str]]:
        encodings = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "utf-8"]
        last_err = None
        for enc in encodings:
            try:
                with open(path, "r", encoding=enc, errors="strict") as f:
                    return [ln.rstrip("\n") for ln in f], enc
            except Exception as e:
                last_err = e
                continue
        # último intento permisivo
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return [ln.rstrip("\n") for ln in f], None

    # --------------------------- parseador de logs ------------------------ #
    def _parse_log_lines(self, lines: List[str]) -> Tuple[pd.DataFrame, str]:
        """
        Detecta separador y cabecera y construye el DataFrame.
        Prioriza TAB en todo el archivo; si no hay tabs en ninguna línea, intenta coma;
        si tampoco, separa por espacios.
        """
        # ¿Existe algún TAB en TODO el fichero?
        any_tab = any("\t" in ln for ln in lines if ln and not self.SUMMARY_RE.match(ln))
        sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in lines) else None)

        # cabecera = primera línea no resumen que tenga algo de contenido
        header_idx = None
        for i, ln in enumerate(lines):
            if not ln.strip() or self.SUMMARY_RE.match(ln):
                continue
            if sep == "\t" and "\t" in ln:
                header_idx = i
                break
            if sep == "," and "," in ln:
                header_idx = i
                break
            if sep is None:
                header_idx = i
                break

        if header_idx is None:
            return pd.DataFrame(), "No header detected"

        header_cols = self._split_line(lines[header_idx], sep)

        rows: List[List[str]] = []
        for ln in lines[header_idx + 1:]:
            if not ln.strip() or self.SUMMARY_RE.match(ln):
                continue
            parts = self._split_line(ln, sep)
            if len(parts) < len(header_cols):
                parts += [""] * (len(header_cols) - len(parts))
            elif len(parts) > len(header_cols):
                parts = parts[:len(header_cols)]
            rows.append([p.strip() for p in parts])

        df = pd.DataFrame(rows, columns=[c.strip() for c in header_cols])
        df = df.replace({"nan": "", "NaN": "", "None": "", "NULL": ""}).dropna(how="all")
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()

        note = "Tab-separated" if sep == "\t" else ("Comma-separated" if sep == "," else "Whitespace-separated")
        return df, note

    @staticmethod
    def _split_line(line: str, sep: Optional[str]) -> List[str]:
        if sep is None:
            return re.split(r"\s+", line.strip())
        return line.split(sep)

    # -------------------------- helpers nombre hoja ----------------------- #
    @staticmethod
    def _sanitize_sheet_name(name: str) -> str:
        # Excel: máx 31 chars, sin : \ / ? * [ ]
        name = re.sub(r'[:\\/\?\*\[\]]', "_", name)
        name = name.strip().strip("'")
        return (name or "Sheet")[:31]

    @staticmethod
    def _unique_sheet_name(base: str, used: set) -> str:
        if base not in used:
            return base
        for k in range(1, 1000):
            suffix = f" ({k})"
            cand = (base[: max(0, 31 - len(suffix))] + suffix)
            if cand not in used:
                return cand
        i, cand = 1, base
        while cand in used:
            cand = f"{base[:28]}_{i:02d}"
            i += 1
        return cand
