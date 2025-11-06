# 📈 Repo Statistics
[![Commit activity](https://img.shields.io/github/commit-activity/y/jaimetur/RetuningAutomations?label=Commit%20activity)](https://github.com/jaimetur/RetuningAutomations/graphs/contributors)
[![Resolved Github issues](https://img.shields.io/github/issues-closed/jaimetur/RetuningAutomations?label=Resolved%20issues)](https://github.com/jaimetur/RetuningAutomations/issues?q=is%3Aissue%20state%3Aclosed)
[![Open Github issues](https://img.shields.io/github/issues/jaimetur/RetuningAutomations?label=Open%20Issues)](https://github.com/jaimetur/RetuningAutomations/issues)
[![Total Github Releases downloads](https://img.shields.io/github/downloads/jaimetur/RetuningAutomations/total?label=Total%20downloads)](https://github.com/jaimetur/RetuningAutomations/releases)
[![Latest version downloads](https://img.shields.io/github/downloads/jaimetur/RetuningAutomations/latest/total?label=Latest%20version%20downloads)](https://github.com/jaimetur/RetuningAutomations/releases/latest)
[![Pre-release version downloads](https://img.shields.io/github/downloads/jaimetur/RetuningAutomations/v3.6.2/total?label=Pre%20version%20downloads)](https://github.com/jaimetur/RetuningAutomations/releases?q=%22alpha%22+OR+%22beta%22+OR+%22RC%22&expanded=true)

---
# Retuning Automations Tool

---

## 🧭 Overview

**RetuningAutomations** streamlines routine tasks during SSB retuning projects.  
It ships a single launcher that can run in **GUI** mode (no arguments) or **CLI** mode (with arguments) to execute one of several modules:

1. **Pre/Post Relations Consistency Check** — loads Pre and Post datasets, compares relations across frequencies, and generates a clean Excel summary (plus detailed tables).  
2. **Create Excel from Logs** — parses raw log folders and builds a curated Excel workbook (module scaffold ready).  
3. **Clean-Up** — helper utilities to tidy intermediate outputs (module scaffold ready).

The tool automatically adds a **timestamped + versioned suffix** to outputs, which makes artifacts fully traceable (e.g., `20251106-153245_v0.2.1`).

---

# 🖥️ Module Selector
![Module Selector](https://github.com/jaimetur/RetuningAutomations/blob/main/assets/screenshots/module_selector.png?raw=true) 

---

## 🧩 Main Modules

### 1) `Pr/ePost Relations Consistency Checks`
**Purpose:** Load Pre/Post inputs from an **input folder**, compare relations between a **Pre frequency** and a **Post frequency**, and save results to Excel.

**Key capabilities**
- Loads and validates the required input tables from the selected folder.  
- Optional **frequency comparison** when both `--freq-pre` and `--freq-post` are provided.  
- Produces:
  - `CellRelation.xlsx` (all relevant tables)  
  - `CellRelationDiscrepancies.xlsx` (summary + detailed discrepancies) **only** if both frequencies are provided.  
- 📁 Output is written under: `<INPUT_FOLDER>/CellRelationConsistencyChecks_<YYYYMMDD-HHMMSS>_v<TOOL_VERSION>/`
- 📁 Output Example Structure: 
  ```
  <InputFolder>/
  ├─ LogsCombined_<timestamp>_v0.2.1.xlsx
  └─ CellRelationConsistencyChecks_<timestamp>_v0.2.1/
     ├─ CellRelation.xlsx
     └─ CellRelationConsistencyChecks.xlsx
  ```

---

### 2) `Create Excel From Logs`
**Purpose:** Scan the log folder and build a consolidated Excel workbook.

**Notes in v0.2.1**
- Public API in place (`CreateExcelFromLogs.run(input_dir, ...)`).  
- Produces a versioned artifact (timestamp + tool version) when it writes output.  
- Parsing/formatting rules can be extended to your specific log structure.

---

### 3) `Clean-Up`
**Purpose:** Utility to sanitize intermediate outputs (delete/add relations, change parameters, etc.).

**Notes in v0.2.1**
- Module scaffold present. Extend `CleanUp.run(...)` with your clean-up policies.

---

## 🖥️ Run Modes

### GUI (no arguments)
Running the launcher **without CLI arguments** opens a compact Tkinter dialog where you can:
- Pick the **module** from a combo box.  
- Choose the **input folder** (Browse…).  
- Optionally set **Pre** and **Post** frequencies (defaults provided).  

**Start (GUI):**
```bash
python RetuningAutomations.py
```

> The GUI is skipped if Tkinter is not available or `--no-gui` is used.

---

### CLI (headless)
You can run any module directly from the command line.

**General form:**
```bash
python RetuningAutomations.py --module {prepost|excel|cleanup} -i "<INPUT_FOLDER>"   --freq-pre 648672 --freq-post 647328
```

> If you omit `-i` but do **not** pass `--no-gui` and Tkinter is available, the tool will offer the GUI to complete missing fields.  
> If both `--no-gui` and `-i` are omitted, the tool exits with an error.

---

## ⚙️ CLI Reference

```text
--module     Module to run: prepost | excel | cleanup
-i, --input  Input folder to process
--freq-pre   Frequency before refarming (Pre), e.g. 648672
--freq-post  Frequency after refarming (Post), e.g. 647328
--no-gui     Disable GUI prompts (require CLI args)
```

### Examples

**A. Pre/Post with comparison (full):**
```bash
python RetuningAutomations.py --module prepost   -i "C:\Projects\Retuning\Round_01\Input"   --freq-pre 648672   --freq-post 647328
```
- Writes:
  - `CellRelation.xlsx`
  - `CellRelationDiscrepancies.xlsx`
  - Under: `CellRelationConsistencyChecks_<YYYYMMDD-HHMMSS>_v0.2.1/`

**B. Pre/Post without frequencies (tables only):**
```bash
python RetuningAutomations.py --module prepost   -i "/data/retuning/PA6/Input"
```
- Writes:
  - `CellRelation.xlsx` (no comparison workbook)

**C. Create Excel from Logs:**
```bash
python RetuningAutomations.py --module excel   -i "/data/retuning/logs/PA6"
```

**D. Clean-Up (scaffold):**
```bash
python RetuningAutomations.py --module cleanup   -i "/data/retuning/outputs"
```

---

## 📂 Expected Input & Produced Output

### Input folder
A typical **input folder** for `PrePostRelations` contains source logs / CSVs / tables exported from your planning or OSS tools.  
The loader in `PrePostRelations.loadPrePost(input_dir)` expects the needed tables (naming/format depends on your pipeline); extend the loader to your conventions.

### Output structure
```
<INPUT_FOLDER>/
└─ CellRelationConsistencyChecks_<YYYYMMDD-HHMMSS>_v0.2.1/
   ├─ CellRelation.xlsx
   └─ CellRelationDiscrepancies.xlsx        # only when both frequencies provided
```

For `CreateExcelFromLogs`, the module itself returns the **path** of the artifact it writes (if any). It also appends the standard `_<YYYYMMDD-HHMMSS>_v<TOOL_VERSION>` suffix to the filename.

---

## 🔎 Versioning & Traceability

- The launcher prints a banner on start:
  ```
  RetuningAutomations_v0.2.1 - 2025-11-06
  Multi-Platform/Multi-Arch tool designed to Automate some process during SSB Retuning
  ©️ 2025 by Jaime Tur (jaime.tur@ericsson.com)
  ```
- All generated artifacts include a **timestamp + tool version** suffix, e.g.:
  ```
  20251106-153245_v0.2.1
  ```
  ensuring reproducibility and traceability across deliveries.

---

## 🛠️ Installation

> Python 3.10+ is recommended.

1. **Clone the repo**
   ```bash
   git clone https://github.com/jaimetur/RetuningAutomations.git
   cd RetuningAutomations
   ```

2. **Create & activate a virtual environment** (optional but recommended)
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # macOS/Linux
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   Typical dependencies include:
   - `pandas` (tabular handling)
   - `openpyxl` (Excel writer/reader)
   - `numpy` (array ops)
   - `tkinter` (GUI; usually preinstalled on Windows/macOS; install via your distro on Linux)

---

## 🛠️ Requirements
Python 3.10+, pandas, openpyxl, tkinter

---

## 📦 Building Standalone Binaries (optional)

If the repository includes `_compile_pyinstaller.py`, you can produce single-folder or single-file executables:

```bash
# Ensure you installed requirements first
pip install -r requirements.txt

# Build
python _compile_pyinstaller.py
```

The script should:
- Verify required modules.  
- Build for your platform/arch.  
- Drop artifacts in a `dist/` or `build/` folder.

> For CI/CD (GitHub Actions), configure a matrix job to run `_compile_pyinstaller.py` on each target OS/arch.  
> Make sure to **install requirements first** in each job before invoking the compile step.

---

## 🧪 Validations & Logging

- Each module prints a **short preamble** indicating the module name and the input folder.  
- Missing required inputs will result in **clear error messages** (and GUI fallbacks where applicable).  
- Extend internal `logger` usage to persist run metadata (input path, timestamps, frequencies, tables loaded, row counts, etc.).

---

## 🗺️ Roadmap

- [ ] Fill `CreateExcelFromLogs` with robust parsers & schema validators.  
- [ ] Implement `CleanUp` policies (temp deletion, file normalization, conflict resolution).  
- [ ] Add schema checks for input tables + repair helpers.  
- [ ] Optional HTML report alongside Excel exports.  
- [ ] Unit tests for loaders and frequency comparison logic.

---

## 🤝 Contributing

1. Fork the repo  
2. Create a feature branch: `feat/<short-name>`  
3. Commit with clear messages  
4. Open a PR describing:
   - Scope  
   - Sample inputs/outputs  
   - Any schema or parameter changes  

---

## 🧾 License

Unless otherwise stated in the repository, this project is provided under a permissive license.  
Check the `LICENSE` file at the root of the repo.

---

## 📬 Contact

- **Author:** Jaime Tur  
- **Email:** jaime.tur@ericsson.com

> For bug reports, please open a **GitHub Issue** with:
> - The exact command you ran (or GUI selections)  
> - OS/arch and Python version (or binary flavor)  
> - A redacted screenshot or snippet of the input folder structure  
> - The generated timestamp/version suffix
