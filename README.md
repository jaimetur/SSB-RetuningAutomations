# 📈 Repo Statistics
[![Commit activity](https://img.shields.io/github/commit-activity/y/jaimetur/RetuningAutomations?label=Commit%20activity)](https://github.com/jaimetur/RetuningAutomations/graphs/contributors)
[![Resolved Github issues](https://img.shields.io/github/issues-closed/jaimetur/RetuningAutomations?label=Resolved%20issues)](https://github.com/jaimetur/RetuningAutomations/issues?q=is%3Aissue%20state%3Aclosed)
[![Open Github issues](https://img.shields.io/github/issues/jaimetur/RetuningAutomations?label=Open%20Issues)](https://github.com/jaimetur/RetuningAutomations/issues)

---

# Retuning Automations
<p align="center">
  <img src="https://github.com/jaimetur/RetuningAutomations/blob/main/assets/logos/logo_01.png?raw=true" alt="RetuningAutomations Logo" width="600" height="480" />
</p>

---

## 📝 Changelog
The Historical Change Log can be checked in the following link:
[Changelog](https://github.com/jaimetur/RetuningAutomations/blob/main/CHANGELOG.md)

## 📅 Roadmap
The Planned Roadmap for futures releases can be checked in the following link:
[Planned Roadmap](https://github.com/jaimetur/RetuningAutomations/blob/main/ROADMAP.md)

## 💾 Download
Download the tool either for Linux, MacOS or Windows (for both x64 and arm64 architectures) as you prefer, directly from following link:
[Latest Stable Release](https://github.com/jaimetur/RetuningAutomations/releases/latest)

---

## 🧭 Overview

**RetuningAutomations** streamlines routine tasks during SSB retuning projects.  
It ships a single launcher that can run in **GUI** mode (no arguments) or **CLI** mode (with arguments) to execute one of several modules:

1. **Configuration Audit** — parses raw log folders and builds a curated Excel workbook (module scaffold ready).  
2. **Consistency Check (Pre/Post Comparison)** — loads Pre and Post datasets, compares relations across frequencies, and generates a clean Excel summary (plus detailed tables).  
3. **Initial Clean-Up (During Maintenance Window)** — helper utilities to tidy intermediate outputs (module scaffold ready).
4. **Final Clean-Up (During Maintenance Window)** — helper utilities to tidy final outputs (module scaffold ready).

The tool automatically adds a **timestamped + versioned suffix** to outputs, which makes artifacts fully traceable (e.g., `20251106-153245_v0.2.0`).

---

## 🖥️ Module Selector
![Module Selector](https://github.com/jaimetur/RetuningAutomations/blob/main/assets/screenshots/module_selector.png?raw=true) 

---

## 🧩 Main Modules

### `1. Configuration Audit`
**Purpose:** Scan the log folder and build a consolidated Excel workbook.

**Notes**
- Public API in place (`ConfigurationAudit.run(input_dir, ...)`).  
- Produces a versioned artifact (timestamp + tool version) when it writes output.  
- Parsing/formatting rules can be extended to your specific log structure.
- 📁 Output is written under: `<INPUT_FOLDER>/ConfigurationAudit__<YYYYMMDD-HHMMSS>_v<TOOL_VERSION>/`
- 📁 Output Example Structure: 
  ```
  <InputFolder>/
  └─ ConfigurationAudit_<timestamp>_v0.2.0/
     ├─ ConfigurationAudit_<timestamp>_v0.2.0.xlsx
     └─ ConfigurationAudit_<timestamp>_v0.2.0.pptx
  ```

---

### `2. Consistency Check (Pre/Post Comparison)`
**Purpose:** Load Pre/Post inputs from an **input folder**, compare relations between a **Pre frequency** and a **Post frequency**, and save results to Excel.

**Key capabilities**
- Loads and validates the required input tables from the selected folder.  
- Optional **frequency comparison** when both `----n77-ssb-pre` and `----n77-ssb-post` are provided.  
- Produces:
  - `CellRelation.xlsx` (all relevant tables)  
  - `CellRelationDiscrepancies.xlsx` (summary + detailed discrepancies) **only** if both frequencies are provided.  
- 📁 Output is written under: `<POST_INPUT_FOLDER>/ConsistencyChecks_<YYYYMMDD-HHMMSS>_v<TOOL_VERSION>/`
- 📁 Correction Commands are written under: `<POST_INPUT_FOLDER>/ConsistencyChecks_<YYYYMMDD-HHMMSS>_v<TOOL_VERSION>/Correction_Cmd`
- 📁 Output Example Structure: 
  ```
  <PostInputFolder>/
  └─ ConsistencyChecks_<timestamp>_v0.2.0/
     ├─ CellRelation_<timestamp>_v0.2.0.xlsx
     └─ ConsistencyChecks_CellRelation_<timestamp>_v0.2.0.xlsx
     └─ Correction_Cmd/
        └─ New_Relations/
           └─ <NODE_NAME>_NR_New.txt
           └─ <NODE_NAME>_GU_New.txt
        └─ Missing_Relations/
           └─ <NODE_NAME>_NR_Missing.txt
           └─ <NODE_NAME>_GU_Missing.txt
        └─ Discrepancies/
           └─ <NODE_NAME>_NR_Disc.txt
           └─ <NODE_NAME>_GU_Disc.txt
  ```

---

### `3. Initial Clean-Up (During Maintenance Window)`
**Purpose:** Utility to sanitize intermediate outputs (delete/add relations, change parameters, etc.) during Maintainance Window (after retuning).

**Notes**
- Module scaffold present. Extend `CleanUp.run(...)` with your clean-up policies.

---

### `4. Final Clean-Up (When retune is finished)`
**Purpose:** Utility to sanitize final cluster (delete profiles , etc.) when the retuning has finished.

**Notes**
- Module scaffold present. Extend `CleanUp.run(...)` with your clean-up policies.

---

# 🔀 Run Modes

## 🖥️ GUI (no arguments)
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

## ⌨️ Command-Line Usage

This tool can be executed either with **GUI mode** (default when no arguments are provided) or entirely through **CLI mode** using the options described below.

### ▶️ Basic Syntax

```bash
RetuningAutomations.exe/bin --module <module-name> [options]

--module                     Module to run: configuration-audit | consistency-check | initial-cleanup | final-cleanup
--input                      Input folder to process (single-input modules)
--input-pre                  PRE input folder (only for consistency-check)
--input-post                 POST input folder (only for consistency-check)

--n77-ssb-pre                N77 SSB frequency before refarming (Pre), e.g. 647328
--n77-ssb-post               N77 SSB frequency after refarming (Post), e.g. 653952
--n77b-ssb                   N77B SSB frequency (ARFCN), e.g. 650334

--freq-filters               Comma-separated list of frequency substrings to filter pivot columns in Configuration Audit

--allowed-n77-ssb-pre        Comma-separated allowed N77 SSB (Pre) values for Configuration Audit
--allowed-n77-arfcn-pre      Comma-separated allowed N77 ARFCN (Pre) values for Configuration Audit

--allowed-n77-ssb-post       Comma-separated allowed N77 SSB (Post) values for Configuration Audit
--allowed-n77-arfcn-post     Comma-separated allowed N77 ARFCN (Post) values for Configuration Audit

--no-gui                     Disable GUI usage (force CLI mode even with missing arguments)
```

If `--module` is omitted and **no other arguments** are provided, the GUI will launch automatically unless `--no-gui` is specified.

---

### 🔧 Available Modules

| Module                | Description                                                         |
|-----------------------|---------------------------------------------------------------------|
| `configuration-audit` | Runs the Configuration Audit module (single input folder).          |
| `consistency-check`   | Runs the Pre/Post Relations Consistency Check (dual input folders). |
| `initial-cleanup`     | Runs the Initial Clean-Up module (single input folder).             |
| `final-cleanup`       | Runs the Final Clean-Up module (single input folder).               |

---

### 📂 Input Options

#### Single-Input Modules (`configuration-audit`, `initial-cleanup`, `final-cleanup`)

```
--input <folder>
```

#### Dual-Input Module (`consistency-check`)

```
--input-pre  <folder>
--input-post <folder>
```

---

### 📡 Frequency Arguments

#### Pre/Post SSB reference frequencies

```
--n77-ssb-pre  <freq>     # Frequency before refarming
--n77-ssb-post <freq>     # Frequency after refarming
```

#### N77B SSB Frequency

```
--n77b-ssb <arfcn>
```

#### Configuration Audit pivot filtering

```
--freq-filters <comma-separated-list>
```
Filters pivot columns by substring match.

---

### 📜 Allowed List Arguments (Configuration Audit)

#### PRE allowed lists

```
--allowed-n77-ssb-pre   <comma-separated-values>
--allowed-n77-arfcn-pre <comma-separated-values>
```

#### POST allowed lists

```
--allowed-n77-ssb-post   <comma-separated-values>
--allowed-n77-arfcn-post <comma-separated-values>
```

---

### 🖥️ GUI Control

```
--no-gui
```

Forces CLI-only mode even if arguments are missing.

---

## 🧪 Usage Examples

### 1. Configuration Audit

```bash
python RetuningAutomations.py \
  --module configuration-audit \
  --input "./AuditInput" \
  --n77-ssb-pre 647328 \
  --n77-ssb-post 653952
```

### 2. Consistency Check (Pre/Post folders)

```bash
python RetuningAutomations.py \
  --module consistency-check \
  --input-pre "./Step0" \
  --input-post "./Step3" \
  --n77-ssb-pre 647328 \
  --n77-ssb-post 653952
```

### 3. Configuration Audit with custom allowed lists

```bash
python RetuningAutomations.py \
  --module configuration-audit \
  --input "./audit" \
  --allowed-n77-ssb-pre 648672,649200 \
  --allowed-n77-arfcn-pre 648648,648984
```

---

## 🔎 Versioning & Traceability

- The launcher prints a banner on start:
  ```
  RetuningAutomations_v0.2.0 - 2025-11-05
  Multi-Platform/Multi-Arch tool designed to Automate some process during SSB Retuning
  ©️ 2025 by Jaime Tur (jaime.tur@ericsson.com)
  ```
- All generated artifacts include a **timestamp + tool version** suffix, e.g.:
  ```
  20251106-153245_v0.2.0
  ```
  ensuring reproducibility and traceability across deliveries.

---

## 🛡️ Code of Conduct
By participating in this project, you agree to abide by our [Code of Conduct](https://github.com/jaimetur/RetuningAutomations/blob/main/CODE_OF_CONDUCT.md).

## 📢 Disclaimer

- ⚠️ The project is under **very active** development.
- ⚠️ Expect bugs and breaking changes.
  
---

## 📊 Repository activity
![Alt](https://repobeats.axiom.co/api/embed/b3021f0fd0db11466b473e34c9de04cc5d85f110.svg "Repobeats analytics image")

## 📈 Star History
<a href="https://www.star-history.com/#jaimetur/RetuningAutomations&Date">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=jaimetur/RetuningAutomations&type=Date&theme=dark" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=jaimetur/RetuningAutomations&type=Date" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=jaimetur/RetuningAutomations&type=Date" />
 </picture>
</a>

## 👥 Contributors
<a href="https://github.com/jaimetur/RetuningAutomations/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=jaimetur/RetuningAutomations" width="15%"/>
</a>

If you want to Contribute to this project please, first read the file [CONTRIBUTING.md](https://github.com/jaimetur/RetuningAutomations/blob/main/CONTRIBUTING.md)

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
