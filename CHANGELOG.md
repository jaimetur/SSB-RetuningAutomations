# 🗓️ CHANGELOG
[Planned Roadmap](https://github.com/jaimetur/RetuningAutomations/blob/main/ROADMAP.md) for the following releases
[Changelog](https://github.com/jaimetur/RetuningAutomations/blob/main/CHANGELOG.md) for the past releases+

---

## Release: v0.3.0
- ### Release Date: 2025-11-19

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Refactor main module `RetuningAutomations` to simplify the logic.
    - Added ARFCN 650006 to default Allowed ARFCN list.
    - Added 'LTE nodes with GUtranSyncSignalFrequency defined' to GUtran Frequency Audit.
    - Changed N77 band detection to filter freqs within range 646600-660000.
    - Avoid adding Inconsistencies slides if there is no any inconsistency found for each metric.
  
  - #### 🐛 Bug fixes:
    - Fix PPT template not found.
    - Fixed some Configuration Audit Metrics.
    - Fixed truncated lists of nodes in Configuration Audit.
    - Fixed duplicates in extra column of NR_Sector_Carrier Inconsistencies.
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.2.9
- ### Release Date: 2025-11-18

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Check if all EndcDistrProfile gUtranFreqRef is always set to (old_arfcn or new_arfcn) and & n77b_ssb (653952)
    - Added N77B SSB fields to GUI and CLI intefaces.

  - #### 🚀 Enhancements:
    - Refactor module `ConfigurationAudit` and split in different submodules
    - Network Audit Enhancements (#26) now includes: 
      - Count/List of Nodes/Cells per category:
        - Count Nodes with N77 cells. NRCellDU Pivot table to check if all cells with SSB 648672&653952. NRSectorCarrier to check if all N77B sectors with ARFCN 654652, 655324, 655984 or 656656
        - Count NR nodes with NrFrequency/NrFreqRelation 648672 defined. Pivot tables to detect if 647328 already defined 
        - Count LTE nodes with GUtranSyncSignalFrequency/GUtranFreqRelation 648672 defined. Pivot table to detect if 647328 already defined
      - Check if any referece to new SSB in any node: FreqRelation, Frequency. If autocreated might need additional actions
      - Check if any cell reached max FreqRelation cardinality (max 16 NrFreqRelation and 16 GUtranFreqRelation per Cell)
      - Check if any nodes reached max 24 GUtranSyncSignalFrequency definitions
      - Check if any nodes reached max 64 NRFrequency definitions
      - Check if all EndcDistrProfile gUtranFreqRef always set to 648672&653952
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.8
- ### Release Date: 2025-11-17

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Added PPT Template.
    - Modified PPT to split nodes in chunks of 50 nodes per slide (2 columns of 50 nodes).
    - Persist Allowed SSB and ARFCN lists between different executions.
    - Improvements on Configuration Audit.
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.7
- ### Release Date: 2025-11-14

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - If Consistency Check module is selected, then use two input folders, one for Pre and one for Post (#25)
    - Network Audit Enhancements:
      - Count/List of Nodes/Cells per category:
        - Count Nodes with N77 cells. NRCellDU Pivot table to check if all cells with SSB 648672&653952. NRSectorCarrier to check if all N77B sectors with ARFCN 654652, 655324, 655984 or 656656
        - Count NR nodes with NrFrequency/NrFreqRelation 648672 defined. Pivot tables to detect if 647328 already defined 
        - Count LTE nodes with GUtranSyncSignalFrequency/GUtranFreqRelation 648672 defined. Pivot table to detect if 647328 already defined
      - Check if any referece to new SSB in any node: FreqRelation, Frequency. If autocreated might need additional actions
      - Check if any cell reached max FreqRelation cardinality (max 16 NrFreqRelation and 16 GUtranFreqRelation per Cell)
      - Check if any nodes reached max 24 GUtranSyncSignalFrequency definitions
      - Check if any nodes reached max 64 NRFrequency definitions
      - Check if all EndcDistrProfile gUtranFreqRef always set to 648672&653952
    - Configuration Audit now generates a PPT with a Text Summary.

  - #### 🚀 Enhancements:
    - Align Summary headers to left (#23)
    - Include input log folder in Summary sheets of Consistency Check (#24)
    - Modified GUI to accept `ALLOWED_SSB_N77` and `ALLOWED_N77B_ARFCN` lists for Configuration Audit.
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes
  
---

## Release: v0.2.6
- ### Release Date: 2025-11-12

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Improvements on Date detection method to do an smart detection of any date included in the input folder.
    - Changed date format to YYYY-MM-DD for a better visualization.
    - Changed color of Summary Sheets in output Excel to green for a better visualization. 
    - Changed macos-x64 runer on GitHub workflow.
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    - Added `Summary NRFrequency` table to `Configuration Audit` module.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.5
- ### Release Date: 2025-11-11

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Configuration Audit now creates Pivot Table with useful Summary Information. 
    - Added Frequencies selector in GUI to select which frequencies you want to run Audit for.

  - #### 🚀 Enhancements:
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    - Fixed Exception on `Consistency Check` module when Pre or Post folder has not been found in Input Folder
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.4
- ### Release Date: 2025-11-10

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Common methods and helpers from different Automation Modules extracted from their classes and added to a new Python module called common.py for a better efficiency and maintainance.
    - Smart sorting of MO names based on prefefined list `TABLES_ORDER` or log filename.
    - Added timer to measure the execution time of each module.
    - Module's Renaming. Current Module Names are:  
      `1. Configuration Audit (Logs Parser)`  
      `2. Consistency Check (Pre/Post Comparison)`  
      `3. Initial Clean-Up (During Maintenance Window)`  
      `4. Final Clean-Up (After Retune is completed)`  
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.3
- ### Release Date: 2025-11-07

- ### Main Changes:
   
  - #### 🌟 New Features:
    - Logger support for all print outputs.
    - `Input Folder` persistent between executions.

  - #### 🚀 Enhancements:
    - GUI for module selection/launcher open again when any module finish successfully.
    - MO autodetection based on content instead of log filename.
    - Multiple table detection for each MO log file.
    - Capture exceptions during modules executions and avoid the tool to exit when any error is found.
    - Added 2 new columns in Summary sheet of module `2. Create Excel from Logs` with the Log filename where the table have been found and with the number of tables of each log file.
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.

  - #### 📚 Documentation: 
    - Updated documentation with latest changes
    
---

## Release: v0.2.2
- ### Release Date: 2025-11-06

- ### Main Changes:

  - #### 🚀 Enhancements:
    - Improvements on GitHub Automatic Release creation.
    - Added Splash Logo on Windows executable while loading.
     
  - #### 📚 Documentation: 
    - Created DOWNLOAD.md
    - Created CONTRIBUTING.md
    - Created CODE_OF_CONDUCT.md

---

## Release: v0.2.1
- ### Release Date: 2025-11-06

- ### Main Changes:
   
  - #### 🌟 New Features:
    - Module '2. Create Excel from Logs' ready.

  - #### 🚀 Enhancements:
    - Created module to compile the tool and generate binaries files for the different OS and architecture.
    - Created GitHub repository.
    - Created GitHub workflow to automatically generate binaries for the different OS and architecture.
    
  - #### 🐛 Bug fixes:
    - Fixed bug on Summary_Detailed that was creating pairs of frequencies where Freq_Pre or Freq_Post was empty.

  - #### 📚 Documentation: 
    - Created README.md
    - Created CHANGELOG.md
    - Created ROADMAP.md
     
---

## Release: v0.2.0
- ### Release Date: 2025-11-05

- ### Main Changes:
   
  - #### 🌟 New Features:
    - Create a Module Selector & Configuration window.

  - #### 🚀 Enhancements:
    - Improvements on Module '1. Pre/Post Relations Consistency Check' to satisfy the requirements.
    
  - #### 🐛 Bug fixes:
    - Fixed bug on Summary_Detailed that was creating pairs of frequencies where Freq_Pre or Freq_Post was empty.

---

## Release: v0.1.1
- ### Release Date: 2025-11-04

- ### Main Changes:

  - #### 🚀 Enhancements:
    - Improvements on Module '1. Pre/Post Relations Consistency Check' to satisfy the requirements.

---

## Release: v0.1.0
- ### Release Date: 2025-11-03

- ### Main Changes:
   
  - #### 🌟 New Features:
    - Module '1. Pre/Post Relations Consistency Check' first release.

    
---
