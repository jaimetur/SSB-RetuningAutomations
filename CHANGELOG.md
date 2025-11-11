# 🗓️ CHANGELOG
[Planned Roadmap](https://github.com/jaimetur/RetuningAutomations/blob/main/ROADMAP.md) for the following releases
[Changelog](https://github.com/jaimetur/RetuningAutomations/blob/main/CHANGELOG.md) for the past releases+

---

## Release: v0.2.6
- ### Release Date: 2025-11-12

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
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
