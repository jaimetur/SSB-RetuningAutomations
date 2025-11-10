# 🗓️ CHANGELOG
[Planned Roadmap](https://github.com/jaimetur/RetuningAutomations/blob/main/ROADMAP.md) for the following releases
[Changelog](https://github.com/jaimetur/RetuningAutomations/blob/main/CHANGELOG.md) for the past releases+

---

## Release: v0.2.4
- ### Release Date: 2025-11-10

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 🚀 Enhancements:
    - Common methods and helpers from different Automation Modules extracted from their classes and added to a new Python module called common.py for a better efficiency and maintainance.
    - Smart sorting of MO names based on prefefined list `TABLES_ORDER` or log filename.
    - Added timer to measure the execution time of each module.
    - Module's Renaming. Current Module Names are:  
      `1. Configuration Audit (Logs Parser)`  
      `2. Consistency Check (Pre/Post Comparisson)`  
      `3. Initial Clean-Up (During Maintenance Window)`  
      `4. Final Clean-Up (After Retune is completed)`  

  - #### 📚 Documentation: 
    - Updated documentation with latest changes

---

## Release: v0.2.3
- ### Release Date: 2025-11-07

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Logger support for all print outputs.
    - `Input Folder` persistent between executions.
    
  - #### 🐛 Bug fixes:
    - Minor bug fixing.

  - #### 📚 Documentation: 
    - Updated documentation with latest changes
    
  - #### 🚀 Enhancements:
    - GUI for module selection/launcher open again when any module finish successfully.
    - MO autodetection based on content instead of log filename.
    - Multiple table detection for each MO log file.
    - Capture exceptions during modules executions and avoid the tool to exit when any error is found.
    - Added 2 new columns in Summary sheet of module `2. Create Excel from Logs` with the Log filename where the table have been found and with the number of tables of each log file.

---

## Release: v0.2.2
- ### Release Date: 2025-11-06

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    
  - #### 🐛 Bug fixes:
    
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
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Module '2. Create Excel from Logs' ready.
    
  - #### 🐛 Bug fixes:
    - Fixed bug on Summary_Detailed that was creating pairs of frequencies where Freq_Pre or Freq_Post was empty.

  - #### 🚀 Enhancements:
    - Created module to compile the tool and generate binaries files for the different OS and architecture.
    - Created GitHub repository.
    - Created GitHub workflow to automatically generate binaries for the different OS and architecture.

  - #### 📚 Documentation: 
    - Created README.md
    - Created CHANGELOG.md
    - Created ROADMAP.md
     
---

## Release: v0.2.0
- ### Release Date: 2025-11-05

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Create a Module Selector & Configuration window.
    
  - #### 🐛 Bug fixes:
    - Fixed bug on Summary_Detailed that was creating pairs of frequencies where Freq_Pre or Freq_Post was empty.

  - #### 🚀 Enhancements:
    - Improvements on Module '1. Pre/Post Relations Consistency Check' to satisfy the requirements.
    
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
