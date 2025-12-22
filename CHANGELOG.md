# 🗓️ CHANGELOG
[Planned Roadmap](https://github.com/jaimetur/RetuningAutomations/blob/main/ROADMAP.md) for the following releases
[Changelog](https://github.com/jaimetur/RetuningAutomations/blob/main/CHANGELOG.md) for the past releases

---

## Release: v0.5.1
- ### Release Date: 2025-12-22

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
    - Removed module `4. Profiles Audit` and incorporated the logic within module `1. Configuration Audit`.
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:

  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.5.0
- ### Release Date: 2025-12-22

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
    - Renamed Tool name from `RetuningAutomations` to `SSB_RetuningAutomations`.
    - Renamed module `4. Initial Clean-Up` to `4. Profiles Audit`.
  
  - #### 🌟 New Features:
    - Include `Profiles Inconsistencies` to `Configuration Audit` module (in SummaryAudit table) when it is executed from module `4. Profiles Audit` module.
    - Include `Profiles Discrepancies` to `Configuration Audit` module (in SummaryAudit table) when it is executed from module `4. Profiles Audit` module.
    - Added new checks to `ConfigurationAuudit` module to include:
      - NR nodes with the old N77 SSB and the new SSB pointing to some mcpcPCellNrFreqRelProfileRef (from NRFreqRelation table)
      - NR nodes with the old N77 SSB and the new SSB pointing to clone mcpcPCellNrFreqRelProfileRef (from NRFreqRelation table)
    - Added new checks to `ProfilesAuudit` module to include:
      - NR nodes with the new N77 SSB and NRCellCU Ref parameters to Profiles with the old SSB name (from NRCellCU table)
      - NR nodes with the new N77 SSB and EUtranFreqRelation Ref parameters to Profiles with the old SSB name (from EUtranFreqRelation table)
      - NR nodes with the old N77 SSB and the new SSB pointing to some mcpcPCellNrFreqRelProfileRef (from NRFreqRelation table)
      - NR nodes with the old N77 SSB and the new SSB pointing to clone mcpcPCellNrFreqRelProfileRef (from NRFreqRelation table)

  - #### 🚀 Enhancements:

  - #### 🐛 Bug fixes:
    - Fixed build_all.yml to update the new Tool name.
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated README.md.
    - Updated ROADMAP.md.
    - Logo updated.
    - Updated documentation with latest changes.

---

## Release: v0.4.3
- ### Release Date: 2025-12-19

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Added `FoldersCompared.txt` file to output folder when `ConsistencyCheck` module has been executed to show the folders used for Pre and Post. 

  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated README.md.
    - Updated documentation with latest changes.

---

## Release: v0.4.2
- ### Release Date: 2025-12-19

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Optimized TXT Commands file to group all commands within the same nodes and avoid duplicate lines with `confb+, lt all, alt, wait` commands.
    - Modified TermpointToGNB Correction Command.
    - Modified TermpointToGnodeB Correction Command.
    - Modified NR Relations Discrepancies Correction Command.
    - Correction_Cmd output subfolders have been renamed as follow:
      - from `Discrepancies` to `RelationsDiscrepancies`
      - from `New Relations` to `NewRelations`
      - from `Missing Relations` to `MissingRelations`
    - Correction_Cmd output subfolders for `RelationsDiscrepancies`, `NewRelations` and `MissingRelations` are now sepparated into two subfolders (`NR` and `GU`) for a better organization.

  - #### 🐛 Bug fixes:
    - Fixed bug on table `Summary GU_FreqRelation` that was taking the data from `GUtranSyncSignalFrequency` table instead of `GUtranFreqRelation`.
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.4.1
- ### Release Date: 2025-12-18

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - When running `ConsistencyCheck` module, the Pre/Post Audits now are saved into the same output folder as the `ConsistencyCheck` output.

  - #### 🚀 Enhancements:
    - Added Correction Commands for LTE External Cells and Termpoints.
    - External commands and Termpoint commands are now split by Node.
    - External commands are now saved into subfolders `ExternalNRCellCU` and `ExternalGUtranCell`.
    - Termpoint commands are now saved into subfolders `TermPointToGNodeB` and `TermPointToGNB`.
    - Code Refactored to split the module ca_summary into smaller modules for a better management of it.

  - #### 🐛 Bug fixes:
    - Fixed `ERROR: 'str' object has no attribute 'astype'` on `ConfigurationAudit` module when `availabilityStatus` column is not found.
    - Fixed Error when any MO has the same name as the expected table but with some letter in upper/lower capital.
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.4.0
- ### Release Date: 2025-12-16

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - New metrics added to `ConfigurationAudit` module to count:
      - Table/Category: `ExternalNRCellCU`
        - ExternalNRCellCU to old N77 SSB (from ExternalNRCellCU)
        - ExternalNRCellCU to new N77 SSB (from ExternalNRCellCU)
      - Table/Category: `TermPointToGNodeB`
        - NR to NR TermPoint administrativeState LOCKED (from TermPointToGNodeB)
        - NR to NR TermPoint operationalState DISABLED  (from TermPointToGNodeB)
      - Table/Category: `ExternalGUtranCell`
        - ExternalGUtranCell to old N77 SSB (xxxx) (from ExternalGUtranCell)
        - ExternalGUtranCell to new N77 SSB (xxxx) (from ExternalGUtranCell)
        - ExternalGUtranCell to old N77 SSB (xxx) serviceStatus OUT_OF_SERVICE (from ExternalGUtranCell)
        - ExternalGUtranCell to new N77 SSB (xxx) serviceStatus OUT_OF_SERVICE (from ExternalGUtranCell)
      - Table/Category: `TermPointToGNB`
          - LTE to NR TermPoints with administrativeState=LOCKED (from TermPointToGNB)
          - LTE to NR TermPoints with operationalState=DISABLED (from TermPointToGNB)
          - LTE to NR TermPoints with usedIpAddress=0.0.0.0/:: (from TermPointToGNB)
      - Table/Category: `TermPointToENodeB`
        - NR to LTE TermPoints with administrativeState=LOCKED (from TermPointToENodeB)
        - NR to LTE TermPoints with operationalState=DISABLED (from TermPointToENodeB)
    - Added Additional Columns to `ExternalNRCellCU` table:
      - `Termpoint`: Unique identifier to identify the termpoint.
      - `TermpointStatus`: Contains the current status of the termpoint as concatenation of `administrativeState`-`operationalState`-`availabilityStatus`.
      - `TermpointConsolidatedStatus`: Contains the current status of the termpoint in a consolidated way.
      - `GNodeB_SSB_Target`: Useful to identify nodes with SSB-Pre/SSB-Post or nodes with SSB different to Pre/Post.
      - `Correction_Cmd`: Contains the correction command to fix the termpoint.
    - Added Additional Columns to `TermPointToGNodeB` table:
      - `Termpoint`: Unique identifier to identify the termpoint.
      - `TermpointStatus`: Contains the current status of the termpoint as concatenation of `administrativeState`-`operationalState`-`availabilityStatus`.
      - `TermpointConsolidatedStatus`: Contains the current status of the termpoint in a consolidated way.
      - `SSB needs update`: Indicates those termpoints whose termpoint id has been found in `ExternalNRCellCU` with "SSB-Pre" and with `GNodeB_SSB_Target` different to "SSB-Pre".
      - `Correction_Cmd`: Contains the correction command to fix the termpoint.
    - Added Correction Commands for NR External Cells and Termpoints.

  - #### 🚀 Enhancements:
    - Excel columns Autofit now is limited to a maximum column width of 100.
    - `SummaryAudit` Category column now contains links to the source table of each audit.
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.11
- ### Release Date: 2025-12-04

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - New feature to allow Frequency Filtering for `ConsistencyCheck` module.
    - GUI lanuncher adapted to include the list of frequencies to filter on `ConsistencyCheck` module.

  - #### 🚀 Enhancements:
    - Some code refactoring to clean-up duplicates functions and group them by logic.
    - Output folder for `ConfigurationAudits` now has the market suffix if they have been executed from `ConsistencyCheck` module and any market have been detected.
    - Removed `timeOfCreation` from list of parameters to find discrepancies on `ConsistencyCheck` moodule.
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.10
- ### Release Date: 2025-12-03

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - New feature to recursively run Audit on all subfolders of Input Folder where any valid log is found. To run this feature you just need to provide an Input folder with no logs inside but with subfolders with valid logs inside.
    - New feature to auto-detect Pre/Post folders given only one Input folder with a predefined folder structure.
      - For this feature to work, the given input folder should contain subfolders with this naming convention: `yyyymmdd_hhmm_step0` (Optionally they may be a Market Subfolder inside it). 
        - Example 1:
          - 20251203_0530_step0 --> This is selected as Pre folder since is the oldest folder for the latest day
          - 20251203_0730_step0 --> This is selected as Post folder since is the latest folder for the latest day
        - Example 2:
          - 20251202_0530_step0
          - 20251202_0730_step0 --> This is selected as Pre folder since is the latest folder for the latest day previous to the Post folder day 
          - 20251203_0530_step0
          - 20251203_0730_step0 --> This is selected as Post folder since is the latest folder for the latest day
    - Added a Blacklist of words to discard any step0 subfolders from auto-detection function. By default the tool will not consideer as Pre/Post candidates any folder with any of the following words in its name: "ignore", "old", "discard", "bad".
    - Added a Blacklist of words to discard any input subfolders from auto-detection function. By default the tool will not consideer as input folder candidates for Audits any folder with any of the following words in its name: "ignore", "old", "discard", "bad".
    - New module called `3. Consistency Check (Bulk mode Pre/Post auto-detection)`. When this module is selected, the tool will automatically run an Smart Consistency Check in all markets detected in the input folder, selecting the most suitable folder for Pre and Post for each market.

  - #### 🚀 Enhancements:
    - Added column `GNBCUCPFunction` to NR tables in `ConcistencyCheck` module to be able to filter those relations of interest.
    - Added `Freq_Pre` and `FreqPost` columns also to `_new` tables of `ConcistencyCheck` module to be consistent with the other tables generated by this module.
    - Deleted unnecessary columns on all `_new`, `_missing`, `_disc` tables in `ConsistencyCheck` module.
    - The GUI launcher dialog now comes to the foreground when it is reopened after the execution of any module.
    - The GUI launcher dialog now is always centered on screen.
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.9
- ### Release Date: 2025-12-02

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Excluded Relations Discrepancies on those relations towards nodes that have not been retuned (buffer) on `ConsistencyCheck` module.
    - Added a new sheet called `SummaryAuditComparison` to `ConsistencyCheck` output Excel file to compare the values of both Audits run (for Pre-folder and Post-folder).

  - #### 🚀 Enhancements:
    - Fixed `Freq_Pre/Freq_Post` values in `NR_Disc` table on `ConsistencyCheck` module.
    - Renamed sheet `Summary_Detailed` to `Summary_CellRelation`.
    - Removed Exception when input folder does not contain logs files. Now a message is displayed instead of throw an exception.
  
  - #### 🐛 Bug fixes:
    - Removed Duplicated check in `GUtranSyncSignalFrequency` Table.
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.8
- ### Release Date: 2025-12-01

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Added Correction Commands on GU_Disc and NR_Disc tables.
    - NR Nodes classification MACRO/mmWave (#35)

  - #### 🚀 Enhancements:
    - Created separated subfolders for each type of Correction Commands.
    - Apply red color font only to Inconsistency rows whose value is higher than zero.
  
  - #### 🐛 Bug fixes:
    - Fixed abnormal prefix for long paths in Windows.
    - Fixed invalid Inconsistency in mixed nodes (MidLowBand & mmWave).
    - Aligned all Audit messages.
    - Modified the mmWave range up to 2.300.000 instead of 2.100.000.
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.7
- ### Release Date: 2025-11-28

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Redesigned GUI Dialog.
  
  - #### 🐛 Bug fixes:
    - Fixed Long path issues on Windows.
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.6
- ### Release Date: 2025-11-27

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Added commands to `Consistency Checks` module.
      - Correction Commands to `GU_new` table.
      - Correction Commands to `NR_new` table.
      - Correction Commands to `GU_missing` table.
      - Correction Commands to `NR_missing` table.
    - Added a TXT file per node with the Correction Commands for each node.
    - Auto-Fit column sizes in Excel sheets.
    - Header is now coloured in different color in Excel sheets.

  - #### 🚀 Enhancements:
    - Created a scheduled workflow for binary genaration.
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.5
- ### Release Date: 2025-11-26

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Added new checks to `Configuration Audit` module.
      - NR cellRelations to old SSB (from NRCellRelation table)
      - NR cellRelations to new SSB (from NRCellRelation table)
      - LTE cellRelations to old SSB (from GUtranCellRelation table)
      - LTE cellRelations to new SSB (from GUtranCellRelation table)
      - LTE nodes with the old SSB but without the new SSB (from FreqPrioNR table)
      - LTE nodes with both, the old SSB and the new SSB (from FreqPrioNR table)
      - LTE cells with mismatching params between FreqPrioNR 648672 and 647328
    - Added new sheet `Summary NR Params Missmatching` with columns `NodeId`, `GNBCUCPFunctionId`, `NRCellCUId` and `NRFreqRelationId`.
    - Added new sheet `Summary LTE Params Missmatching` with columns `NodeId`, `EUtranCellId` and `GUtranFreqRelationId`.
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.4
- ### Release Date: 2025-11-25

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Added new table `Summary Param Missmatching` to `Configuration Audit` module to show which cells/nodes has changed and which params have been changed. 

  - #### 🚀 Enhancements:
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - README.md updated.
    - Updated documentation with latest changes.


---

## Release: v0.3.3
- ### Release Date: 2025-11-24

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Swapped columns Category/SubCategory in `ConfigurationAudit`.
    - Added more Checks to `NRCellDU` Category.
    - Included colors in `SummaryAudit` table for an easier visibility of each category. 
    - `ConfigurationAudit` output is now saved into a subfolder per execution within the log folder.
    - Other changes to `ConfigurationAudit`.
    - Input folders now are not cleaned when a module that requires two input folders is selected.
  
  - #### 🐛 Bug fixes:
    - Fixed errors when running `Consistency Checks` module after refactoring.
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.2
- ### Release Date: 2025-11-21

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:
    - Detect Auto-created NRFreqRelation to new SSB, will not follow VZ naming convention NRFreqRelation=647328.
    - Detect Auto-created GUtranFreqRelation to new SSB, will not follow VZ naming convention GUtranFreqRelation=647328-30-20-0-1.
    - Added two new lists of `Allowed N77 SSB (Post)` and `Allowed N77 ARFCN (Post)` to use in 'ConfigurationAudit'.
    - Added new check to detect number of nodes from NRSectorCarrier whose ARFCN is in the list of allowed ARFCN (Pre).
    - Added new check to detect number of nodes from NRSectorCarrier whose ARFCN is in the list of allowed ARFCN (Post).
    - Added new check to detect those nodes from NRSectorCarrier whose ARFCN is not in the list of allowed ARFCN (Pre) nor allowed ARFCN (Post).
    - Added mismatching params between cells with old SSB and cells with new SSB in tables NRFreqRelation and GUtranFreqRelation

  - #### 🚀 Enhancements:
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.1
- ### Release Date: 2025-11-20

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Included Pivot `GUtranFreqRelation` in `ConfigurationAudit` module.
    - Included LogPath in Summary tab of `ConfigurationAudit`.
    - Check that all nodes with NrFrequency=old_arfcn also have NrFrequency=new_arfcn. (#36)
    - Check that all NRCellCUId with NrFreqRelation=old_arfcn also have NrFreqRelation=new_arfcn and all params are same (except nRFreqRelationId, nRFrequencyRef and reservedBy). (#36)
    - Check that all nodes with GUtranSyncSignalFrequency=old_arfcn also have GUtranSyncSignalFrequency=new_arfcn. (#36)
    - Check that all EUtranCellFDDId with GUtranFreqRelationId=old_arfcn-30-20-0-1 also have GUtranFreqRelationId=new_arfcn-30-20-0-1 and all params are same (except gUtranFreqRelationId and gUtranSyncSignalFrequencyRef). (#36)
    - From table FreqPrioNR, detect how many N77 nodes has RATFreqPrioId equal to 'fwa' and 'publicsafety' and add them to Frequency Audit. (#40)
    - From table FreqPrioNR, detect how many N77 nodes has any RATFreqPrioId different from 'fwa' or 'publicsafety' and add them to Frequency Inconsistencies. (#40)
    - Increased up to 100 nodes/slide (max 4 columns) for the Inconsistencies slides in `ConfigurationAudit` module.
    - Other minor changes to `ConfigurationAudit`.
  
  - #### 🐛 Bug fixes:
    - Minor bug fixing.
    
  - #### 📚 Documentation: 
    - Updated documentation with latest changes.

---

## Release: v0.3.0
- ### Release Date: 2025-11-19

- ### Main Changes:
  
  - #### 🚨 Breaking Changes:
  
  - #### 🌟 New Features:

  - #### 🚀 Enhancements:
    - Refactor main module `RetuningAutomations` to simplify the logic.
    - Added ARFCN 650006 to default Allowed ARFCN list.
    - Added 'LTE nodes with GUtranSyncSignalFrequency defined' to LTE Frequency Audit.
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
    - Fixed bug on Summary_CellRelation that was creating pairs of frequencies where Freq_Pre or Freq_Post was empty.

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
    - Fixed bug on Summary_CellRelation that was creating pairs of frequencies where Freq_Pre or Freq_Post was empty.

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
