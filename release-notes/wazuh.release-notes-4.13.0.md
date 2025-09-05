## 2025-09-11 Version 4.13.0 Release Notes

## [4.13.0]
### Added
- Add index templates for FIM and Inventory [(#744)](https://github.com/wazuh/wazuh-indexer/pull/744)
- Automated packages smoke tests [(#765)](https://github.com/wazuh/wazuh-indexer/pull/765)
- Implement version bumper script [(#802)](https://github.com/wazuh/wazuh-indexer/pull/802) [(#803)](https://github.com/wazuh/wazuh-indexer/pull/803)
- Add workflow for version bumping [(#910)](https://github.com/wazuh/wazuh-indexer/pull/910)

### Dependencies
-

### Changed
- Improve allocator usage [(#957)](https://github.com/wazuh/wazuh-indexer/pull/957)
- Add missing globalquery fields [(#771)](https://github.com/wazuh/wazuh-indexer/pull/771)
- Change `registry.mtime` to date [(#780)](https://github.com/wazuh/wazuh-indexer/pull/780)
- Infer previous version on packages smoke tests [(#820)](https://github.com/wazuh/wazuh-indexer/pull/820)
- Update setup-gradle version [(#831)](https://github.com/wazuh/wazuh-indexer/pull/831)
- Update `ecs/generate.sh` to remove multi-fields [(#833)](https://github.com/wazuh/wazuh-indexer/pull/833)
- Remove @timestamp from inventory index templates [(#839)](https://github.com/wazuh/wazuh-indexer/pull/839)
- Fine tune ECS templates [(#850)](https://github.com/wazuh/wazuh-indexer/pull/850)
- Reorganize ecs folder [(#899)](https://github.com/wazuh/wazuh-indexer/pull/899)
- Enhance FIM and Inventory indices settings [(#939)](https://github.com/wazuh/wazuh-indexer/pull/939)
- Change date format for the repository bumper[(#944)](https://github.com/wazuh/wazuh-indexer/pull/944)
- Update `host.cpu.cores` field type to `short` [(#945)](https://github.com/wazuh/wazuh-indexer/pull/945)
- Update inventory interfaces index template [(#952)](https://github.com/wazuh/wazuh-indexer/pull/952)
- Update refresh interval to 2 seconds [(#1066)](https://github.com/wazuh/wazuh-indexer/pull/1066)

### Deprecated
-

### Removed
-

### Fixed
- Set corresponding permissions to performance analyzer binary [(#997)](https://github.com/wazuh/wazuh-indexer/pull/997)

### Security
- 
