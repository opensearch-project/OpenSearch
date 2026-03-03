## 2025-05-07 Version 4.12.0 Release Notes

## [4.12.0]
### Added
- Add `scanner.condition` custom field to vulnerability detector index definition ([#637](https://github.com/wazuh/wazuh-indexer/pull/637))
- Enable assembly of ARM packages [(#444)](https://github.com/wazuh/wazuh-indexer/pull/444)
- Add `vulnerability.scanner.reference` field to VD and alerts indexes [(#689)](https://github.com/wazuh/wazuh-indexer/pull/689)

### Dependencies

### Changed
- Version file standarization [(#693)](https://github.com/wazuh/wazuh-indexer/pull/693)
- Redesign the mechanism to preserve the status of the service on upgrades [(#794)](https://github.com/wazuh/wazuh-indexer/pull/794)

### Deprecated

### Removed
- Removed unused GitHub Workflows [(#762)](https://github.com/wazuh/wazuh-indexer/pull/762)

### Fixed
- Fix startup errors on STIG compliant systems due to noexec filesystems [(#533)](https://github.com/wazuh/wazuh-indexer/pull/533)
- Fix CI Docker environment [(#760)](https://github.com/wazuh/wazuh-indexer/pull/760)

### Security
- Migration to OpenSearch 2.19.0 (JDK 21 and Gradle 8.12) [(#702)](https://github.com/wazuh/wazuh-indexer/pull/702)
- Migration to OpenSearch 2.19.1 [(#739)](https://github.com/wazuh/wazuh-indexer/pull/739)
