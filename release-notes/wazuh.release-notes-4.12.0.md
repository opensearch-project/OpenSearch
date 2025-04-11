## 2025-04-30 Version 4.12.0 Release Notes

## [4.12.0]
### Added
- Add `scanner.condition` custom field to vulnerability detector index definition ([#637](https://github.com/wazuh/wazuh-indexer/pull/637))
- Enable assembly of ARM packages [(#444)](https://github.com/wazuh/wazuh-indexer/pull/444)

### Dependencies

### Changed
- Version file standarization [[#693]](https://github.com/wazuh/wazuh-indexer/pull/693)

### Deprecated

### Removed
- Remove unused GitHub Workflows [(#762)](https://github.com/wazuh/wazuh-indexer/pull/762)

### Fixed
- Fix startup errors on STIG compliant systems due to noexec filesystems [(#533)](https://github.com/wazuh/wazuh-indexer/pull/533)
- Fix CI Docker environment [(#760)](https://github.com/wazuh/wazuh-indexer/pull/760)

### Security
- Migration to OpenSearch 2.19.0 (JDK 21 and Gradle 8.2) [(#702)](https://github.com/wazuh/wazuh-indexer/pull/702)
- Migration to OpenSearch 2.19.1 [(#739)](https://github.com/wazuh/wazuh-indexer/pull/739)