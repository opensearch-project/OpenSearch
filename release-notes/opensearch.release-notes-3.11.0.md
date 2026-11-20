## Version 3.11.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.11.0

### Breaking Changes
* Default blob-write methods now preserve writes without metadata or crypto settings and throw an `IOException` when they cannot honor requested metadata or `CryptoMetadata`, instead of silently discarding those options. Provider-specific overrides retain their existing behavior.
