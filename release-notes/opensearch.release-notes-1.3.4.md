## Version 1.3.4 Release Notes

### Upgrades
* Upgrade Jackson-databind to 2.13.2 resolving [CVE-2020-36518] ([#3777](https://github.com/opensearch-project/OpenSearch/pull/3777))
* Upgrade netty to 4.1.79.Final resolving [CVE-2022-24823] ([#3875](https://github.com/opensearch-project/OpenSearch/pull/3875))
* Upgrade tukaani:xz from 1.9 in plugins/ingest-attachment ([#3802](https://github.com/opensearch-project/OpenSearch/pull/3802))
* Upgrade Tika (2.1.0), xmlbeans (3.1.0), Apache Commons-IO (2.11.0), and PDFBox (2.0.25) in plugins/ingest-attachment ([#3794](https://github.com/opensearch-project/OpenSearch/pull/3794))

### Bug Fixes
* Fix bug where opensearch crashes on closed client connection before search reply.  ([#3655](https://github.com/opensearch-project/OpenSearch/pull/3655))
