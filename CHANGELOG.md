# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add support for dependencies in plugin descriptor properties with semver range ([#11441](https://github.com/opensearch-project/OpenSearch/pull/11441))
- Add community_id ingest processor ([#12121](https://github.com/opensearch-project/OpenSearch/pull/12121))
- Introduce query level setting `index.query.max_nested_depth` limiting nested queries ([#3268](https://github.com/opensearch-project/OpenSearch/issues/3268)

### Dependencies
- Bumps jetty version to 9.4.52.v20230823 to fix GMS-2023-1857 ([#9822](https://github.com/opensearch-project/OpenSearch/pull/9822))
- Bump `netty` from 4.1.99.Final to 4.1.100.Final ([#10564](https://github.com/opensearch-project/OpenSearch/pull/10564))
- Bump Lucene from 9.7.0 to 9.8.0 ([10276](https://github.com/opensearch-project/OpenSearch/pull/10276))
- Bump `commons-io:commons-io` from 2.13.0 to 2.14.0 ([#10294](https://github.com/opensearch-project/OpenSearch/pull/10294))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.25.0 to 2.25.1 ([#10298](https://github.com/opensearch-project/OpenSearch/pull/10298))
- Bump `de.thetaphi:forbiddenapis` from 3.5.1 to 3.6 ([#10508](https://github.com/opensearch-project/OpenSearch/pull/10508))
- Bump OpenTelemetry from 1.30.1 to 1.31.0 ([#10617](https://github.com/opensearch-project/OpenSearch/pull/10617))
- Bump `org.codehaus.woodstox:stax2-api` from 4.2.1 to 4.2.2 ([#10639](https://github.com/opensearch-project/OpenSearch/pull/10639))
- Bump `org.bouncycastle:bc-fips` from 1.0.2.3 to 1.0.2.4 ([#10297](https://github.com/opensearch-project/OpenSearch/pull/10297))
- Bump `org.apache.logging.log4j:log4j-core` from 2.20.0 to 2.21.1 ([#10858](https://github.com/opensearch-project/OpenSearch/pull/10858), [#11000](https://github.com/opensearch-project/OpenSearch/pull/11000))
- Bump `aws-actions/configure-aws-credentials` from 2 to 4 ([#10504](https://github.com/opensearch-project/OpenSearch/pull/10504))
- Bump `com.squareup.okio:okio` from 3.7.0 to 3.8.0 ([#12290](https://github.com/opensearch-project/OpenSearch/pull/12290))
- Bump `org.bouncycastle:bcprov-jdk15to18` to `org.bouncycastle:bcprov-jdk18on` version 1.77 ([#12317](https://github.com/opensearch-project/OpenSearch/pull/12317))
- Bump `org.bouncycastle:bcmail-jdk15to18` to `org.bouncycastle:bcmail-jdk18on` version 1.77 ([#12317](https://github.com/opensearch-project/OpenSearch/pull/12317))
- Bump `org.bouncycastle:bcpkix-jdk15to18` to `org.bouncycastle:bcpkix-jdk18on` version 1.77 ([#12317](https://github.com/opensearch-project/OpenSearch/pull/12317))

### Changed

### Deprecated

### Removed

### Fixed
- Add a system property to configure YamlParser codepoint limits ([#12298](https://github.com/opensearch-project/OpenSearch/pull/12298))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.12...2.x
