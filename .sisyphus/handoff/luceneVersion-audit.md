# Complete Audit of `luceneVersion` References in OpenSearch

Total occurrences: 130 (across .java files)
Non-test: ~55 | Test: ~75

---

## A. WRITE PATH (IndexWriter, segment creation, store creation)

| # | File | Line | What it does |
|---|------|------|-------------|
| 1 | `server/.../index/engine/NativeLuceneIndexWriterFactory.java` | 143 | Sets `iwc.setIndexCreatedVersionMajor(…luceneVersion.major)` when creating IndexWriterConfig for native Lucene engine |
| 2 | `server/.../index/engine/DataFormatAwareNRTReplicationEngine.java` | 187 | Calls `store.createEmpty(…luceneVersion, translogUUIDFromHeader)` to create empty store for NRT replication |
| 3 | `server/.../index/store/Store.java` | 1976 | `createEmpty(Version luceneVersion, String translogUUID)` — creates empty index with IndexWriter using luceneVersion |
| 4 | `server/.../index/store/Store.java` | 1978 | Passes luceneVersion to `newEmptyIndexWriter(directory, luceneVersion)` |
| 5 | `server/.../index/store/Store.java` | 1996 | `createEmpty(Version luceneVersion)` — overload that delegates to createEmpty(luceneVersion, null) |
| 6 | `server/.../index/store/Store.java` | 1997 | Delegates to createEmpty with null translogUUID |
| 7 | `server/.../index/store/Store.java` | 2159 | `newEmptyIndexWriter(dir, luceneVersion)` — creates IndexWriterConfig with `setIndexCreatedVersionMajor(luceneVersion.major)` |
| 8 | `server/.../index/store/Store.java` | 2161 | Actually sets the version major on IndexWriterConfig |
| 9 | `server/.../index/shard/StoreRecovery.java` | 677 | `store.createEmpty(…luceneVersion, translogUUID)` during recovery from remote translog |
| 10 | `server/.../index/shard/StoreRecovery.java` | 757 | `store.createEmpty(…luceneVersion)` during recovery (empty shard) |
| 11 | `server/.../index/shard/StoreRecovery.java` | 851 | `store.createEmpty(…luceneVersion)` during bootstrap from snapshot |

## B. READ PATH (segment reading, compatibility checks for reading)

| # | File | Line | What it does |
|---|------|------|-------------|
| 1 | `server/.../common/lucene/Lucene.java` | 148 | `minimumVersion.minimumIndexCompatibilityVersion().luceneVersion.major` — determines min supported Lucene major for reading SegmentInfos from older indices |
| 2 | `server/.../index/store/Store.java` | 1265 | Fallback `maxVersion` for metadata checksumming when no segments found: `CURRENT.minimumIndexCompatibilityVersion().luceneVersion` |
| 3 | `server/.../index/store/Store.java` | 1325 | Same fallback for catalog-based metadata path |
| 4 | `modules/store-subdirectory/.../SubdirectoryAwareStore.java` | 237 | Uses `CURRENT.minimumIndexCompatibilityVersion().luceneVersion` as version for StoreFileMetadata when computing file metadata |

## C. MERGE/UPGRADE PATH (merge policy, upgrade status)

| # | File | Line | What it does |
|---|------|------|-------------|
| 1 | `server/.../index/shard/OpenSearchMergePolicy.java` | 86 | `Version.CURRENT.luceneVersion` — compares current Lucene version against segment version to decide if segment needs upgrade merge |
| 2 | `server/.../action/admin/indices/upgrade/get/TransportUpgradeStatusAction.java` | 147 | Compares `seg.version.major` against `Version.CURRENT.luceneVersion.major` to count segments needing upgrade |
| 3 | `server/.../action/admin/indices/upgrade/get/TransportUpgradeStatusAction.java` | 150 | Compares `seg.version.minor` against `Version.CURRENT.luceneVersion.minor` for minor-version upgrade detection |
| 4 | `server/.../action/admin/indices/upgrade/post/TransportUpgradeAction.java` | 125 | Local variable `luceneVersion` extracted from version tuple during upgrade action |
| 5 | `server/.../action/admin/indices/upgrade/post/TransportUpgradeAction.java` | 134 | Updates `luceneVersion` to `result.oldestLuceneSegment()` after upgrade |
| 6 | `server/.../action/admin/indices/upgrade/post/TransportUpgradeAction.java` | 136 | Stores `(version, luceneVersion)` tuple in versions map |

## D. COMPATIBILITY GATING (join checks, metadata upgrade, node metadata)

| # | File | Line | What it does |
|---|------|------|-------------|
| 1 | `server/.../index/shard/IndexShard.java` | 1769-1775 | `minimumCompatibleVersion()` — iterates segments to find oldest Lucene version; falls back to `indexSettings.getIndexVersionCreated().luceneVersion` if no segments |
| 2 | `libs/core/.../Version.java` | 272-281 | `fromId()` — constructs Version with computed luceneVersion; for unknown versions, derives luceneVersion from known version list (major-1 for oldest, or exact match) |
| 3 | `libs/core/.../Version.java` | 235-238 | Static assertion: `CURRENT.luceneVersion` must equal `org.apache.lucene.util.Version.LATEST` |
| 4 | `libs/core/.../Version.java` | 366 | Field declaration: `public final org.apache.lucene.util.Version luceneVersion` |
| 5 | `libs/core/.../Version.java` | 368-381 | Constructor: `Version(int id, org.apache.lucene.util.Version luceneVersion)` — stores luceneVersion |
| 6 | `server/.../bootstrap/Bootstrap.java` | 484 | Startup check: verifies `Version.CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST)` |
| 7 | `server/.../bootstrap/Bootstrap.java` | 487 | Error message includes `Version.CURRENT.luceneVersion` if check fails |

## E. FEATURE BEHAVIOR (similarity, analysis, caching)

| # | File | Line | What it does |
|---|------|------|-------------|
| 1 | `server/.../index/similarity/SimilarityService.java` | 225 | `indexCreatedVersion.luceneVersion.major` — passed to `FieldInvertState` constructor for similarity score validation (positive scores) |
| 2 | `server/.../index/similarity/SimilarityService.java` | 250 | Same — for "scores don't decrease with freq" validation |
| 3 | `server/.../index/similarity/SimilarityService.java` | 286 | Same — for "scores don't increase with norm" validation |
| 4 | `server/.../index/analysis/PreConfiguredCharFilter.java` | 81 | Factory method `luceneVersion(...)` — creates a PreConfiguredCharFilter cached by Lucene version |
| 5 | `server/.../index/analysis/PreConfiguredCharFilter.java` | 90 | Lambda: `(reader, version) -> create.apply(reader, version.luceneVersion)` — extracts luceneVersion from OpenSearch Version for char filter creation |
| 6 | `server/.../index/analysis/PreConfiguredTokenizer.java` | 65 | Factory method `luceneVersion(...)` — creates PreConfiguredTokenizer cached by Lucene version |
| 7 | `server/.../index/analysis/PreConfiguredTokenizer.java` | 66 | Lambda: `version -> create.apply(version.luceneVersion)` — extracts luceneVersion for tokenizer creation |
| 8 | `server/.../index/analysis/PreConfiguredTokenFilter.java` | 88 | Factory method `luceneVersion(...)` — creates PreConfiguredTokenFilter cached by Lucene version |
| 9 | `server/.../index/analysis/PreConfiguredTokenFilter.java` | 98 | Lambda: `(tokenStream, version) -> create.apply(tokenStream, version.luceneVersion)` — extracts luceneVersion for token filter creation |
| 10 | `server/.../indices/analysis/PreBuiltCacheFactory.java` | 153 | `version.luceneVersion` — used as cache key to retrieve cached analysis model by Lucene version |
| 11 | `server/.../indices/analysis/PreBuiltCacheFactory.java` | 158 | `version.luceneVersion` — used as cache key to store analysis model by Lucene version |

## F. INFORMATIONAL/DISPLAY (REST API, toString, logging)

| # | File | Line | What it does |
|---|------|------|-------------|
| 1 | `server/.../action/main/MainResponse.java` | 125 | `version.luceneVersion.toString()` — serialized as `lucene_version` in REST `GET /` response |
| 2 | `client/rest-high-level/.../client/core/MainResponse.java` | 141 | Field declaration: `private final String luceneVersion` in client-side VersionInfo |
| 3 | `client/rest-high-level/.../client/core/MainResponse.java` | 151 | Constructor parameter for luceneVersion |
| 4 | `client/rest-high-level/.../client/core/MainResponse.java` | 160 | Assignment: `this.luceneVersion = luceneVersion` |
| 5 | `client/rest-high-level/.../client/core/MainResponse.java` | 186 | Getter: `return luceneVersion` |
| 6 | `client/rest-high-level/.../client/core/MainResponse.java` | 207 | Equality check: `luceneVersion.equals(version.luceneVersion)` |
| 7 | `client/rest-high-level/.../client/core/MainResponse.java` | 220 | Included in hashCode computation |

## G. OTHER NON-TEST

| # | File | Line | What it does |
|---|------|------|-------------|
| 1 | `server/.../common/settings/Settings.java` | 870-874 | `put(String key, org.apache.lucene.util.Version luceneVersion)` — convenience method to put a Lucene version into Settings (parameter named luceneVersion) |
| 2 | `server/.../common/lucene/search/XMoreLikeThis.java` | 177 | Commented-out assertion: `// assert Version.CURRENT.luceneVersion == ...` — dead code |
| 3 | `buildSrc/.../gradle/RepositoriesSetupPlugin.java` | 87-92 | Gradle build: parses `luceneVersion` string from VersionProperties to configure snapshot repository URLs (NOT the Version.luceneVersion field — this is build tooling) |

## G. TEST FILES (summary — 75 occurrences)

Key test files using `luceneVersion`:
- `VersionTests.java` (lines 123, 128, 316-318, 385, 391-392) — validates luceneVersion consistency
- `AnalysisModuleTests.java` (lines 227-431) — tests PreConfigured analysis components with luceneVersion caching
- `InternalEngineTests.java` (lines 1198, 3910, 4117, 6442, 7591) — store.createEmpty with luceneVersion
- `ReadOnlyEngineTests.java` (lines 224, 261) — store.createEmpty
- `EngineTestCase.java` (lines 724, 763) — test framework store.createEmpty
- `IndexShardTests.java` (lines 1686-1690) — minimumCompatibleVersion assertions
- `FullClusterRestartIT.java` (lines 765-767) — upgrade compatibility checks
- `VerifyVersionConstantsIT.java` (lines 54-56) — REST API version verification
- `MainResponseTests.java` (line 123) — REST response serialization test
- `StoreTests.java` (line 142), `RecoveryStatusTests.java` (line 49), `ReplicaShardAllocatorTests.java` (line 93), `ReplicaShardBatchAllocatorTests.java` (line 76), `FileInfoTests.java` (line 56) — min compat version for test constants
- `SegmentReplicationSourceHandlerTests.java` (lines 196, 224) — StoreFileMetadata construction
- `MergedSegmentWarmerTests.java` (lines 163-164) — version comparison in tests
- `VersionsTests.java` (lines 219-227) — luceneVersion field validation
- `IngestionEngineTests.java` (lines 245, 280) — store.createEmpty
- `NRTReplicationEngineTests.java` (line 438) — store.createEmpty
- `LeafSorterOptimizationTests.java` (lines 44, 165, 242) — store.createEmpty
- `RefreshListenersTests.java` (line 146) — store.createEmpty
- `PreConfiguredTokenFilterTests.java` (lines 123, 126) — luceneVersion factory test
- `MiscellaneousDocumentationIT.java` (lines 64, 76) — REST client doc test
- `MainResponseTests.java` (client, line 77) — client response test
- `RestHighLevelClientTests.java` (line 194) — field name reference
