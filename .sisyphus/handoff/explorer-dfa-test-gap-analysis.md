# DFA Test-Coverage Gap Analysis

## Section 1 — Lucene Test Landscape Inventory

### (a) IndexShard unit — lifecycle, recovery finalize, snapshot, replication finalize

| Test Method | Purpose |
|---|---|
| `IndexShardTests#testRecoverFromStore` | Full store-based recovery lifecycle |
| `IndexShardTests#testSnapshotStore` | Snapshot acquire/release on shard |
| `IndexShardTests#testRecoveryFailsAfterMovingToRelocatedState` | Recovery finalize edge case |
| `SegmentReplicationIndexShardTests#testSegmentInfosAndReplicationCheckpointTuple` | SegmentInfos+checkpoint consistency on finalize |
| `SegmentReplicationIndexShardTests#testCleanupRedundantPendingMergeSegment` | Cleanup of pending merge entries via referenced-segments |

### (b) Segment-replication component unit — target/source dispatch, file copy, merge publisher

| Test Method | Purpose |
|---|---|
| `SegmentReplicationTargetServiceTests#testsSuccessfulReplication_listenerCompletes` | Target service dispatches replication to completion |
| `SegmentReplicationTargetServiceTests#testsSuccessfulMergeSegmentReplication_listenerCompletes` | Pre-copy merge target dispatch |
| `SegmentReplicationTargetTests#testFailure_finalizeReplication_NonCorruptionException` | Target handler finalize error path |
| `SegmentReplicationSourceHandlerTests#testSendFiles` | Source handler file serve |
| `MergedSegmentReplicationTargetTests#testSuccessfulResponseStartReplication_segRep` | Merged-segment target file copy |
| `PublishMergedSegmentActionTests#testPublishMergedSegment` | Pre-copy merge publisher action |
| `PublishReferencedSegmentsActionTests#testPublishReferencedSegmentsActionOnReplica` | Referenced-segments cleanup on replica |

### (c) Peer recovery unit — source handler phases, target file receive, translog

| Test Method | Purpose |
|---|---|
| `LocalStorePeerRecoverySourceHandlerTests#testSendFiles` | Phase-1 file send |
| `LocalStorePeerRecoverySourceHandlerTests#testSendSnapshotSendsOps` | Phase-2 translog ops |
| `RecoveryTests#testPeerRecoverySendSafeCommitInFileBased` | File-based recovery sends safe commit |
| `RecoveryTests#testDifferentHistoryUUIDDisablesOPsRecovery` | History UUID mismatch forces file-based |
| `RecoveryTests#testRecoveryTrimsLocalTranslog` | Translog trim post-recovery |

### (d) Segment-replication integration — full copy, relocation, pressure, failover

| Test Method | Purpose |
|---|---|
| `SegmentReplicationIT#testReplicationAfterPrimaryRefreshAndFlush` | Full primary→replica copy |
| `SegmentReplicationRelocationIT#testPrimaryRelocation` | Primary relocation with segrep |
| `SegmentReplicationPressureIT#testWritesRejected` | Backpressure when replica lags |
| `SegmentReplicationIT#testDropPrimaryDuringReplication` | Failover mid-replication |
| `SegmentReplicationDisruptionIT#testSendCorruptBytesToReplica` | Corruption handling |
| `SegmentReplicationPrimaryPromotionIT#testPrimaryStopped_ReplicaPromoted_no_data_loss` | Failover data integrity |

### (e) Remote-store recovery integration

| Test Method | Purpose |
|---|---|
| `RemoteIndexRecoveryIT#testUsesFileBasedRecoveryIfRetentionLeaseMissing` | Remote recovery fallback |
| `RemoteIndexRecoveryIT#testRecoverLocallyUpToGlobalCheckpoint` | Local recovery from remote |
| `SegmentReplicationUsingRemoteStoreIT` (class) | Segrep with remote store backend |
| `RemoteStoreRestoreIT` (class) | Restore from remote after failure |

### (f) Merge pre-copy integration

| Test Method | Purpose |
|---|---|
| `MergedSegmentWarmerIT#testMergeSegmentWarmer` | Primary publishes merged segment, replica pre-copies |
| `MergedSegmentWarmerIT#testConcurrentMergeSegmentWarmer` | Concurrent merges with pre-copy |
| `MergedSegmentWarmerIT#testCleanupRedundantPendingMergeFile` | Stale pre-copy cleanup |
| `RemoteStoreMergedSegmentWarmerIT#testMergeSegmentWarmerRemote` | Pre-copy with remote store |
| `SegmentReplicationIndexShardTests#testMergedSegmentReplication` | Unit-level merge replication |

---

## Section 2 — DFA Test Coverage Today

### (a) IndexShard unit / Engine lifecycle
| Class | Methods | Summary |
|---|---|---|
| `DataFormatAwareEngineTests` | 67 | Covers: index, refresh, flush, close, concurrent ops, catalog snapshot production, commit data, acquire reader, translog, seq-no stats, safe-commit. **Strong coverage of primary engine lifecycle.** |
| `DataFormatAwareNRTReplicationEngineTests` | 14 | Covers: create, updateCatalogSnapshot (generation bump, flush trigger), replica write stubs, close, fail. **Covers replica engine basics.** |
| `CatalogSnapshotManagerTests` | 22 | Covers: commit, ref-counting, merge-apply, replica apply-replication-snapshot, file deletion protection. **Good snapshot management coverage.** |
| `DataformatAwareCatalogSnapshotTests` | 30+ | Covers: serialization round-trip, ref-counting, clone, getUploadFileNames, getSegmentInfosBytes. |
| `CombinedCatalogSnapshotDeletionPolicyTests` | 19 | Covers: safe/last commit, snapshot protection, doc-count. |
| `SegmentInfosCatalogSnapshotTests` | 8 | Covers: delegation, clone, serialization. |

### (b) Segment-replication component unit
| Class | Methods | Summary |
|---|---|---|
| — | 0 | **NO DFA-specific unit tests for SegmentReplicationTarget, SegmentReplicationTargetService, SegmentReplicationSourceHandler, or MergedSegmentReplicationTarget with DFA payloads.** |

### (c) Peer recovery unit
| Class | Methods | Summary |
|---|---|---|
| — | 0 | **NO DFA-specific peer recovery source/target handler tests.** |

### (d) Segment-replication integration
| Class | Methods | Summary |
|---|---|---|
| `DataFormatAwareReplicationIT` | 5 | Covers: basic replication convergence, multiple refresh cycles, replica recovery with remote store, replica startup cleans orphan files, multiple replicas. |

### (e) Remote-store recovery integration
| Class | Methods | Summary |
|---|---|---|
| `DataFormatAwareRemoteStoreRecoveryIT` | 10 | Covers: recovery preserves format metadata, multiple flush generations, translog-only recovery, primary restart, primary relocation (with multiple generations, then new writes), replica relocation, pure-Lucene backward compat. |
| `DataFormatAwareUploadIT` | 4 | Covers: upload consistency after flush, version advances, upload after refresh only, mixed refresh+flush. |

### (f) Merge pre-copy integration
| Class | Methods | Summary |
|---|---|---|
| — | 0 | **NO DFA-specific merge pre-copy tests.** No `DataFormatAwareMergeCheckpointPublisherTests` exists. |

### Supporting DFA tests (store/directory/checksum)
| Class | Methods | Summary |
|---|---|---|
| `DataFormatAwareStoreDirectoryTests` | 55+ | Comprehensive: file identity, round-trip, path mapping, checksum, rename, multi-format isolation. |
| `DataFormatAwareRemoteDirectoryTests` | 50+ | Remote directory: blob routing, delete, open-input, async copy, rate-limiter, format cache. |
| `DefaultDataFormatAwareStoreDirectoryFactoryTests` | 5 | Factory creation. |
| `FileMetadataTests` | 20+ | Serialize/deserialize round-trip, equality, delimiter handling. |
| `FormatChecksumStrategySharingTests` | 6 | Checksum strategy instance sharing across indices. |
| `DataFormatAwareMergePolicyTests` | 13 | Merge candidate finding, context tracking, concurrent safety. |

---

## Section 3 — Gap Analysis

### (b) Segment-replication component unit — CRITICAL GAP
**Why it matters for DFA:** The replication target receives `FileMetadata`-serialized file names and must route them to the correct `DataFormatAwareStoreDirectory` sub-directory. `SegmentReplicationTarget#finalizeReplication` must handle `CatalogSnapshot` (not just `SegmentInfos`). The source handler must serve files from format-specific directories. None of this is unit-tested with DFA payloads.
- **Risk:** `FileMetadata.serialize()` mismatch between primary publish and replica receive causes files to land in wrong directory or NPE on checksum lookup.

### (c) Peer recovery unit — MODERATE GAP
**Why it matters:** Peer recovery sends a safe commit. For DFA, the safe commit includes catalog-snapshot metadata in commit user-data. The target must reconstruct `DataFormatAwareStoreDirectory` from received files. No unit test validates this path.
- **Risk:** Recovery after network partition sends Lucene-only commit data, losing DFA format metadata.

### (f) Merge pre-copy — CRITICAL GAP
**Why it matters:** `DataFormatAwareMergeCheckpointPublisher` uses `Segment.replicationCheckpointName()` to identify merged segments. The replica's `cleanupPendingMergedSegments` and `cleanupRedundantPendingMergeSegment` must match on this name. No test validates the DFA name-identity contract end-to-end.
- **Risk:** Generation-based naming (`gen3_seg0`) diverges between publisher and cleanup, leaving stale files on replica.

### (a) IndexShard unit — MODERATE GAP
**Why it matters:** `IndexShard.computeReferencedSegmentsCheckpoint()` iterates `CatalogSnapshot.getSegments()` calling `replicationCheckpointName()`. The DFA branch produces different names than Lucene. Only one grep hit shows this tested indirectly via `SegmentReplicationIndexShardTests#testCleanupRedundantPendingMergeSegment` which uses Lucene engine.
- **Risk:** `computeReferencedSegmentsCheckpoint` with DFA segments returns wrong names → premature cleanup of valid pre-copied files.

### (d) Segment-replication integration — MODERATE GAP
Existing `DataFormatAwareReplicationIT` covers happy-path but lacks: relocation during replication, backpressure, close-while-replicating, merge-during-replication, failover scenarios.
- **Risk:** DFA-specific file identity issues only manifest under concurrent/failure conditions.

### (e) Remote-store recovery — LOW GAP
`DataFormatAwareRemoteStoreRecoveryIT` is relatively comprehensive. Missing: recovery after primary failover with in-flight merge pre-copy, and `bumpGenerationForNewEngineLifecycle` collision test.

---

## Section 4 — Prioritized Test Recommendations

| # | Pri | Cat | Class | Method | Purpose & Assertions | SLOC | Infra |
|---|---|---|---|---|---|---|---|
| 1 | P0 | f | `DataFormatAwareMergeCheckpointPublisherTests` | `testPublishMergedSegmentUsesReplicationCheckpointName` | Verify publisher sends segment name matching `Segment.replicationCheckpointName()`. Assert request contains correct generation-based name. | ~60 | Mock `IndexShard`, mock transport |
| 2 | P0 | f | `DataFormatAwareMergeCheckpointPublisherTests` | `testCleanupPendingMergedSegmentsMatchesPublishedName` | Primary publishes merge → replica receives → `cleanupPendingMergedSegments` removes correct entry. Assert pending map empty after cleanup. | ~80 | `DataFormatAwareEngineTestCase` base |
| 3 | P0 | b | `DataFormatAwareSegmentReplicationTargetTests` | `testFinalizeReplicationWithCatalogSnapshot` | Target receives DFA checkpoint, finalizes with `CatalogSnapshot`. Assert files land in correct format-specific directories. | ~90 | Mock source, `DataFormatAwareStoreDirectory` fixture |
| 4 | P0 | a | `DataFormatAwareEngineTests` | `testComputeReferencedSegmentsCheckpointDFABranch` | After refresh+merge, call `computeReferencedSegmentsCheckpoint`. Assert returned names match `replicationCheckpointName()` for each DFA segment. | ~50 | Existing test infra |
| 5 | P0 | b | `DataFormatAwareSegmentReplicationTargetTests` | `testFileMetadataDeserializesToCorrectDirectory` | Replica receives serialized `FileMetadata` strings, deserializes, writes to `DataFormatAwareStoreDirectory`. Assert parquet files in parquet subdir, lucene in index dir. | ~70 | `DataFormatAwareStoreDirectory` fixture |
| 6 | P0 | f | `DataFormatAwareReplicationIT` | `testMergePreCopyWithDFAEngine` | Primary merges → publishes pre-copy → replica receives merged segment → regular segrep finalizes. Assert replica has merged files, no stale pending entries. | ~100 | Composite-engine IT base (extensible) |
| 7 | P0 | a | `DataFormatAwareNRTReplicationEngineTests` | `testBumpGenerationForNewEngineLifecycleNoCollision` | Open engine, note generation. Close. Re-open (simulating relocation). Assert new generation > old, no file collision. | ~40 | Existing test infra |
| 8 | P1 | f | `DataFormatAwareReplicationIT` | `testCleanupRedundantPendingMergeSegmentDFA` | Primary publishes referenced-segments → replica cleans stale pre-copy entries. Assert only active merge files remain. | ~80 | Composite-engine IT base |
| 9 | P1 | b | `DataFormatAwareSegmentReplicationSourceHandlerTests` | `testSendFilesServesFromFormatSpecificDirectory` | Source handler serves files from DFA directory. Assert parquet files read from parquet subdir. | ~60 | Mock `DataFormatAwareStoreDirectory` |
| 10 | P1 | d | `DataFormatAwareReplicationIT` | `testPrimaryRelocationDuringReplication` | Relocate primary while replica is mid-replication. Assert replica converges after relocation completes. | ~90 | Composite-engine IT base |
| 11 | P1 | d | `DataFormatAwareReplicationIT` | `testFailoverMidReplication` | Kill primary during active replication. Promote replica. Assert no data loss, generation bumps correctly. | ~90 | Composite-engine IT base |
| 12 | P1 | e | `DataFormatAwareRemoteStoreRecoveryIT` | `testRecoveryAfterPrimaryFailoverWithInFlightMerge` | Primary has in-flight merge pre-copy, fails. New primary recovers from remote. Assert no stale merge artifacts. | ~80 | Existing IT base |
| 13 | P1 | a | `DataFormatAwareEngineTests` | `testFlushDoesNotBumpGenerationWhenWritersEmpty` | Refresh (no new docs) → flush. Assert catalog generation unchanged. Diverges from Lucene commit semantics. | ~40 | Existing test infra |
| 14 | P1 | b | `DataFormatAwareStoreDirectoryTests` | `testChecksumStrategyRegistrationNPEProtection` | Create directory without registering checksum strategy for a format. Attempt checksum calculation. Assert meaningful error (not NPE). | ~30 | Existing test infra |
| 15 | P1 | d | `DataFormatAwareReplicationIT` | `testBackpressureWithDFAEngine` | Index rapidly, slow replica. Assert backpressure rejects writes when replica falls behind. | ~70 | Composite-engine IT base |
| 16 | P1 | c | `RecoveryTests` | **PARAMETERIZE** existing `testPeerRecoverySendSafeCommitInFileBased` | Run with DFA engine. Assert commit user-data contains catalog-snapshot keys. | ~20 (param) | Parameterized test fixture |
| 17 | P2 | d | `DataFormatAwareReplicationIT` | `testCloseWhileReplicating` | Close index while replication in progress. Assert clean shutdown, no leaked files. | ~60 | Composite-engine IT base |
| 18 | P2 | b | `SegmentReplicationTargetServiceTests` | **PARAMETERIZE** `testsSuccessfulReplication_listenerCompletes` | Run with DFA checkpoint type. Assert target service handles CatalogSnapshot-based checkpoint. | ~20 (param) | Mock DFA engine |
| 19 | P2 | e | `DataFormatAwareRemoteStoreRecoveryIT` | `testBumpGenerationCollisionAfterRelocation` | Relocate primary twice rapidly. Assert no generation collision in catalog snapshots. | ~70 | Existing IT base |
| 20 | P2 | a | `SegmentReplicationIndexShardTests` | **PARAMETERIZE** `testCleanupRedundantPendingMergeSegment` | Run with DFA engine producing DFA-named segments. Assert cleanup works with generation-based names. | ~20 (param) | DFA engine factory in test |
| 21 | P2 | f | `DataFormatAwareReplicationIT` | `testConcurrentMergesWithPreCopy` | Multiple concurrent merges on primary. Assert all pre-copies arrive, no name collision on replica. | ~90 | Composite-engine IT base |
| 22 | P2 | b | `DataFormatAwareSegmentReplicationTargetTests` | `testStaleCheckpointRejectedWithDFACheckpoint` | Send stale DFA checkpoint to target. Assert rejection with correct error. | ~50 | Mock source |
| 23 | P2 | c | `DataFormatAwareRemoteStoreRecoveryIT` | `testPeerRecoveryPreservesCatalogSnapshotMetadata` | Peer recovery (non-remote) with DFA. Assert recovered shard has valid catalog snapshot in commit data. | ~70 | Composite-engine IT base |

**PARAMETERIZATION candidates (marked above):** #16, #18, #20 — these Lucene tests are generic enough that adding a DFA engine parameter covers the gap without new test classes.

---

## Section 5 — Test Infrastructure Recommendations

| # | Item | Purpose | Effort |
|---|---|---|---|
| 1 | `DataFormatAwareEngineTestCase` shared base | Provides DFA engine setup (mock DataFormat, CatalogSnapshotManager, StoreDirectory) for unit tests. Reuse across `DataFormatAwareSegmentReplicationTargetTests`, `DataFormatAwareMergeCheckpointPublisherTests`. | Medium |
| 2 | `DataFormatAwareReplicationBaseIT` base class | Extends `SegmentReplicationBaseIT` with composite-engine plugin loading, DFA index settings, format-aware assertions (verify files in correct subdirs). All DFA ITs inherit from this. | Medium |
| 3 | `MockDataFormat` test fixture | Already exists as `MockCatalogSnapshot` in test-framework. Extend to full `MockDataFormat` that produces predictable file names for assertion. Register in test `DataFormatRegistry`. | Low |
| 4 | Parameterized engine factory for existing segrep tests | Add `@ParametersFactory` to `SegmentReplicationIndexShardTests` that runs selected tests with both `InternalEngine` and `DataFormatAwareEngine`. Covers gaps #16, #18, #20 without new test classes. | Medium |
| 5 | `DFAReplicationAssertions` utility | Helper methods: `assertFilesInCorrectFormatDirs()`, `assertCatalogSnapshotConsistent()`, `assertNoStalePreCopyFiles()`, `assertGenerationMonotonic()`. Used across all DFA tests. | Low |
