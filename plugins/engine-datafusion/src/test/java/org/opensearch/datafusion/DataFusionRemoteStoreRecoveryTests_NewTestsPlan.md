# DataFusion Remote Store Recovery Tests - Implementation Plan

This document outlines the new tests to be added to `DataFusionRemoteStoreRecoveryTests.java` to make it comprehensive and extensive for optimized indices recovery flows.

## Current Test Coverage

The existing tests cover:
- `testDataFusionWithRemoteStoreRecovery` - Basic remote store recovery
- `testDataFusionRecoveryWithMultipleParquetGenerations` - Multiple generation files
- `testDataFusionReplicaPromotionToPrimary` - Replica promotion
- `testClusterRecoveryFromTranslogWithoutFlush` - Translog recovery
- `testReplicaPromotionWithTranslogReplay` - Replica promotion with translog
- `testDataFusionPrimaryRestartWithExtraCommits` - Primary restart scenarios

---

## Category 1: Snapshot/Restore Recovery Tests

### Test 1: `testDataFusionSnapshotRestore`

**Priority:** HIGH

**Description:** Tests that snapshot and restore operations preserve Parquet format metadata and CatalogSnapshot for optimized indices.

**Implementation Plan:**
```java
public void testDataFusionSnapshotRestore() throws Exception {
    // Setup
    // 1. Start cluster with cluster manager and data nodes
    // 2. Create snapshot repository
    // 3. Create optimized index with Parquet data format
    
    // Test Steps
    // 4. Index documents (10-50 docs)
    // 5. Flush and refresh to ensure Parquet files are created
    // 6. Validate format-aware metadata before snapshot
    // 7. Create snapshot of the index
    // 8. Delete the index
    // 9. Restore from snapshot
    // 10. Validate format-aware metadata after restore
    
    // Validations
    // - Document count matches before/after
    // - Parquet file count matches
    // - FileMetadata.dataFormat() returns "parquet" for all Parquet files
    // - CatalogSnapshot bytes are properly restored
    // - Search operations work correctly
}
```

**Key Assertions:**
- `assertEquals(docCountBefore, docCountAfter)`
- `validateRemoteStoreSegments()` - Parquet format preserved
- `validateCatalogSnapshot()` - CatalogSnapshot bytes valid
- Search query returns expected results

**Reference Implementation:** `RemoteRestoreSnapshotIT.testRestoreOperationsShallowCopyEnabled()`

---

### Test 2: `testDataFusionRestoreWithForceMerge`

**Priority:** MEDIUM

**Description:** Tests recovery after force merge operations to ensure merged Parquet files maintain format integrity.

**Implementation Plan:**
```java
public void testDataFusionRestoreWithForceMerge() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents in multiple batches (creates multiple Parquet files)
    // 4. Flush after each batch
    // 5. Execute force merge to single segment
    // 6. Validate merged Parquet file has correct format metadata
    // 7. Stop data node
    // 8. Start new data node
    // 9. Restore from remote store
    // 10. Validate merged file is recovered with format metadata
    
    // Validations
    // - Single merged Parquet file exists
    // - Format metadata preserved post-merge
    // - Document count correct
}
```

**Key Assertions:**
- Single segment file after merge
- `FileMetadata.dataFormat()` == "parquet" for merged file
- Document count unchanged

**Reference Implementation:** `RemoteStoreForceMergeIT.testRestoreWithMergeFlow()`

---

### Test 3: `testDataFusionShallowCopySnapshotRestore`

**Priority:** MEDIUM

**Description:** Tests shallow copy snapshot specifically for optimized indices to ensure format-aware metadata references are preserved.

**Implementation Plan:**
```java
public void testDataFusionShallowCopySnapshotRestore() throws Exception {
    // Setup
    // 1. Start cluster with remote store enabled
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents
    // 4. Flush and refresh
    // 5. Capture remote store file references
    // 6. Create shallow copy snapshot
    // 7. Verify snapshot metadata references remote store files
    // 8. Delete index
    // 9. Restore from shallow copy
    // 10. Verify restored index uses same remote store files
    
    // Validations
    // - Remote store file paths preserved
    // - No data copied during snapshot (shallow)
    // - Format metadata intact post-restore
}
```

**Key Assertions:**
- Snapshot is shallow (minimal data transfer)
- Remote store file paths match before/after
- Format metadata preserved

**Reference Implementation:** `RestoreShallowSnapshotV2IT.testRestoreShallowSnapshotRepository()`

---

## Category 2: Error/Failure Handling Tests

### Test 4: `testDataFusionRecoveryWithTransientErrors`

**Priority:** HIGH

**Description:** Tests that recovery correctly retries on transient failures while preserving Parquet format metadata.

**Implementation Plan:**
```java
public void testDataFusionRecoveryWithTransientErrors() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    // 3. Configure mock transport service
    
    // Test Steps
    // 4. Index documents
    // 5. Flush to create Parquet files
    // 6. Inject transient errors during recovery (using MockTransportService)
    //    - Block FILES_INFO, FILE_CHUNK, or CLEAN_FILES actions randomly
    //    - Throw OpenSearchRejectedExecutionException or CircuitBreakingException
    // 7. Start replica recovery
    // 8. Allow recovery to complete after few retries
    // 9. Validate format metadata consistency
    
    // Validations
    // - Recovery completes successfully after retries
    // - Format metadata preserved despite retries
    // - Document count correct on replica
}
```

**Key Components to Mock:**
- `PeerRecoveryTargetService.Actions.FILES_INFO`
- `PeerRecoveryTargetService.Actions.FILE_CHUNK`
- `PeerRecoveryTargetService.Actions.CLEAN_FILES`

**Key Assertions:**
- Recovery state reaches `Stage.DONE`
- Parquet files present on replica
- Format metadata matches primary

**Reference Implementation:** `IndexRecoveryIT.testTransientErrorsDuringRecoveryAreRetried()`

---

### Test 5: `testDataFusionRecoveryWithDisconnects`

**Priority:** HIGH

**Description:** Tests recovery behavior when nodes disconnect during recovery process.

**Implementation Plan:**
```java
public void testDataFusionRecoveryWithDisconnects() throws Exception {
    // Setup
    // 1. Start cluster with 3 nodes
    // 2. Create optimized index on specific node
    
    // Test Steps
    // 3. Index documents
    // 4. Flush to create Parquet files
    // 5. Start adding replica
    // 6. Simulate disconnect during recovery (using MockTransportService)
    //    - Either drop requests or throw ConnectTransportException
    // 7. Allow reconnection
    // 8. Wait for recovery to complete
    // 9. Validate format metadata on recovered replica
    
    // Validations
    // - Recovery completes after reconnect
    // - No duplicate Parquet files
    // - Format metadata intact
}
```

**Key Assertions:**
- Replica reaches green state
- No orphaned partial files
- Document count matches

**Reference Implementation:** `IndexRecoveryIT.testDisconnectsWhileRecovering()`

---

### Test 6: `testDataFusionRecoveryWithCorruptedFiles`

**Priority:** HIGH

**Description:** Tests that corrupted Parquet files are detected and properly handled during recovery.

**Implementation Plan:**
```java
public void testDataFusionRecoveryWithCorruptedFiles() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents
    // 4. Flush to create Parquet files
    // 5. Capture file list before corruption
    // 6. Corrupt one Parquet file on disk (using CorruptionUtils)
    // 7. Trigger replication/recovery
    // 8. Verify corrupted file is detected
    // 9. Verify recovery re-downloads correct file from remote store
    // 10. Validate all files have correct format metadata
    
    // Validations
    // - Corrupted file detected
    // - Recovery downloads fresh copy
    // - Format metadata valid post-recovery
}
```

**Key Assertions:**
- Corrupted file replaced
- Document count preserved
- No data loss
- Format metadata valid

**Reference Implementation:** `RemoteIndexShardTests.testNoFailuresOnFileReads()`

---

### Test 7: `testDataFusionRecoveryRetryOnRemoteStoreFailure`

**Priority:** MEDIUM

**Description:** Tests retry logic when remote store operations fail intermittently.

**Implementation Plan:**
```java
public void testDataFusionRecoveryRetryOnRemoteStoreFailure() throws Exception {
    // Setup
    // 1. Start cluster with mock repository that can inject failures
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents
    // 4. Flush
    // 5. Configure mock to fail first N upload attempts
    // 6. Trigger refresh that uploads to remote store
    // 7. Wait for retry mechanism to succeed
    // 8. Verify files eventually uploaded
    // 9. Stop and restart node
    // 10. Verify recovery from remote store works
    
    // Validations
    // - Retry mechanism works (exponential backoff)
    // - Files eventually uploaded
    // - Recovery works after retries
}
```

**Key Assertions:**
- Upload eventually succeeds
- Format metadata preserved through retries
- Recovery successful

**Reference Implementation:** `RemoteStoreRefreshListenerIT.testRemoteRefreshRetryOnFailure()`

---

## Category 3: Cluster Operations Tests

### Test 8: `testDataFusionGatewayRecovery`

**Priority:** HIGH

**Description:** Tests full cluster restart recovery to ensure CatalogSnapshot is properly recovered from remote store.

**Implementation Plan:**
```java
public void testDataFusionGatewayRecovery() throws Exception {
    // Setup
    // 1. Start cluster (1 master, 1 data)
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents
    // 4. Flush and refresh
    // 5. Capture CatalogSnapshot and format metadata
    // 6. Full cluster restart (internalCluster().fullRestart())
    // 7. Wait for green status
    // 8. Validate CatalogSnapshot matches pre-restart
    // 9. Validate format metadata preserved
    // 10. Execute search to verify data accessible
    
    // Validations
    // - Recovery source is ExistingStoreRecoverySource or RemoteStoreRecoverySource
    // - CatalogSnapshot restored correctly
    // - Parquet format metadata preserved
}
```

**Key Assertions:**
- `RecoveryState.getStage() == Stage.DONE`
- CatalogSnapshot bytes match
- Document count preserved
- Search returns correct results

**Reference Implementation:** `IndexRecoveryIT.testGatewayRecovery()`

---

### Test 9: `testDataFusionRerouteRecovery`

**Priority:** MEDIUM

**Description:** Tests shard relocation between nodes while preserving Parquet format metadata.

**Implementation Plan:**
```java
public void testDataFusionRerouteRecovery() throws Exception {
    // Setup
    // 1. Start cluster with 3 data nodes
    // 2. Create optimized index on node A
    
    // Test Steps
    // 3. Index documents
    // 4. Flush to create Parquet files
    // 5. Slow down recovery (for observation)
    // 6. Reroute shard from node A to node B
    // 7. Monitor recovery progress
    // 8. Wait for reroute to complete
    // 9. Validate format metadata on node B
    // 10. Optional: Reroute again to node C
    
    // Validations
    // - Shard successfully relocated
    // - Parquet files copied with format metadata
    // - No data loss
}
```

**Key Assertions:**
- Shard state STARTED on target node
- Format metadata preserved
- Recovery stats valid

**Reference Implementation:** `IndexRecoveryIT.testRerouteRecovery()`

---

### Test 10: `testDataFusionClusterManagerFailover`

**Priority:** MEDIUM

**Description:** Tests format metadata consistency during cluster manager failover.

**Implementation Plan:**
```java
public void testDataFusionClusterManagerFailover() throws Exception {
    // Setup
    // 1. Start cluster with 2 master-eligible nodes
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents
    // 4. Flush
    // 5. Start recovery on replica
    // 6. During recovery, restart current cluster manager
    // 7. Wait for new cluster manager election
    // 8. Wait for recovery to complete
    // 9. Validate format metadata consistency
    
    // Validations
    // - Recovery completes after failover
    // - Format metadata not corrupted
    // - Index remains healthy
}
```

**Key Assertions:**
- New cluster manager elected
- Recovery completes
- Format metadata valid

**Reference Implementation:** `IndexRecoveryIT.testOngoingRecoveryAndClusterManagerFailOver()`

---

### Test 11: `testDataFusionRecoveryWithMultipleReplicas`

**Priority:** HIGH

**Description:** Tests recovery with multiple replica shards to validate format-aware replication to multiple targets.

**Implementation Plan:**
```java
public void testDataFusionRecoveryWithMultipleReplicas() throws Exception {
    // Setup
    // 1. Start cluster with 4 data nodes
    // 2. Create optimized index with 3 replicas
    
    // Test Steps
    // 3. Index documents
    // 4. Flush to create Parquet files
    // 5. Validate all replicas have same Parquet files
    // 6. Validate format metadata on all replicas
    // 7. Stop primary node
    // 8. Wait for replica promotion
    // 9. Validate new primary has correct format metadata
    // 10. Add new replica
    // 11. Validate new replica recovers with format metadata
    
    // Validations
    // - All replicas have identical Parquet files
    // - Format metadata consistent across all shards
}
```

**Key Assertions:**
- `Store.segmentReplicationDiff()` shows no differences
- All replicas have same format metadata
- Document count consistent

**Reference Implementation:** `IndexRecoveryIT.testReplicaRecovery()`

---

## Category 4: Data Integrity & Consistency Tests

### Test 12: `testDataFusionNoDuplicateSeqNo`

**Priority:** HIGH

**Description:** Ensures sequence number integrity after recovery with Parquet format.

**Implementation Plan:**
```java
public void testDataFusionNoDuplicateSeqNo() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index with replica
    
    // Test Steps
    // 3. Index documents in batches
    // 4. Replicate segments to replica
    // 5. Flush primary
    // 6. Index more documents
    // 7. Replicate again
    // 8. Promote replica to primary
    // 9. Check for duplicate sequence numbers
    
    // Validations
    // - No duplicate sequence numbers
    // - Parquet records maintain correct seqno
}
```

**Key Assertions:**
- `assertAtMostOneLuceneDocumentPerSequenceNumber(engine)`
- Format metadata preserved

**Reference Implementation:** `RemoteIndexShardTests.testNoDuplicateSeqNo()`

---

### Test 13: `testDataFusionReplicaCommitsInfosOnRecovery`

**Priority:** MEDIUM

**Description:** Validates that replica commits segment infos with CatalogSnapshot bytes after recovery.

**Implementation Plan:**
```java
public void testDataFusionReplicaCommitsInfosOnRecovery() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index (no replica initially)
    
    // Test Steps
    // 3. Index documents
    // 4. Refresh primary
    // 5. Verify primary has CatalogSnapshot
    // 6. Add replica
    // 7. Recover replica
    // 8. Verify replica committed segment infos include CatalogSnapshot
    // 9. Compare primary and replica segment metadata
    
    // Validations
    // - Replica commits include CatalogSnapshot bytes
    // - Segment files match between primary and replica
}
```

**Key Assertions:**
- `SegmentInfos.readLatestCommit()` includes expected files
- CatalogSnapshot bytes present
- `Store.segmentReplicationDiff()` shows no differences

**Reference Implementation:** `RemoteIndexShardTests.testReplicaCommitsInfosBytesOnRecovery()`

---

### Test 14: `testDataFusionReplicaCleansUpOldCommits`

**Priority:** MEDIUM

**Description:** Tests that old Parquet generation files are properly cleaned up during replication.

**Implementation Plan:**
```java
public void testDataFusionReplicaCleansUpOldCommits() throws Exception {
    // Setup
    // 1. Start cluster with primary and replica
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index batch 1 -> Flush -> Replicate
    // 4. Capture initial commit generation
    // 5. Index batch 2 -> Refresh -> Replicate
    // 6. Verify no new commit on replica (refresh only)
    // 7. Index batch 3 -> Flush -> Replicate
    // 8. Verify new commit generation
    // 9. Verify old segments file cleaned up
    // 10. Verify single segments_N file exists
    
    // Validations
    // - Old commit files cleaned up
    // - Single segment file on replica
    // - Format metadata consistent
}
```

**Key Assertions:**
- Single `segments_N` file exists
- Old segment files removed
- Document count correct

**Reference Implementation:** `RemoteIndexShardTests.testRepicaCleansUpOldCommitsWhenReceivingNew()`

---

### Test 15: `testDataFusionSegmentFileConsistency`

**Priority:** MEDIUM

**Description:** Validates FileMetadata format information matches between local and remote store.

**Implementation Plan:**
```java
public void testDataFusionSegmentFileConsistency() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents
    // 4. Flush to create Parquet files
    // 5. List local Parquet files with FileMetadata
    // 6. List remote store Parquet files with FileMetadata
    // 7. Compare format information
    // 8. Verify all Parquet files have dataFormat() == "parquet"
    // 9. Stop node, start new node, recover
    // 10. Verify recovered files have same format metadata
    
    // Validations
    // - Local and remote files match
    // - Format metadata consistent
}
```

**Key Assertions:**
- File count matches local vs remote
- `FileMetadata.dataFormat()` consistent
- File checksums match

---

## Category 5: Multi-Index & Complex Scenarios

### Test 16: `testDataFusionRecoveryMultipleIndices`

**Priority:** MEDIUM

**Description:** Tests concurrent recovery of multiple optimized indices.

**Implementation Plan:**
```java
public void testDataFusionRecoveryMultipleIndices() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create 3 optimized indices
    
    // Test Steps
    // 3. Index documents to all indices
    // 4. Flush all indices
    // 5. Stop data node
    // 6. Start new data node
    // 7. Restore all indices concurrently
    // 8. Validate format metadata for each index
    // 9. Verify no cross-contamination of format metadata
    
    // Validations
    // - All indices recovered
    // - Format metadata correct for each index
    // - No mixed up files between indices
}
```

**Key Assertions:**
- Each index has correct document count
- Format metadata per-index is correct
- No shared file references between indices

**Reference Implementation:** `RemoteStoreRestoreIT.testRestoreFlowMultipleIndices()`

---

### Test 17: `testDataFusionRecoveryWithDeletedDocs`

**Priority:** MEDIUM

**Description:** Tests recovery with deleted documents to validate Parquet tombstone handling.

**Implementation Plan:**
```java
public void testDataFusionRecoveryWithDeletedDocs() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index 100 documents
    // 4. Flush
    // 5. Delete 50 documents
    // 6. Flush (creates tombstones)
    // 7. Verify doc count (50 live, 50 deleted)
    // 8. Stop node, start new node
    // 9. Recover from remote store
    // 10. Verify same doc count after recovery
    // 11. Force merge to remove deleted docs
    // 12. Verify only 50 docs remain
    
    // Validations
    // - Deleted doc count preserved
    // - Recovery handles tombstones
    // - Force merge works post-recovery
}
```

**Key Assertions:**
- Live doc count correct
- Deleted doc count correct
- Force merge reduces to expected count

---

### Test 18: `testDataFusionRecoveryAllShardsNoRedIndex`

**Priority:** MEDIUM

**Description:** Tests recovery ensuring no red index state during process.

**Implementation Plan:**
```java
public void testDataFusionRecoveryAllShardsNoRedIndex() throws Exception {
    // Setup
    // 1. Start cluster with 3 data nodes
    // 2. Create optimized index with 3 shards, 1 replica
    
    // Test Steps
    // 3. Index documents
    // 4. Flush all shards
    // 5. Stop 1 data node
    // 6. Verify cluster is yellow (not red)
    // 7. Start replacement node
    // 8. Restore from remote store
    // 9. Verify cluster returns to green
    // 10. Never hit red state during process
    
    // Validations
    // - Cluster health never red
    // - All shards recovered
    // - Format metadata preserved
}
```

**Key Assertions:**
- `ClusterHealthStatus != RED` throughout
- All shards eventually green
- Document count correct

**Reference Implementation:** `RemoteStoreRestoreIT.testRestoreFlowAllShardsNoRedIndex()`

---

### Test 19: `testDataFusionRecoveryWithMixedFormats`

**Priority:** LOW

**Description:** Tests CompositeStoreDirectory handles mixed Lucene and Parquet format recovery.

**Implementation Plan:**
```java
public void testDataFusionRecoveryWithMixedFormats() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents (creates Parquet files)
    // 4. Flush
    // 5. Verify both Lucene segment files and Parquet files exist
    // 6. Stop node
    // 7. Start new node
    // 8. Recover from remote store
    // 9. Verify both file types recovered
    // 10. Verify format metadata correct for each type
    
    // Validations
    // - Lucene files have format "lucene" or similar
    // - Parquet files have format "parquet"
    // - CompositeStoreDirectory handles both
}
```

**Key Assertions:**
- Both file types present
- Correct format metadata per type
- Search works across both formats

---

## Category 6: Edge Cases & Stress Tests

### Test 20: `testDataFusionRecoveryEmptyIndex`

**Priority:** MEDIUM

**Description:** Tests recovery of empty optimized index to validate initial CatalogSnapshot creation.

**Implementation Plan:**
```java
public void testDataFusionRecoveryEmptyIndex() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index (don't index any documents)
    
    // Test Steps
    // 3. Verify empty index has initial CatalogSnapshot
    // 4. Stop node
    // 5. Start new node
    // 6. Recover from remote store
    // 7. Verify empty index recovered
    // 8. Verify CatalogSnapshot initialized
    // 9. Index documents after recovery
    // 10. Verify normal operation
    
    // Validations
    // - Empty index recovers successfully
    // - CatalogSnapshot properly initialized
    // - Can index after recovery
}
```

**Key Assertions:**
- Doc count == 0
- CatalogSnapshot exists (even if minimal)
- Post-recovery indexing works

---

### Test 21: `testDataFusionRecoveryWithLargeParquetFiles`

**Priority:** LOW

**Description:** Tests recovery with large Parquet files to validate chunked transfer.

**Implementation Plan:**
```java
public void testDataFusionRecoveryWithLargeParquetFiles() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index large number of documents (1000+)
    // 4. Flush to create large Parquet files
    // 5. Verify file sizes are significant
    // 6. Configure small chunk size for recovery
    // 7. Stop node, start new node
    // 8. Recover from remote store
    // 9. Monitor recovery progress (multiple chunks)
    // 10. Verify complete file recovered
    
    // Validations
    // - Large files transferred in chunks
    // - No corruption during chunked transfer
    // - Format metadata preserved
}
```

**Key Assertions:**
- File checksums match
- Recovery completes without timeout
- Document count correct

---

### Test 22: `testDataFusionRecoveryWithHighConcurrency`

**Priority:** LOW

**Description:** Tests format metadata consistency under concurrent write operations.

**Implementation Plan:**
```java
public void testDataFusionRecoveryWithHighConcurrency() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    
    // Test Steps
    // 3. Start background indexer thread
    // 4. Trigger recovery while indexing continues
    // 5. Continue indexing during recovery
    // 6. Wait for recovery to complete
    // 7. Stop background indexer
    // 8. Verify document count
    // 9. Verify format metadata consistency
    
    // Validations
    // - No data loss
    // - Format metadata consistent
    // - No deadlocks or race conditions
}
```

**Key Assertions:**
- All indexed documents present
- Format metadata valid
- No exceptions during concurrent operations

**Reference Implementation:** `IndexRecoveryIT` with `BackgroundIndexer`

---

### Test 23: `testDataFusionRecoveryAfterIndexClose`

**Priority:** MEDIUM

**Description:** Tests recovery after index close/reopen to validate format state persistence.

**Implementation Plan:**
```java
public void testDataFusionRecoveryAfterIndexClose() throws Exception {
    // Setup
    // 1. Start cluster
    // 2. Create optimized index
    
    // Test Steps
    // 3. Index documents
    // 4. Flush
    // 5. Close index
    // 6. Verify index state is CLOSE
    // 7. Open index
    // 8. Verify format metadata preserved
    // 9. Stop node, start new node
    // 10. Close index, then restore from remote store
    // 11. Open index
    // 12. Verify format metadata and documents
    
    // Validations
    // - Format metadata survives close/open
    // - Recovery works on closed index
    // - Documents accessible after open
}
```

**Key Assertions:**
- Index state transitions correctly
- Format metadata preserved through close/open
- Document count correct

---

## Implementation Order (Recommended)

### Phase 1 (High Priority - Week 1)
1. `testDataFusionSnapshotRestore`
2. `testDataFusionRecoveryWithCorruptedFiles`
3. `testDataFusionGatewayRecovery`
4. `testDataFusionRecoveryWithMultipleReplicas`

### Phase 2 (High Priority - Week 2)
5. `testDataFusionRecoveryWithTransientErrors`
6. `testDataFusionRecoveryWithDisconnects`
7. `testDataFusionNoDuplicateSeqNo`

### Phase 3 (Medium Priority - Week 3)
8. `testDataFusionRerouteRecovery`
9. `testDataFusionReplicaCommitsInfosOnRecovery`
10. `testDataFusionReplicaCleansUpOldCommits`
11. `testDataFusionRecoveryAfterIndexClose`

### Phase 4 (Medium Priority - Week 4)
12. `testDataFusionRestoreWithForceMerge`
13. `testDataFusionShallowCopySnapshotRestore`
14. `testDataFusionClusterManagerFailover`
15. `testDataFusionSegmentFileConsistency`

### Phase 5 (Lower Priority - Week 5)
16. `testDataFusionRecoveryMultipleIndices`
17. `testDataFusionRecoveryWithDeletedDocs`
18. `testDataFusionRecoveryAllShardsNoRedIndex`
19. `testDataFusionRecoveryEmptyIndex`
20. `testDataFusionRecoveryRetryOnRemoteStoreFailure`

### Phase 6 (Nice to Have)
21. `testDataFusionRecoveryWithMixedFormats`
22. `testDataFusionRecoveryWithLargeParquetFiles`
23. `testDataFusionRecoveryWithHighConcurrency`

---

## Common Helper Methods to Add

```java
/**
 * Helper to validate Parquet format in uploaded segments
 */
private void validateParquetFormatInRemoteStore(IndexShard shard) {
    RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
    Map<String, UploadedSegmentMetadata> segments = remoteDir.getSegmentsUploadedToRemoteStore();
    
    for (Map.Entry<String, UploadedSegmentMetadata> entry : segments.entrySet()) {
        FileMetadata metadata = new FileMetadata(entry.getKey());
        if (entry.getKey().endsWith(".parquet")) {
            assertEquals("parquet", metadata.dataFormat());
        }
    }
}

/**
 * Helper to capture and compare recovery states
 */
private RecoveryStateSnapshot captureRecoveryState(IndexShard shard) {
    return new RecoveryStateSnapshot(
        shard.docStats().getCount(),
        validateLocalShardFiles(shard, "snapshot"),
        shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore().size()
    );
}

/**
 * Helper to validate states match after recovery
 */
private void assertRecoveryStateMatches(RecoveryStateSnapshot before, RecoveryStateSnapshot after) {
    assertEquals("Document count should match", before.docCount, after.docCount);
    assertEquals("Local file count should match", before.localFileCount, after.localFileCount);
    assertEquals("Remote file count should match", before.remoteFileCount, after.remoteFileCount);
}

/**
 * Helper record for recovery state snapshots
 */
private record RecoveryStateSnapshot(long docCount, long localFileCount, int remoteFileCount) {}

/**
 * Helper to create a snapshot repository for testing
 */
private void createSnapshotRepository(String repoName, Path path) {
    assertAcked(
        client().admin()
            .cluster()
            .preparePutRepository(repoName)
            .setType("fs")
            .setSettings(Settings.builder().put("location", path).put("compress", false))
    );
}
```

---

## Test Summary Table

| # | Test Name | Priority | Category | Est. Effort |
|---|-----------|----------|----------|-------------|
| 1 | testDataFusionSnapshotRestore | HIGH | Snapshot/Restore | 4h |
| 2 | testDataFusionRestoreWithForceMerge | MEDIUM | Snapshot/Restore | 3h |
| 3 | testDataFusionShallowCopySnapshotRestore | MEDIUM | Snapshot/Restore | 3h |
| 4 | testDataFusionRecoveryWithTransientErrors | HIGH | Error Handling | 6h |
| 5 | testDataFusionRecoveryWithDisconnects | HIGH | Error Handling | 6h |
| 6 | testDataFusionRecoveryWithCorruptedFiles | HIGH | Error Handling | 4h |
| 7 | testDataFusionRecoveryRetryOnRemoteStoreFailure | MEDIUM | Error Handling | 5h |
| 8 | testDataFusionGatewayRecovery | HIGH | Cluster Ops | 3h |
| 9 | testDataFusionRerouteRecovery | MEDIUM | Cluster Ops | 4h |
| 10 | testDataFusionClusterManagerFailover | MEDIUM | Cluster Ops | 5h |
| 11 | testDataFusionRecoveryWithMultipleReplicas | HIGH | Cluster Ops | 4h |
| 12 | testDataFusionNoDuplicateSeqNo | HIGH | Data Integrity | 3h |
| 13 | testDataFusionReplicaCommitsInfosOnRecovery | MEDIUM | Data Integrity | 3h |
| 14 | testDataFusionReplicaCleansUpOldCommits | MEDIUM | Data Integrity | 3h |
| 15 | testDataFusionSegmentFileConsistency | MEDIUM | Data Integrity | 3h |
| 16 | testDataFusionRecoveryMultipleIndices | MEDIUM | Complex | 4h |
| 17 | testDataFusionRecoveryWithDeletedDocs | MEDIUM | Complex | 3h |
| 18 | testDataFusionRecoveryAllShardsNoRedIndex | MEDIUM | Complex | 3h |
| 19 | testDataFusionRecoveryWithMixedFormats | LOW | Complex | 4h |
| 20 | testDataFusionRecoveryEmptyIndex | MEDIUM | Edge Cases | 2h |
| 21 | testDataFusionRecoveryWithLargeParquetFiles | LOW | Stress | 4h |
| 22 | testDataFusionRecoveryWithHighConcurrency | LOW | Stress | 5h |
| 23 | testDataFusionRecoveryAfterIndexClose | MEDIUM | Edge Cases | 3h |

**Total Estimated Effort:** ~85 hours (approximately 2-3 weeks of development)

---

## Dependencies & Prerequisites

### Required Test Framework Components
- `MockTransportService` - For simulating network failures
- `CorruptionUtils` - For file corruption tests
- `BackgroundIndexer` - For concurrent indexing tests
- `InternalTestCluster` - For cluster operations

### Required Imports to Add
```java
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.CorruptionUtils;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.transport.ConnectTransportException;
```

### Plugin Dependencies to Verify
- `MockTransportService.TestPlugin.class` in `nodePlugins()` for network simulation tests
- `MockFSIndexStore.TestPlugin.class` for file system mocking

---

## Notes for Implementation

1. **Test Isolation**: Each test should clean up resources properly using `@After` methods or try-with-resources
2. **Flaky Test Prevention**: Use `assertBusy()` with appropriate timeouts for async operations
3. **Logging**: Add appropriate `@TestLogging` annotations for debugging
4. **Cluster Scope**: Most tests should use `@ClusterScope(scope = Scope.TEST, numDataNodes = 0)` for isolation
5. **Parquet Validation**: Always validate `FileMetadata.dataFormat()` returns "parquet" for Parquet files
6. **CatalogSnapshot Validation**: Validate `RemoteSegmentMetadata.getSegmentInfosBytes()` is non-null and non-empty

---

## Success Criteria

A test is considered complete when:
1. ✅ Test passes consistently (no flaky failures)
2. ✅ Validates Parquet format metadata preservation
3. ✅ Validates CatalogSnapshot consistency
4. ✅ Validates document count before/after recovery
5. ✅ Properly cleans up resources
6. ✅ Has appropriate assertions and error messages
7. ✅ Is documented with clear JavaDoc comments
