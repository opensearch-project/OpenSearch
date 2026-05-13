# OpenSearch Snapshot V2 (Shallow Snapshot V2) — End-to-End Flow

## 1. What It Is & Why It Exists

**Snapshot V1 (shallow copy)** stores per-shard metadata (`RemoteStoreShallowCopySnapshot`) that references specific commits (primaryTerm + commitGeneration) in the remote segment store. Each shard requires individual metadata writes during snapshot creation.

**Snapshot V2** eliminates per-shard work entirely. Instead of recording per-shard commit points, it **pins a timestamp** against the remote store. All segment metadata files that existed at that timestamp are implicitly "locked" — the remote store's GC won't delete them as long as the pin exists. This makes snapshot creation O(1) in shard count: one timestamp pin + cluster-state metadata write + `finalizeSnapshot`.

Key motivation: For clusters with thousands of shards, V1's per-shard metadata writes become a bottleneck. V2 reduces snapshot creation to a single timestamp pin operation regardless of shard count.

## 2. Core Concepts

### Settings (Repository-Level)
- `remote_store_index_shallow_copy` (bool, default false) — enables V1 shallow snapshots. Defined at `BlobStoreRepository:415`.
- `shallow_snapshot_v2` (bool, default false) — enables V2. Defined at `BlobStoreRepository:417`.
- Both must be true + min cluster version ≥ 2.17.0 + all indices requested (no partial index list) for V2 path.
- Only ONE repository in the cluster can have `shallow_snapshot_v2=true` (`RepositoriesService:756`).

### Pinned Timestamp Mechanism (`RemoteStorePinnedTimestampService`)
- Timestamps are pinned by writing zero-byte blobs to `<segment-repo-basePath>/pinned_timestamps/` with filename format: `<pinningEntity>__<timestamp>`.
- Pinning entity for snapshots: `<repoName>__<snapshotUUID>` (delimiter = `__`, defined in `SnapshotsService:216`).
- A background async task periodically reads all pinned timestamp blobs and caches them in-memory as `pinnedTimestampsSet`.
- Remote store GC (`RemoteStoreUtils.getPinnedTimestampLockedFiles`) checks pinned timestamps before deleting metadata files — any metadata file whose timestamp ≤ a pinned timestamp is retained.

### Cluster State Tracking
- `SnapshotsInProgress.Entry` has two booleans: `remoteStoreIndexShallowCopy` and `remoteStoreIndexShallowCopyV2`.
- V2 entries have an **empty shards map** (`new HashMap<>()` at `SnapshotsService:530`) — no per-shard status tracking.
- State transitions: STARTED → SUCCESS (immediately, since `completed(emptyMap)` = true).

### Immutable vs Mutable Metadata
- **Immutable**: Segment files in remote store (referenced by metadata files at pinned timestamp).
- **Mutable**: `SnapshotInfo` (written to `snap-<uuid>.dat`), `RepositoryData` (index-N blob), index metadata blobs.

## 3. Snapshot Creation Flow

### Entry Point
`SnapshotsService.executeSnapshot()` (line 284)

### V2 Decision Gate (line 288-298)
```java
boolean isSnapshotV2 = SHALLOW_SNAPSHOT_V2.get(repository.getMetadata().settings());
boolean remoteStoreIndexShallowCopy = remoteStoreShallowCopyEnabled(repository);
if (remoteStoreIndexShallowCopy && isSnapshotV2
    && request.indices().length == 0  // must be all-indices snapshot
    && clusterService.state().nodes().getMinNodeVersion().onOrAfter(Version.V_2_17_0)) {
    createSnapshotV2(request, listener);
}
```

### `createSnapshotV2()` (line 471) — Phase Sequence

1. **Pin timestamp** (line 478-482): `pinnedTimestamp = System.currentTimeMillis()` → `updateSnapshotPinnedTimestamp(snapshot, pinnedTimestamp)` which calls `remoteStorePinnedTimestampService.pinTimestamp(...)` synchronously (with latch).

2. **Cluster state update** (line 486): `repository.executeConsistentStateUpdate(...)` → adds `SnapshotsInProgress.Entry` with empty shards map and `remoteStoreIndexShallowCopyV2=true`.

3. **Enter repo loop** (line 556): `tryEnterRepoLoop(repositoryName)` — prevents concurrent V2 snapshots on same repo.

4. **`clusterStateProcessed` callback** (line 574):
   - Builds `ShardGenerations` from existing `RepositoryData` (no per-shard snapshot work — just reads current generations).
   - Constructs `SnapshotInfo` with `pinnedTimestamp` as both start time and pinned timestamp field.
   - Calls `repository.finalizeSnapshot(...)` which writes:
     - Global metadata blob
     - Per-index metadata blobs  
     - `snap-<uuid>.dat` (SnapshotInfo)
     - Updated `index-N` blob (RepositoryData)

5. **Post-finalize** (line 621): `cleanOrphanTimestamp()` — removes any pinned timestamps for snapshot UUIDs not in RepositoryData (orphan cleanup).

6. **Leave repo loop** + notify listener.

### Key Difference from V1
V1 (`createSnapshot`) assigns shards to data nodes → each node writes `BlobStoreIndexShardSnapshot` per shard → cluster-manager waits for all shard completions → then finalizes. V2 skips ALL of this.

## 4. Snapshot Restore Flow

### Entry Point
`RestoreService` (line 406): Sets `isRemoteStoreShallowCopy` from `snapshotInfo.isRemoteStoreIndexShallowCopyEnabled()`.

### Recovery Source Construction (line 427)
```java
new SnapshotRecoverySource(..., isRemoteStoreShallowCopy, sourceRemoteStoreRepository,
    sourceRemoteTranslogRepository, snapshotInfo.getPinnedTimestamp())
```

### Shard-Level Recovery Decision (`IndexShard:3496`)
```java
if (recoverySource.pinnedTimestamp() != 0) {
    storeRecovery.recoverShallowSnapshotV2(...);  // V2 path
} else {
    storeRecovery.recoverFromSnapshotAndRemoteStore(...);  // V1 path
}
```

### `StoreRecovery.recoverShallowSnapshotV2()` (line 465)

1. Gets `RepositoryData` → resolves `IndexId` → fetches `IndexMetadata` from snapshot.
2. Creates `RemoteSegmentStoreDirectory` pointing at source index's remote segment store.
3. **Key V2 call**: `sourceRemoteDirectory.initializeToSpecificTimestamp(recoverySource.pinnedTimestamp())` — finds the metadata file whose timestamp is ≤ pinned timestamp using `RemoteStoreUtils.getPinnedTimestampLockedFiles()`.
4. `indexShard.syncSegmentsFromGivenRemoteSegmentStore(true, sourceRemoteDirectory, remoteSegmentMetadata, true)` — copies segment files from source remote directory.
5. `indexShard.syncTranslogFilesFromGivenRemoteTranslog(...)` — syncs translog from source remote translog repo using the pinned timestamp.
6. `indexShard.openEngineAndRecoverFromTranslog(false)` — replays translog.
7. Finalize recovery.

### V1 vs V2 Restore Contrast
- **V1**: Uses `RemoteStoreShallowCopySnapshot` per-shard metadata → `initializeToSpecificCommit(primaryTerm, commitGeneration, snapshotUUID)`.
- **V2**: Uses pinned timestamp → `initializeToSpecificTimestamp(pinnedTimestamp)`. No per-shard metadata needed from snapshot repo.

## 5. Snapshot Deletion Flow

### Entry (`SnapshotsService` line 2989)
```java
final boolean remoteStoreShallowCopyEnabled = REMOTE_STORE_INDEX_SHALLOW_COPY.get(repository.getMetadata().settings());
```
Partitions snapshots into `snapshotsWithPinnedTimestamp` (V2, where `snapshotInfo.getPinnedTimestamp() > 0`) and `snapshotsWithLockFiles` (V1).

### V2 Deletion Path (line 3031)
```java
repository.deleteSnapshotsWithPinnedTimestamp(snapshotsWithPinnedTimestamp, ...)
```

### `BlobStoreRepository.deleteSnapshotsWithPinnedTimestamp()` (line 1224)
Calls `deleteSnapshotsInternal(...)` with `isShallowSnapshotV2=true` and `remoteStoreLockManagerFactory=null`.

### `doDeleteShardSnapshots()` (line 1352) — Steps:
1. **Write updated shard metadata** (remove snapshot from shard-level index files).
2. **Update RepositoryData** (remove snapshot, write new `index-N`).
3. **Unpin timestamps** (`removeSnapshotsPinnedTimestamp` at line 1489): Calls `remoteStorePinnedTimestampService.unpinTimestamp(timestamp, pinningEntity, ...)` for each deleted snapshot.
4. **Clean remote store files** (`cleanUpRemoteStoreFilesForDeletedIndicesV2` at line 1463): For indices that are no longer referenced by any snapshot, triggers `cleanRemoteStoreDirectoryIfNeeded()` to delete orphaned segment files.

### Stale File Cleanup
Once a timestamp is unpinned, the remote store's periodic GC (driven by `RemoteStorePinnedTimestampService`'s cached set) will naturally allow deletion of segment metadata files that are no longer protected by any pin.

## 6. Key Files

| File | Purpose |
|------|---------|
| `server/src/main/java/org/opensearch/snapshots/SnapshotsService.java` | Orchestrates V2 creation (`createSnapshotV2`), deletion routing, pinning entity management |
| `server/src/main/java/org/opensearch/node/remotestore/RemoteStorePinnedTimestampService.java` | Pin/unpin/clone timestamps, async refresh of pinned set, blob-based storage |
| `server/src/main/java/org/opensearch/repositories/blobstore/BlobStoreRepository.java` | Settings (`SHALLOW_SNAPSHOT_V2`, `REMOTE_STORE_INDEX_SHALLOW_COPY`), `finalizeSnapshot`, `deleteSnapshotsWithPinnedTimestamp`, stale cleanup |
| `server/src/main/java/org/opensearch/repositories/RepositoriesService.java` | Validates V2 settings (single-repo constraint, version checks, delimiter check) |
| `server/src/main/java/org/opensearch/snapshots/RestoreService.java` | Constructs `SnapshotRecoverySource` with `pinnedTimestamp` for V2 restores |
| `server/src/main/java/org/opensearch/index/shard/IndexShard.java` | Routes to `recoverShallowSnapshotV2()` when `pinnedTimestamp != 0` |
| `server/src/main/java/org/opensearch/index/shard/StoreRecovery.java` | `recoverShallowSnapshotV2()` — segment sync from remote store at pinned timestamp |
| `server/src/main/java/org/opensearch/index/store/RemoteSegmentStoreDirectory.java` | `initializeToSpecificTimestamp()` — resolves metadata file matching pinned timestamp |
| `server/src/main/java/org/opensearch/index/remote/RemoteStoreUtils.java` | `getPinnedTimestampLockedFiles()` — determines which metadata files are protected by pins |
| `server/src/main/java/org/opensearch/cluster/SnapshotsInProgress.java` | `Entry.startedEntry()` with `remoteStoreIndexShallowCopyV2` flag |
| `server/src/main/java/org/opensearch/snapshots/SnapshotInfo.java` | `pinnedTimestamp` field serialization in snapshot metadata |
| `server/src/main/java/org/opensearch/cluster/routing/RecoverySource.java` | `SnapshotRecoverySource` carries `pinnedTimestamp`, `sourceRemoteStoreRepository` |
