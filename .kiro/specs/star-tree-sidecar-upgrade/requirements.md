# Requirements Document

## Introduction

This feature replaces the codec-switching approach for retroactive star tree upgrades with a sidecar architecture. The current approach switches each segment's codec from `Lucene912Codec` to `Composite912Codec` via a direct `SegmentInfos` rewrite. This breaks on segments with soft deletes because Lucene's `SegmentDocValuesProducer` routing assumes the codec declared in `.si` is the codec that originally wrote the base doc values files — switching the codec confuses the routing for `__soft_deletes`, causing a `PendingSoftDeletes` assertion failure.

The sidecar approach eliminates the codec switch entirely for existing segments. Star tree files are built as sidecar files alongside existing segments without modifying the segment's codec, `.si` file, or `SegmentInfos`. The star tree data is discovered at query time through a parallel discovery path.

Sidecar files are protected from `IndexWriter`'s garbage collection via a `SidecarProtectedDirectory` wrapper that intercepts `deleteFile()` calls. This wrapper wraps `Store.directory()` (the `StoreDirectory`) and is returned by a new `Store.engineDirectory()` method. All engine construction paths use `store.engineDirectory()` instead of `store.directory()`. Internal `Store` operations (`cleanupAndVerify`, `removeCorruptionMarker`, etc.) continue to use `store.directory()` (the raw `StoreDirectory`) — these must be audited and patched to consult the protected set. The wrapper is installed in `IndexShard` before engine creation across all recovery paths.

The `starTreeUpgradeInProgress` flag is shard-local. The `TransportStarTreeUpgradeAction` orchestrates the upgrade across all shards and only clears the flag on each shard after ALL shard upgrades have returned successfully. This ensures no shard serves star tree queries while other shards are still upgrading.

## Glossary

- **SidecarProtectedDirectory**: A `FilterDirectory` wrapper around `Store.directory()` that intercepts `deleteFile()`. Returned by `Store.engineDirectory()`. `unprotect()` + file deletion happen inside `StarTreeSidecarReader.deleteFiles()` at refCount=0 — never before `decRef()`.
- **Store.engineDirectory()**: A new method on `Store` that returns the `SidecarProtectedDirectory` when installed, or `Store.directory()` when not. All engine construction paths (`newEngineConfig()`) use this method. Internal `Store` operations continue using `Store.directory()` but are audited for sidecar file safety.
- **Sidecar_Metadata**: Generational per-shard metadata file (`_startree_sidecar_genN.meta`). Primary source of truth for crash recovery. In-memory singleton at `IndexShard` level, mutated under `sidecarMetadataLock` (in-memory only), disk I/O outside lock.
- **Sidecar_Reader**: `StarTreeSidecarReader` with Lucene-style CAS ref counting. `incRef()` throws `AlreadyClosedException` if count was 0. `deleteFiles()` at refCount=0 calls `unprotect()` then `deleteFile()` on underlying directory.
- **StarTreeValuesProvider**: Standalone interface (NOT extending `CompositeIndexReader`). `Composite912DocValuesReader` and `StarTreeSidecarReader` implement it directly. `StarTreeQueryHelper` uses separate `instanceof` checks.
- **starTreeUpgradeInProgress**: Shard-local `AtomicBoolean`. Set at start of `upgradeToStarTree()`. Cleared by `TransportStarTreeUpgradeAction` AFTER all shard upgrades return — not in the shard's `finally` block. Exposed via `isStarTreeUpgradeInProgress()` for query path.

## Requirements

### Requirement 1: Sidecar Star Tree File Generation

#### Acceptance Criteria

1. WHEN building star tree data for a segment, THE Star_Tree_Upgrade_Service SHALL write star tree files to the index directory as sidecar files without modifying the segment's `.si` file, codec declaration, or `SegmentInfos`.
2. WHEN building star tree data for a segment that has soft deletes, THE Star_Tree_Upgrade_Service SHALL use `SegmentReader.getLiveDocs()` to skip deleted documents.
3. WHEN building star tree data for a segment that already has sidecar files or uses Composite912Codec, THE Star_Tree_Upgrade_Service SHALL skip that segment.
4. THE Star_Tree_Upgrade_Service SHALL track every sidecar file written during a build run in a local list. In the finally block, any file not in committed metadata SHALL be deleted.
5. THE sidecar star tree SHALL be a self-contained aggregation index with a contiguous doc ID space (0 to numLiveDocs-1) and no per-doc mapping back to the original segment.
6. THE Star_Tree_Upgrade_Service SHALL write sidecar files to `store().directory()` (the raw `StoreDirectory`). This is consistent with `SidecarProtectedDirectory.getDelegate()` which also returns the raw `StoreDirectory`, so write and delete operations target the same directory. On non-POSIX filesystems (Windows), `deleteFile()` on a file with open handles may fail — the ref-counted reader ensures `deleteFiles()` is only called after all handles are closed (refCount=0), making this safe on all platforms.

### Requirement 2: Sidecar File Protection from IndexWriter GC

#### Acceptance Criteria

1. THE shard's `Directory` SHALL be wrapped in a `SidecarProtectedDirectory` (`FilterDirectory` subclass) that intercepts `deleteFile()`. If the file is in the protected set, deletion is silently skipped.
2. THE `SidecarProtectedDirectory` SHALL wrap `Store.directory()` and be returned by a new `Store.engineDirectory()` method. `IndexShard.newEngineConfig()` SHALL pass `store.engineDirectory()` (not `store.directory()`) to `EngineConfigFactory`. This ensures all engine construction paths — including `resetEngineToGlobalCheckpoint()` — use the wrapped directory.
3. `Store.directory()` SHALL continue returning the raw `StoreDirectory` for internal Store operations. `Store.cleanupAndVerify()` SHALL be patched to consult the `SidecarProtectedDirectory`'s protected set before deleting files — either by routing through the wrapper or by checking the set directly. All other Store methods that call `deleteFile()` internally SHALL be audited.
4. WHEN a sidecar file is intentionally cleaned up, `unprotect()` + `deleteFile()` SHALL happen inside `StarTreeSidecarReader.deleteFiles()` (called when refCount reaches zero with `pendingDeletion` set) — NOT before `decRef()`.
5. ON shard start, THE `SidecarProtectedDirectory` SHALL be initialized with the protected set from loaded metadata BEFORE the engine is created.
6. THE sidecar file names SHALL also be stored in `IndexWriter` commit data for crash recovery cross-checking — but this is NOT the GC protection mechanism.

### Requirement 3: Sidecar Metadata Management

#### Acceptance Criteria

1. THE Star_Tree_Upgrade_Service SHALL maintain a generational sidecar metadata file per shard (`_startree_sidecar_genN.meta`).
2. WHEN a segment is merged away, THE sidecar metadata SHALL be updated to remove the entry.
3. WHEN the shard is recovered, THE recovery process SHALL load the highest valid generational metadata file. The metadata file is the primary source of truth — if it exists and referenced files are on disk, they are valid regardless of commit data state. If metadata exists but files are missing, entries are stale and cleaned up. Commit data is used only to detect incomplete builds.
4. THE `StarTreeSidecarMetadata` instance SHALL be kept in memory at the `IndexShard` level. Mutations under `sidecarMetadataLock` (in-memory state only). Disk I/O (commit, update commit data) outside the lock to prevent deadlocks with flush.
5. WHEN `commit()` writes `_startree_sidecar_genN+1.meta` successfully, it SHALL delete the previous generation file `_startree_sidecar_genN.meta`. Only the current generation SHALL exist on disk at any time. ON shard start, the recovery scan SHALL delete all generation files except the highest valid one (handles crash between write and delete).

### Requirement 4: Sidecar Star Tree Reader with Reference Counting

#### Acceptance Criteria

1. WHEN `StarTreeQueryHelper.getStarTreeValues()` is called for a non-`CompositeIndexReader` segment, THE query path SHALL check sidecar metadata and open a sidecar reader if files exist.
2. THE sidecar reader SHALL implement `StarTreeValuesProvider` (standalone, NOT extending `CompositeIndexReader`). `Composite912DocValuesReader` SHALL also implement it directly. `StarTreeQueryHelper` checks `CompositeIndexReader` first, then sidecar reader by segment name.
3. THE sidecar reader SHALL use Lucene-style CAS ref counting: `incRef()` throws `AlreadyClosedException` if count was 0 before increment. `decRef()` at zero closes file handles; if `pendingDeletion`, calls `deleteFiles()` which does `unprotect()` + `deleteFile()` on underlying directory.
4. THE per-shard cache holds one reference. Each query increments before use and decrements in finally. Files NOT deleted until refCount=0 with `pendingDeletion`.
5. WHEN `StarTreeSidecarReader` fails to open files (`FileNotFoundException`), `StarTreeQueryHelper` catches it, removes the stale entry, returns null (benign TOCTOU handling).

### Requirement 5: Read and Write Availability During Upgrade

#### Acceptance Criteria

1. WHILE building sidecar files, THE IndexShard SHALL keep the InternalEngine live serving reads and writes.
2. THE Star_Tree_Upgrade_Service SHALL hold a `DirectoryReader` snapshot for the build duration to pin segment file handles.
3. WHEN sidecar generation completes, THE IndexShard SHALL flush OUTSIDE `blockOperations()` (drain translog), then enter `blockOperations()` for: update commit data → flush (persist, fast) → set `codecServiceOverride` → `resetEngineToGlobalCheckpoint()` → unblock.

### Requirement 6: Sidecar File Lifecycle and Cleanup

#### Acceptance Criteria

1. WHEN a merge consumes a sidecar segment, cleanup SHALL: (a) under `sidecarMetadataLock`: update in-memory metadata + `markPendingDeletion()` + `decRef()` on cached reader, (b) outside lock: commit metadata + update commit data. `unprotect()` + file deletion happen inside reader's `decRef()` at refCount=0.
2. Cleanup SHALL be triggered from `EngineMergeScheduler.afterMerge()`, dispatched to FLUSH thread pool.
3. ON shard start and periodically (on refresh), compare metadata vs `SegmentInfos` and clean up stale entries.

### Requirement 7: Mapping and Settings Update

#### Acceptance Criteria

1. THE TransportStarTreeUpgradeAction SHALL perform the mapping update first (add star tree field, set `index.composite_index=true`, set `index.append_only.enabled=true`).
2. AFTER sidecar generation, flush (outside block) + blockOperations + flush (persist commit data) + restart engine.

### Requirement 8: Error Handling and Recovery

#### Acceptance Criteria

1. Per-segment failures: log, skip, clean up partial files (per-run list), continue.
2. Metadata write failure: clean up all files from current run, report error.
3. Engine restart failure: attempt recovery with original codec. Sidecar files remain valid.
4. Idempotent: re-run skips segments with existing sidecar data.
5. Crash recovery: metadata file is primary source of truth. If metadata + files on disk → valid, protect. If metadata + files missing → stale, clean up. Commit data detects incomplete builds only.

### Requirement 9: Concurrent Upgrade Safety

#### Acceptance Criteria

1. Second upgrade request on same shard rejected via `starTreeUpgradeInProgress`.
2. Segments merged during build: `DirectoryReader` pins handles. Compare generations under lock. Discard sidecar files for merged-away segments. Residual TOCTOU made benign by Requirement 4.5.

### Requirement 10: Build Throttling

#### Acceptance Criteria

1. Yield between segment builds. Configurable rate limit.

### Requirement 11: Upgrade Completion Coordination and Replica Consistency

#### Acceptance Criteria

1. THE `starTreeUpgradeInProgress` flag is shard-local. It is set at the start of `IndexShard.upgradeToStarTree()`.
2. THE `starTreeUpgradeInProgress` flag SHALL NOT be cleared in `IndexShard.upgradeToStarTree()`'s finally block. Instead, THE `TransportStarTreeUpgradeAction` SHALL clear the flag on ALL shards across ALL nodes after the broadcast completes. Since `TransportBroadcastByNodeAction` already sends requests to every node hosting target shards, the flag clearing SHALL use the same broadcast mechanism — a second lightweight `TransportBroadcastByNodeAction` (or a `TransportNodesAction`) that calls `clearStarTreeUpgradeInProgress()` on each shard. This ensures remote shards on other nodes are cleared, not just local shards.
3. WHILE `starTreeUpgradeInProgress` is true on any shard, `StarTreeQueryHelper.isStarTreeSupported()` SHALL return false for that shard, ensuring all shards use normal aggregation during the upgrade window.
4. IF any shard upgrade fails, THE TransportStarTreeUpgradeAction SHALL still clear the flag on all shards (including successful ones) and report the failure. The failed shard can be retried independently.
