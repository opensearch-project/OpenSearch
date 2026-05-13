# Requirements Document

## Introduction

This feature enhances the existing per-segment star tree upgrade process to maintain search/read availability throughout the upgrade window. The current implementation closes the InternalEngine entirely (setting `currentEngineReference` to null), which blocks both reads and writes while star tree data is built and SegmentInfos are rewritten. The proposed approach introduces a ReadOnlyEngine bridge: after closing the InternalEngine, a ReadOnlyEngine (with `obtainLock=false`) is opened atomically in the same `synchronized(engineMutex)` block, keeping reads available during the entire upgrade. After the upgrade completes, the ReadOnlyEngine is closed and a new InternalEngine is opened atomically in another `synchronized(engineMutex)` block. This eliminates the read unavailability window (typically 27+ seconds for large indices) at a cost of ~100-200ms for ReadOnlyEngine open/close overhead.

Additionally, this feature addresses three correctness improvements to the existing upgrade flow:
1. The `codecServiceOverride` is kept set after the upgrade (not cleared to null) so that engine-only restarts (e.g., `resetEngineToGlobalCheckpoint`) use the composite codec.
2. The `StarTreeUpgradeService.upgradeSegments()` method is split into two public methods (`buildStarTreeDataForSegments()` and `rewriteSegmentInfos()`) to allow the caller to interleave engine lifecycle operations between phases.
3. The `index.composite_index` setting is set to `true` in cluster state during `applyStarTreeMapping()` so the metadata is truthful and subsequent engine creations use the composite codec without relying on the override.

## Glossary

- **IndexShard**: The OpenSearch shard abstraction that manages engine lifecycle, flush, operation blocking, and the `currentEngineReference`.
- **InternalEngine**: The read-write engine backed by an IndexWriter that handles indexing, search, and translog operations.
- **ReadOnlyEngine**: An engine that serves reads from the last committed SegmentInfos without an IndexWriter. Accepts an `obtainLock` parameter — passing `false` skips acquiring the write lock.
- **Engine_Mutex**: The `engineMutex` object in IndexShard used to synchronize engine swaps. Lock ordering: engineMutex → mutex.
- **Current_Engine_Reference**: The `AtomicReference<Engine>` in IndexShard that holds the active engine. All read/search operations acquire the engine from this reference.
- **Star_Tree_Upgrade_Service**: The utility class that performs the two-phase star tree upgrade: Phase 1 (build star tree files) and Phase 2 (rewrite SegmentInfos).
- **Phase_1**: Star tree data generation — iterates segments, reads doc values, builds star tree files (.cid, .cim, .cidvd, .cidvm) via StarTreesBuilder. Creates only new files; does not modify existing files.
- **Phase_2**: Codec switch — rewrites SegmentInfos and .si files to declare Composite912Codec, adds star tree files to segment file sets, commits segments_N+1. Deletes old .si files and writes new ones.
- **Codec_Service_Override**: A volatile `CodecService` field on IndexShard that, when non-null, overrides the stale final `codecService` field in `newEngineConfig()`. Ensures post-upgrade engines use Composite912Codec.
- **Composite_Index_Setting**: The `index.composite_index` boolean setting in cluster state that indicates whether an index uses composite indexes.
- **TransportStarTreeUpgradeAction**: The transport action that handles star tree upgrade requests, including the cluster state mapping update via `applyStarTreeMapping()`.

## Requirements

### Requirement 1: Atomic Engine Swap from InternalEngine to ReadOnlyEngine

**User Story:** As a cluster operator, I want the star tree upgrade to swap from the InternalEngine to a ReadOnlyEngine atomically, so that there is no window where `currentEngineReference` is null and reads fail.

#### Acceptance Criteria

1. WHEN the star tree upgrade begins the engine swap phase, THE IndexShard SHALL close the InternalEngine and set the ReadOnlyEngine as the current engine within a single `synchronized(engineMutex)` block.
2. WHEN creating the ReadOnlyEngine for the upgrade bridge, THE IndexShard SHALL pass `obtainLock=false` so the ReadOnlyEngine does not acquire the IndexWriter write lock, allowing Phase_2 to write to the directory.
3. WHEN the ReadOnlyEngine is set as the current engine, THE Current_Engine_Reference SHALL transition directly from InternalEngine to ReadOnlyEngine without passing through null.
4. IF the ReadOnlyEngine fails to open during the swap, THEN THE IndexShard SHALL propagate the exception to the caller without leaving Current_Engine_Reference in a null state.

### Requirement 2: Read Availability During Star Tree Upgrade

**User Story:** As a cluster operator, I want search and read operations to remain available during the star tree upgrade, so that queries are not rejected during the upgrade window.

#### Acceptance Criteria

1. WHILE the ReadOnlyEngine is active during the star tree upgrade, THE IndexShard SHALL serve search and read operations from the last committed SegmentInfos state.
2. WHILE Phase_1 is in progress, THE ReadOnlyEngine SHALL serve reads because Phase_1 only creates new files and does not modify existing segment files or SegmentInfos.
3. WHILE Phase_2 is in progress, THE ReadOnlyEngine SHALL continue serving reads from its snapshot of the pre-upgrade SegmentInfos. The ReadOnlyEngine holds a DirectoryReader that is a snapshot of segments_N and is unaffected by the segments_N+1 commit.
4. WHEN a search request arrives during the upgrade, THE IndexShard SHALL acquire the engine from Current_Engine_Reference and execute the search against the ReadOnlyEngine.

### Requirement 3: Atomic Engine Swap from ReadOnlyEngine to InternalEngine

**User Story:** As a cluster operator, I want the post-upgrade engine swap to be atomic, so that reads transition seamlessly from the ReadOnlyEngine to the new InternalEngine without a gap.

#### Acceptance Criteria

1. WHEN the star tree upgrade completes successfully, THE IndexShard SHALL close the ReadOnlyEngine and set the new InternalEngine as the current engine within a single `synchronized(engineMutex)` block.
2. WHEN the new InternalEngine is opened, THE IndexShard SHALL call `onNewEngine()` and refresh the engine before setting it as the current engine, so that the searcher is warmed up.
3. WHEN the new InternalEngine is set as the current engine, THE Current_Engine_Reference SHALL transition directly from ReadOnlyEngine to InternalEngine without passing through null.
4. WHEN the post-upgrade refresh completes, THE IndexShard SHALL execute the refresh outside the Engine_Mutex to avoid holding the lock during a potentially slow operation.

### Requirement 4: StarTreeUpgradeService Method Split

**User Story:** As a developer, I want `upgradeSegments()` split into two public methods, so that the IndexShard can interleave engine lifecycle operations between Phase_1 and Phase_2.

#### Acceptance Criteria

1. THE Star_Tree_Upgrade_Service SHALL expose a public method `buildStarTreeDataForSegments(Directory, StarTreeField, MapperService)` that executes Phase_1 and returns the set of successfully upgraded segment names.
2. THE Star_Tree_Upgrade_Service SHALL expose the existing `rewriteSegmentInfos(Directory, Set<String>)` method as public, executing Phase_2 for the given segment names.
3. THE Star_Tree_Upgrade_Service SHALL retain the combined `upgradeSegments()` method as a convenience method that calls `buildStarTreeDataForSegments()` followed by `rewriteSegmentInfos()`.
4. WHEN `buildStarTreeDataForSegments()` returns an empty set, THE caller SHALL skip Phase_2 because no segments were upgraded.

### Requirement 5: Error Handling for Engine Swap Failures

**User Story:** As a cluster operator, I want the upgrade to recover gracefully if the new InternalEngine fails to open after the upgrade, so that the shard does not become permanently unavailable.

#### Acceptance Criteria

1. IF the new InternalEngine fails to open after a successful upgrade, THEN THE IndexShard SHALL attempt a recovery by opening a new InternalEngine with the original codec configuration.
2. IF the recovery attempt with the original codec also fails, THEN THE IndexShard SHALL call `failShard()` to mark the shard as failed and allow the cluster to reallocate it.
3. WHEN an error occurs during Phase_1, THE Star_Tree_Upgrade_Service SHALL track the set of star tree files created for the failed segment and provide a `cleanupStarTreeFiles(Directory, Set<String>)` method to delete orphaned files.
4. IF Phase_1 partially succeeds and Phase_2 fails, THEN THE IndexShard SHALL clean up orphaned star tree files from Phase_1 using the cleanup method.

### Requirement 6: codecServiceOverride Lifecycle Fix

**User Story:** As a developer, I want the `codecServiceOverride` to persist after the upgrade, so that engine-only restarts (like `resetEngineToGlobalCheckpoint`) use the composite codec instead of the stale original codec.

#### Acceptance Criteria

1. WHEN the star tree upgrade succeeds, THE IndexShard SHALL set `codecServiceOverride` to a CodecService created with the updated MapperService and SHALL NOT clear it to null afterward.
2. WHILE `codecServiceOverride` is set, THE IndexShard SHALL use it in `newEngineConfig()` instead of the stale final `codecService` field, ensuring any engine creation on this IndexShard instance uses Composite912Codec.
3. WHEN `codecServiceOverride` is set before opening the new InternalEngine, THE new InternalEngine SHALL use Composite912Codec for new segments written after the upgrade (flushes and merges).

### Requirement 7: Set index.composite_index in Cluster State

**User Story:** As a cluster operator, I want the `index.composite_index` setting to be set to `true` during the star tree mapping update, so that the cluster state metadata is truthful and subsequent engine creations use the composite codec without relying on the override.

#### Acceptance Criteria

1. WHEN `applyStarTreeMapping()` updates the cluster state, THE TransportStarTreeUpgradeAction SHALL set `index.composite_index = true` in the index settings alongside the existing `index.append_only.enabled = true` setting.
2. WHEN `index.composite_index` is already `true` on the target index, THE TransportStarTreeUpgradeAction SHALL skip setting it again to avoid unnecessary settings version increments.
3. WHEN `index.composite_index` is set to `true` in cluster state, THE CodecService created for subsequent engine configurations SHALL include Composite912Codec because `MapperService.isCompositeIndexPresent()` returns `true`.

### Requirement 8: Write Lock Acquisition During Phase 2

**User Story:** As a developer, I want Phase_2 to explicitly acquire the IndexWriter write lock as a defensive measure, so that no other process can modify the directory during SegmentInfos rewrite.

#### Acceptance Criteria

1. WHEN Phase_2 begins, THE Star_Tree_Upgrade_Service SHALL acquire the IndexWriter write lock on the directory before deleting old .si files and writing new ones.
2. WHEN Phase_2 completes or fails, THE Star_Tree_Upgrade_Service SHALL release the write lock in a finally block.
3. WHILE the write lock is held during Phase_2, THE ReadOnlyEngine SHALL continue serving reads because it was opened with `obtainLock=false` and holds its own DirectoryReader snapshot.
