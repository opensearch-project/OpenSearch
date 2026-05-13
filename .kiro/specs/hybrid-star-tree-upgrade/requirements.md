# Requirements Document

## Introduction

This feature combines the two existing star tree upgrade approaches — native codec switching and sidecar file building — into a single hybrid strategy for retroactively adding star tree acceleration to existing OpenSearch indices.

The hybrid approach uses the mapping update (already implemented in `TransportStarTreeUpgradeAction`) to set `composite_index=true` and `append_only=true`, which causes `IndexWriter`'s codec to become `Composite912Codec`. After the mapping update, every new flush produces native composite segments automatically. For existing pre-upgrade segments, the sidecar path builds star tree files alongside them without modifying their codec. Background merges gradually converge sidecar segments with native composite segments — the merge output is always native composite because the write codec is `Composite912Codec`. Eventually all sidecar segments are absorbed and the index is fully native composite.

The key insight: `getCandidateSegmentNames()` already skips `Composite912Codec` segments, so the sidecar build only targets pre-upgrade segments. New documents flushed after the mapping update produce native composite segments and never enter the sidecar path.

## Glossary

- **Hybrid_Upgrade_Orchestrator**: The modified `TransportStarTreeUpgradeAction.doExecute()` flow that coordinates the mapping update, immediate write acceptance on the new codec, and concurrent sidecar build for pre-upgrade segments.
- **Sidecar_Build_Service**: The `StarTreeUpgradeService.buildSidecarStarTreeData()` method that builds star tree files as sidecar files alongside existing segments without modifying their codec.
- **Native_Composite_Segment**: A segment flushed or merged with `Composite912Codec` as the write codec, containing star tree data natively in its file set.
- **Sidecar_Segment**: A pre-upgrade segment that retains its original codec but has sidecar star tree files (`.cid`, `.cim`, `.cidvd`, `.cidvm`) managed by `StarTreeSidecarMetadata`.
- **Mapping_Update**: The cluster state update in `applyStarTreeMapping()` that adds the star tree field to the index mapping and sets `composite_index=true` and `append_only=true`.
- **SidecarProtectedDirectory**: The `FilterDirectory` wrapper that intercepts `deleteFile()` calls to protect sidecar files from `IndexWriter` garbage collection.
- **StarTreeSidecarMetadata**: The generational metadata file (`_startree_sidecar_genN.meta`) that tracks which segments have sidecar star tree files.
- **StarTreeSidecarReader**: The reference-counted reader that opens sidecar star tree files and implements `StarTreeValuesProvider`.
- **StarTreeValuesProvider**: The interface implemented by both `Composite912DocValuesReader` (native) and `StarTreeSidecarReader` (sidecar) for providing star tree values to the query path.
- **Merge_Convergence**: The process by which background Lucene merges combine sidecar segments with native composite segments, producing native composite output and triggering sidecar cleanup.
- **getCandidateSegmentNames**: The method in `StarTreeUpgradeService` that identifies segments eligible for sidecar build by skipping those already using `Composite912Codec`.
- **LiveDocsFilteredDocValuesProducer**: The `DocValuesProducer` wrapper that filters out soft-deleted documents using a `Bits liveDocs` bitset during sidecar star tree construction.
- **InternalEngine**: The primary read-write engine implementation in OpenSearch that manages `IndexWriter`, `SearcherManager`, and translog.
- **IndexShard**: The core shard abstraction that manages engine lifecycle, sidecar metadata, and the sidecar reader cache.

## Requirements

### Requirement 1: Mapping Update Enables Native Codec for New Data

**User Story:** As a cluster operator, I want the mapping update to immediately enable the composite codec for new data, so that documents indexed after the upgrade API call produce native composite segments with star tree acceleration.

#### Acceptance Criteria

1. WHEN the `POST /{index}/_star_tree/upgrade` request is received and the Mapping_Update succeeds, THE Hybrid_Upgrade_Orchestrator SHALL ensure that `IndexWriter`'s write codec becomes `Composite912Codec` on each shard via the updated `MapperService` (which `newEngineConfig` picks up from `mapperService.isCompositeIndexPresent()`).
2. WHEN the Mapping_Update sets `composite_index=true` and `append_only=true` in the cluster state, THE Hybrid_Upgrade_Orchestrator SHALL accept writes on the shard immediately after the mapping update is acknowledged, without waiting for the sidecar build to complete.
3. WHEN a new document is indexed after the Mapping_Update, THE InternalEngine SHALL flush it into a Native_Composite_Segment that contains star tree data natively.
4. WHEN getCandidateSegmentNames is called during the sidecar build, THE Sidecar_Build_Service SHALL skip all segments whose codec is `Composite912Codec`, ensuring new flushes are never processed by the sidecar path.

### Requirement 2: Concurrent Sidecar Build for Pre-Upgrade Segments

**User Story:** As a cluster operator, I want existing segments to receive star tree acceleration via sidecar files built concurrently with normal indexing, so that the upgrade does not block writes or require downtime.

#### Acceptance Criteria

1. WHEN the Mapping_Update is acknowledged, THE Hybrid_Upgrade_Orchestrator SHALL start the sidecar build targeting only segments that existed before the mapping update (segments not using `Composite912Codec`).
2. WHILE the Sidecar_Build_Service is building sidecar files for pre-upgrade segments, THE IndexShard SHALL continue accepting read and write operations without blocking.
3. WHEN the Sidecar_Build_Service builds star tree data for a pre-upgrade segment that has soft deletes, THE Sidecar_Build_Service SHALL use LiveDocsFilteredDocValuesProducer to filter out soft-deleted documents and remap document IDs to a contiguous space.
4. WHEN the sidecar build completes for a segment, THE Sidecar_Build_Service SHALL register the sidecar files in StarTreeSidecarMetadata and commit the metadata to a generational file on disk.
5. IF a segment is merged away by background merge activity during the sidecar build, THEN THE Sidecar_Build_Service SHALL discard the sidecar files for that segment and remove its entry from StarTreeSidecarMetadata.

### Requirement 3: Sidecar File Protection from IndexWriter Garbage Collection

**User Story:** As a cluster operator, I want sidecar star tree files to be protected from deletion by IndexWriter's garbage collection, so that in-flight queries and the sidecar reader cache can safely access them.

#### Acceptance Criteria

1. WHEN sidecar files are registered in StarTreeSidecarMetadata, THE IndexShard SHALL add them to the SidecarProtectedDirectory's protected set so that `IndexWriter.deleteUnusedFiles()` does not delete them.
2. WHEN `SidecarProtectedDirectory.deleteFile()` is called for a file in the protected set, THE SidecarProtectedDirectory SHALL silently skip the deletion (no-op).
3. WHEN `Store.cleanupAndVerify()` encounters a file in the SidecarProtectedDirectory's protected set, THE Store SHALL skip that file during its cleanup sweep.
4. WHEN a shard starts and StarTreeSidecarMetadata contains entries, THE IndexShard SHALL install the SidecarProtectedDirectory wrapper with the initial protected file set before engine creation.
5. IF a shard starts and `mapperService.isCompositeIndexPresent()` is true but no generational metadata files exist on disk, THEN THE IndexShard SHALL skip SidecarProtectedDirectory installation, treating the index as a fresh composite index with no sidecar history.

### Requirement 4: Background Merge Convergence to Native Composite Format

**User Story:** As a cluster operator, I want background merges to gradually convert sidecar segments into native composite segments, so that the index eventually reaches a fully native composite state without manual intervention.

#### Acceptance Criteria

1. WHEN Lucene merges a Sidecar_Segment with a Native_Composite_Segment (or with other Sidecar_Segments), THE InternalEngine SHALL produce a Native_Composite_Segment as output because the write codec is `Composite912Codec`.
2. WHEN a merge completes that consumed one or more Sidecar_Segments, THE InternalEngine's `EngineMergeScheduler.afterMerge()` SHALL dispatch sidecar cleanup to the FLUSH thread pool via the `sidecarMergeCleanupCallback`.
3. WHEN sidecar cleanup runs for a merged-away segment, THE IndexShard SHALL remove the segment from StarTreeSidecarMetadata, remove the StarTreeSidecarReader from the sidecar reader cache, call `markPendingDeletion()` on the reader, and call `decRef()` to release the cache reference.
4. WHEN a StarTreeSidecarReader's reference count reaches zero and `pendingDeletion` is true, THE StarTreeSidecarReader SHALL call `unprotect()` on the SidecarProtectedDirectory and then delete the sidecar files from the underlying directory.
5. WHEN all Sidecar_Segments have been merged into Native_Composite_Segments, THE index SHALL be fully native composite with no remaining sidecar files or metadata.

### Requirement 5: Unified Query Path for Native and Sidecar Star Tree Data

**User Story:** As a cluster operator, I want queries to transparently use star tree acceleration from both native composite segments and sidecar segments, so that aggregation performance improves immediately after the upgrade without waiting for full merge convergence.

#### Acceptance Criteria

1. WHEN a search query arrives and `StarTreeQueryHelper.getStarTreeValues()` is called for a leaf reader context, THE StarTreeQueryHelper SHALL first check if the segment's `DocValuesReader` is a `CompositeIndexReader` (native path).
2. WHEN the native path check fails (segment is not native composite), THE StarTreeQueryHelper SHALL look up the segment name in the IndexShard's sidecar reader cache and return star tree values from the StarTreeSidecarReader if present.
3. WHEN a StarTreeSidecarReader is found in the cache, THE StarTreeQueryHelper SHALL call `incRef()` to protect the reader from concurrent merge cleanup, and catch `AlreadyClosedException` to handle the race window where the reader was closed between cache lookup and `incRef()`.
4. WHEN neither native nor sidecar star tree data is available for a segment, THE StarTreeQueryHelper SHALL return null, causing the aggregator to fall back to normal doc values collection for that segment.
5. WHILE a star tree upgrade is in progress on a shard (`starTreeUpgradeInProgress` is true), THE StarTreeQueryHelper SHALL skip star tree acceleration for that shard to avoid result divergence.

### Requirement 6: Sidecar Reader Reference Counting and Safe Cleanup

**User Story:** As a developer, I want sidecar readers to use reference counting so that in-flight queries are not disrupted when merge cleanup deletes sidecar files.

#### Acceptance Criteria

1. THE StarTreeSidecarReader SHALL use CAS-based atomic reference counting starting at refCount=1 (the cache reference).
2. WHEN `incRef()` is called on a StarTreeSidecarReader with refCount less than or equal to zero, THE StarTreeSidecarReader SHALL throw `AlreadyClosedException`.
3. WHEN `decRef()` is called and the reference count reaches zero, THE StarTreeSidecarReader SHALL close all internal file handles via `closeInternal()`.
4. WHEN `decRef()` reaches zero and `pendingDeletion` is true, THE StarTreeSidecarReader SHALL call `deleteFiles()` which first calls `unprotect()` on the SidecarProtectedDirectory and then deletes each sidecar file from the underlying directory.
5. THE StarTreeSidecarReader SHALL call `unprotect()` only inside `deleteFiles()` at refCount=0, ensuring files remain protected while any query thread holds a reference.

### Requirement 7: Crash Recovery with Metadata as Primary Source of Truth

**User Story:** As a cluster operator, I want the shard to recover correctly after a crash, preserving valid sidecar data and cleaning up incomplete builds.

#### Acceptance Criteria

1. WHEN a shard starts, THE IndexShard SHALL scan for `_startree_sidecar_genN.meta` files, load the highest valid generation, and delete stale generation files.
2. WHEN a segment entry in StarTreeSidecarMetadata references files that exist on disk, THE IndexShard SHALL treat the entry as valid and add the files to the SidecarProtectedDirectory's protected set.
3. WHEN a segment entry in StarTreeSidecarMetadata references files that do not exist on disk, THE IndexShard SHALL treat the entry as stale and remove it from the metadata.
4. WHEN the sidecar metadata file exists and is valid, THE IndexShard SHALL use it as the primary source of truth for crash recovery, regardless of commit data state.
5. WHEN no sidecar metadata files exist on disk, THE IndexShard SHALL proceed with an empty StarTreeSidecarMetadata and skip SidecarProtectedDirectory installation.

### Requirement 8: Sidecar Metadata Cleanup Lock Scope

**User Story:** As a developer, I want the sidecar metadata lock to never be held during disk I/O, so that merge cleanup does not deadlock with flush operations.

#### Acceptance Criteria

1. WHEN merge cleanup modifies StarTreeSidecarMetadata, THE IndexShard SHALL perform in-memory state updates (remove entries, mark readers for deletion, decRef) under the `sidecarMetadataLock`.
2. WHEN merge cleanup needs to commit StarTreeSidecarMetadata to disk, THE IndexShard SHALL release the `sidecarMetadataLock` before performing disk I/O (metadata commit and flush).
3. THE IndexShard SHALL split merge cleanup into two phases: Phase 1 (in-memory updates under lock) and Phase 2 (disk I/O outside lock).

### Requirement 9: Idempotency and Concurrent Upgrade Safety

**User Story:** As a cluster operator, I want the upgrade to be safe to retry and to reject concurrent upgrades on the same shard, so that operational errors do not cause data corruption.

#### Acceptance Criteria

1. WHEN a star tree upgrade is requested on an index that already has the star tree field in its mapping and all segments already contain star tree data (native or sidecar), THE Hybrid_Upgrade_Orchestrator SHALL return a success response indicating no work was needed.
2. WHEN a star tree upgrade is requested on an index that has the star tree field in its mapping but some segments lack star tree data, THE Hybrid_Upgrade_Orchestrator SHALL skip the mapping update and proceed with the sidecar build for remaining segments.
3. IF a second upgrade request arrives while an upgrade is already in progress on the same shard, THEN THE IndexShard SHALL reject the second request with an `IllegalStateException` via the `starTreeUpgradeInProgress` atomic guard.
4. WHEN the sidecar build fails on a shard, THE IndexShard SHALL clear the `starTreeUpgradeInProgress` flag so the shard can be retried independently.

### Requirement 10: Shard Start Behavior for Hybrid State

**User Story:** As a developer, I want shard start to correctly handle the hybrid state where both sidecar and native composite segments coexist, so that star tree acceleration works immediately after a node restart.

#### Acceptance Criteria

1. WHEN a shard starts and StarTreeSidecarMetadata contains entries, THE IndexShard SHALL install the SidecarProtectedDirectory, populate the sidecar reader cache, and wire the merge cleanup callback before the engine is created.
2. WHEN a shard starts and `mapperService.isCompositeIndexPresent()` is true but no sidecar metadata files exist, THE IndexShard SHALL skip SidecarProtectedDirectory installation, as the index is either a fresh composite index or has fully converged to native composite.
3. WHEN a shard starts with both sidecar and native composite segments, THE StarTreeQueryHelper SHALL serve star tree values from the native path for native composite segments and from the sidecar reader cache for sidecar segments.
4. WHEN a shard starts, THE IndexShard SHALL populate the sidecar reader cache using segment IDs and maxDoc values from the current `SegmentInfos`, creating a StarTreeSidecarReader for each segment in the metadata.

### Requirement 11: Per-Shard Results and Error Reporting

**User Story:** As a cluster operator, I want the upgrade API to return per-shard results, so that I can verify the upgrade succeeded on all shards.

#### Acceptance Criteria

1. WHEN a shard upgrade succeeds, THE Hybrid_Upgrade_Orchestrator SHALL report the shard identifier and success status in the response.
2. WHEN a shard upgrade fails, THE Hybrid_Upgrade_Orchestrator SHALL report the shard identifier, failure status, and error details in the response.
3. WHEN the upgrade completes, THE Hybrid_Upgrade_Orchestrator SHALL return a response containing the total number of shards, successful shards, and failed shards.
4. IF the mapping update fails, THEN THE Hybrid_Upgrade_Orchestrator SHALL return an error without any side effects (no settings or mapping changes committed).
