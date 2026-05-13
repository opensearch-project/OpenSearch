# Requirements Document

## Introduction

This feature enables retroactive upgrading of existing OpenSearch index segments to use Star Tree indexes. Indexes that were created before Star Tree was enabled (or before the feature existed) can be upgraded in-place by building Star Tree data structures on each segment using standard Lucene doc values APIs, then switching the segment codec to Composite912Codec via a direct SegmentInfos rewrite (no force merge, no IndexUpgrader). The upgrade operates at the shard level: flush → block operations → swap to read-only engine → build star tree data per segment → rewrite SegmentInfos with new codec + star tree files → reopen read-write engine via resetEngineToGlobalCheckpoint → unblock operations. Reads remain available throughout the upgrade via the read-only engine. The star tree field configuration (dimensions, metrics) is provided in the API request body. A mapping update is performed before the per-shard upgrade so that MapperService has the composite field types available (required by BaseStarTreeBuilder.generateMetricAggregatorInfos()).

## Glossary

- **Star_Tree_Upgrade_Service**: The dedicated service responsible for orchestrating the retroactive star tree upgrade on a per-shard basis.
- **IndexShard**: The OpenSearch shard abstraction that manages engine lifecycle, flush, and write blocking.
- **Composite912Codec**: The Lucene codec that supports composite index file formats including star tree data.
- **StarTreesBuilder**: The existing builder infrastructure that constructs star tree data structures from segment doc values.
- **SegmentInfos**: The Lucene data structure that tracks all segments and their metadata for an index.
- **SegmentCommitInfo**: Metadata about a single committed segment including codec, file set, and delete counts.
- **TransportStarTreeUpgradeAction**: The transport action that handles star tree upgrade requests across the cluster.
- **Star_Tree_Field**: The configuration object defining dimensions, metrics, and build parameters for a star tree index, parsed from the API request body.
- **SegmentInfos rewrite**: Direct manipulation of Lucene's SegmentInfos to switch codec declarations and update file sets, then commit as segments_N+1. Follows the same pattern as Store.bootstrapNewHistory().

## Requirements

### Requirement 1: Shard-Level Star Tree Upgrade Orchestration

**User Story:** As a cluster operator, I want to upgrade existing index shards to use star tree indexes, so that I can benefit from star tree accelerated aggregations on historical data without reindexing.

#### Acceptance Criteria

1. WHEN a star tree upgrade is requested for a shard, THE Star_Tree_Upgrade_Service SHALL flush the shard to ensure all in-memory data is persisted to segments before processing.
2. WHEN the flush completes, THE Star_Tree_Upgrade_Service SHALL block all operations on the shard and drain in-flight operations for the duration of the upgrade.
3. WHEN operations are blocked, THE Star_Tree_Upgrade_Service SHALL close the current read-write engine and replace it with a read-only engine so that search operations remain available during the upgrade while the upgrade operates directly on the index directory.
4. WHEN the upgrade completes successfully, THE Star_Tree_Upgrade_Service SHALL replace the read-only engine with a new read-write engine via resetEngineToGlobalCheckpoint() and unblock operations to resume normal read-write traffic.
5. WHEN the upgrade completes with a failure, THE Star_Tree_Upgrade_Service SHALL replace the read-only engine with a new read-write engine via resetEngineToGlobalCheckpoint(), unblock operations, and report the error.

### Requirement 2: Per-Segment Star Tree Construction

**User Story:** As a cluster operator, I want each segment in a shard to be individually upgraded with star tree data, so that the upgrade is incremental and segments already using Composite912Codec are not reprocessed.

#### Acceptance Criteria

1. WHEN processing a segment that already uses Composite912Codec, THE Star_Tree_Upgrade_Service SHALL skip that segment without modification.
2. WHEN processing a segment that does not use Composite912Codec, THE Star_Tree_Upgrade_Service SHALL read the segment using a SegmentReader to access its doc values.
3. WHEN building star tree data for a segment, THE Star_Tree_Upgrade_Service SHALL use the existing StarTreesBuilder infrastructure with the segment's doc values producers.
4. WHEN star tree files are generated for a segment, THE Star_Tree_Upgrade_Service SHALL track the segment name in an `upgradedSegmentNames` set for use in Phase 2.
5. WHEN all segments are processed, THE Star_Tree_Upgrade_Service SHALL rewrite SegmentInfos directly: for each segment in `upgradedSegmentNames`, create a new `SegmentCommitInfo` with `Composite912Codec` and the star tree files added to the file set, preserving all other segment metadata (delCount, softDelCount, delGen, fieldInfosGen, docValuesGen).
6. WHEN the SegmentInfos rewrite completes, THE Star_Tree_Upgrade_Service SHALL commit `segments_N+1` atomically via `SegmentInfos.commit(directory)`, sync files, and sync metadata. Segments that failed in Phase 1 SHALL retain their original codec and file set.

### Requirement 3: Star Tree Configuration via API Request

**User Story:** As a cluster operator, I want to provide the star tree field configuration (dimensions, metrics) in the upgrade API request body, so that I can upgrade indexes that were created without star tree enabled and without needing to modify the index mapping or `index.composite_index` setting.

#### Acceptance Criteria

1. WHEN initiating a star tree upgrade, THE Star_Tree_Upgrade_Service SHALL parse the star tree field configuration (dimensions, metrics, build parameters) from the API request body.
2. IF the API request body does not contain a valid star tree field configuration, THEN THE Star_Tree_Upgrade_Service SHALL reject the upgrade request with a descriptive error message.
3. WHEN building star tree data, THE Star_Tree_Upgrade_Service SHALL construct the field-to-producer mapping using all dimensions and metrics defined in the request-provided Star_Tree_Field configuration. MapperService is required by BaseStarTreeBuilder.generateMetricAggregatorInfos() to resolve FieldValueConverter for each metric field.
4. THE Star_Tree_Upgrade_Service SHALL NOT require `index.composite_index` to be enabled on the target index. A mapping update is performed before the per-shard upgrade using the same bypass mechanism as the existing star-tree-upgrade-via-mapping feature (allowCompositeFieldWithoutSettings flag + STAR_TREE_UPGRADE merge reason).
5. WHEN the mapping update and per-shard upgrade complete, the search layer SHALL be able to route aggregation queries through the star tree path because the mapping contains the star tree field configuration.

### Requirement 4: Upgrade API Exposure

**User Story:** As a cluster operator, I want to trigger the star tree upgrade through an API, so that I can initiate the upgrade on selected indices without manual intervention.

#### Acceptance Criteria

1. WHEN a star tree upgrade API request is received, THE TransportStarTreeUpgradeAction SHALL parse the star tree field configuration from the request body and route the request to the appropriate nodes hosting the target index shards.
2. WHEN the API request specifies target indices, THE TransportStarTreeUpgradeAction SHALL resolve the concrete indices and validate that all primary shards are available.
3. THE API request body SHALL include the star tree configuration with ordered dimensions, metrics with stats, and optional build parameters (e.g., max leaf docs, build mode).
4. WHEN a shard upgrade succeeds, THE TransportStarTreeUpgradeAction SHALL report the shard as successfully upgraded in the response.
5. WHEN a shard upgrade fails, THE TransportStarTreeUpgradeAction SHALL report the failure with the shard identifier and error details.

### Requirement 5: Error Handling and Recovery

**User Story:** As a cluster operator, I want the upgrade process to handle errors gracefully and leave the shard in a consistent state, so that a failed upgrade does not corrupt the index.

#### Acceptance Criteria

1. IF an error occurs during star tree file generation for a segment, THEN THE Star_Tree_Upgrade_Service SHALL close the SegmentReader, skip the failed segment, and continue processing remaining segments.
2. IF an error occurs during the SegmentInfos commit phase, THEN THE Star_Tree_Upgrade_Service SHALL leave the original segments_N file intact so the shard can recover from the last committed state. Star tree files written in Phase 1 become orphaned but harmless — they are not in any committed file set and will be cleaned up on the next merge or segment deletion.
3. IF the engine fails to reopen after the upgrade, THEN THE Star_Tree_Upgrade_Service SHALL log the error and propagate the exception to the caller.
4. THE Star_Tree_Upgrade_Service SHALL close all SegmentReader instances in a finally block to prevent resource leaks regardless of success or failure.

### Requirement 6: Concurrency and Write Safety

**User Story:** As a cluster operator, I want the upgrade to be safe with respect to concurrent operations, so that no data is lost or corrupted during the upgrade.

#### Acceptance Criteria

1. WHILE the star tree upgrade is in progress, THE Star_Tree_Upgrade_Service SHALL prevent all indexing, delete, and bulk write operations on the shard.
2. WHILE the star tree upgrade is in progress, THE Star_Tree_Upgrade_Service SHALL allow read and search operations to continue serving from the last committed state via the read-only engine.
3. WHEN operations are blocked, THE Star_Tree_Upgrade_Service SHALL use the IndexShard operation permits mechanism to block and drain in-flight operations.
4. WHEN the upgrade completes or fails, THE Star_Tree_Upgrade_Service SHALL release the operation permits to resume normal operations.
5. IF a second upgrade request arrives while an upgrade is already in progress on the same shard, THEN THE Star_Tree_Upgrade_Service SHALL reject the second request with an appropriate error.
