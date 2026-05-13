# Implementation Plan: Retroactive Star Tree Building

## Overview

Implement the retroactive star tree upgrade feature by creating a dedicated `StarTreeUpgradeService`, extending `IndexShard` with an `upgradeToStarTree(StarTreeField)` method, and exposing the functionality through a new transport action with a REST endpoint that accepts the star tree configuration in the request body. Phase 1 builds star tree files per segment using standard Lucene doc values APIs. Phase 2 switches the codec via direct `SegmentInfos` rewrite (no force merge, no IndexUpgrader). The mapping update happens before the per-shard upgrade so that `MapperService` has the composite field types available.

## Tasks

- [x] 1. Create StarTreeUpgradeService with core segment upgrade logic
  - [x] 1.1 Create `StarTreeUpgradeService` class in `org.opensearch.index.compositeindex.datacube.startree`
    - Implement `upgradeSegments(Directory directory, StarTreeField starTreeField, MapperService mapperService)`
    - Phase 1: Read `SegmentInfos.readLatestCommit(directory)`, iterate segments, skip if codec == Composite912Codec, call `buildStarTreeData()` for each eligible segment, track successful segment names in `upgradedSegmentNames` set
    - Phase 2: Call `rewriteSegmentInfos(directory, upgradedSegmentNames)` to switch codec and commit segments_N+1
    - Catch and log per-segment failures in Phase 1, do NOT add failed segment names to `upgradedSegmentNames`
    - Return count of upgraded segments
    - _Requirements: 2.1, 2.6, 3.1, 3.3, 5.1_
  - [x] 1.2 Implement `buildStarTreeData()` method for single segment star tree file generation
    - Open `SegmentReader` from `SegmentCommitInfo` (uses segment's existing codec — any codec works for doc values access)
    - Get `DocValuesProducer` from the reader
    - Build `fieldProducerMap` from doc values producer for all dimensions and metrics in the provided `StarTreeField`
    - Create `SegmentWriteState` for the segment (use segment's existing `FieldInfos`, set maxDoc from `SegmentInfo.maxDoc()`)
    - Open `IndexOutput` for `.cid` and `.cim` files using `directory.createOutput()`
    - Create `DocValuesConsumer` for `.cidvd` and `.cidvm` files using `LuceneDocValuesConsumerFactory`
    - Pass `MapperService` to `StarTreesBuilder` constructor (needed by `BaseStarTreeBuilder.generateMetricAggregatorInfos()` to resolve `FieldValueConverter`)
    - Call `StarTreesBuilder.build(metaOut, dataOut, fieldProducerMap, consumer)` to generate star tree data
    - Close `SegmentReader`, `IndexOutput`, and `DocValuesConsumer` in finally block
    - NOTE: Does NOT modify codec or .si file — that's Phase 2
    - _Requirements: 2.2, 2.3, 5.4_
  - [x] 1.3 Implement `rewriteSegmentInfos()` method for direct SegmentInfos rewrite
    - Read `SegmentInfos.readLatestCommit(directory)`
    - Clone the `SegmentInfos`, clear it, then iterate original segments
    - For each segment in `upgradedSegmentNames`: create new `SegmentInfo` with `Composite912Codec`, copy all other fields from original, add star tree files to file set, create new `SegmentCommitInfo` preserving delCount/softDelCount/delGen/fieldInfosGen/docValuesGen/id
    - For segments NOT in `upgradedSegmentNames`: add original `SegmentCommitInfo` unchanged
    - Copy user data from original `SegmentInfos` to new one
    - Add assertions to verify no state is lost (maxDoc, version, id, indexSort, attributes)
    - Call `newSegmentInfos.commit(directory)` to write `segments_N+1` (generation auto-incremented by commit())
    - Call `directory.sync(newSegmentInfos.files(true))` and `directory.syncMetaData()`
    - Follows same pattern as `Store.commitSegmentInfos()` (Store.java line 907)
    - _Requirements: 2.4, 2.5, 2.6_
  - [ ]* 1.4 Write unit tests for StarTreeUpgradeService
    - Test codec detection (skip Composite912Codec segments)
    - Test that `upgradeSegments` accepts `StarTreeField` and `MapperService`
    - Test SegmentInfo construction (codec is Composite912Codec, file set includes star tree files, all other attributes preserved)
    - Test SegmentCommitInfo preservation (delCount, softDelCount, delGen, fieldInfosGen, docValuesGen)
    - Test partial failure: only successfully upgraded segments get codec switch
    - _Requirements: 2.1, 2.4, 3.1, 3.3, 5.1_
  - [ ]* 1.5 Write property test for upgrade idempotency
    - **Property 1: Upgrade idempotency**
    - **Validates: Requirements 2.1**
  - [ ]* 1.6 Write property test for codec and file set correctness
    - **Property 2: Upgraded segment codec and file set correctness**
    - **Validates: Requirements 2.4, 2.5, 2.6**

- [x] 2. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Integrate with IndexShard for shard-level orchestration
  - [x] 3.1 Add `upgradeToStarTree(StarTreeField starTreeField)` method to `IndexShard`
    - Call `flush(new FlushRequest().force(true))`
    - Use `indexShardOperationPermits.blockOperations()` to block and drain in-flight operations
    - Inside the blocked section: swap to read-only engine following `resetEngineToGlobalCheckpoint()` pattern (pass seqNoStats, translogStats, override acquireLastIndexCommit/acquireSafeIndexCommit/getSegmentInfosSnapshot)
    - Delegate to `StarTreeUpgradeService.upgradeSegments(store().directory(), starTreeField, mapperService)`
    - In finally block: call `resetEngineToGlobalCheckpoint()` to reopen the engine from segments_N+1
    - After callback completes: operations are unblocked automatically
    - Return the count of upgraded segments
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 6.1, 6.2, 6.3_
  - [x] 3.2 Add concurrent upgrade guard
    - Add an `AtomicBoolean starTreeUpgradeInProgress` field to `IndexShard`
    - Check and set at the start of `upgradeToStarTree()`, reset in finally block
    - Throw `IllegalStateException` if already in progress
    - _Requirements: 6.5_
  - [ ]* 3.3 Write property test for partial failure resilience
    - **Property 5: Partial failure resilience**
    - **Validates: Requirements 5.1**
  - [ ]* 3.4 Write property test for resource cleanup
    - **Property 6: Resource cleanup on all paths**
    - **Validates: Requirements 5.4**

- [x] 4. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Create transport action and REST handler for API exposure
  - [x] 5.1 Create `StarTreeUpgradeRequest` and `StarTreeUpgradeResponse` classes
    - `StarTreeUpgradeRequest` extends `BroadcastRequest` with target index names and `StarTreeField` parsed from request body
    - `StarTreeUpgradeResponse` includes per-shard results (upgradedSegments, skippedSegments, failedSegments)
    - Implement serialization/deserialization including the `StarTreeField`
    - _Requirements: 4.1, 4.3, 4.4_
  - [x] 5.2 Create `TransportStarTreeUpgradeAction` extending `TransportBroadcastByNodeAction`
    - Follow the pattern of `TransportUpgradeAction`
    - In `doExecute()`: resolve indices, validate primary shards, submit mapping update (same bypass mechanism as existing star-tree-upgrade-via-mapping: `allowCompositeFieldWithoutSettings` flag + `STAR_TREE_UPGRADE` merge reason), wait for ack, then broadcast `shardOperation()`
    - In `shardOperation()`: verify MapperService has the star tree field before proceeding (guard against cluster state propagation race — throw retryable error if not yet available)
    - Implement `shardOperation()` to extract `StarTreeField` from request and call `indexShard.upgradeToStarTree(starTreeField)`
    - Check for `ClusterBlockLevel.METADATA_WRITE` blocks
    - Register the action in the `ActionModule`
    - _Requirements: 4.1, 4.2, 4.4, 4.5_
  - [x] 5.3 Create REST handler for the star tree upgrade API
    - Register endpoint `POST /{index}/_star_tree/upgrade`
    - Parse request body JSON into `StarTreeField` object (name, ordered_dimensions, metrics with stats)
    - Validate the parsed config and reject with descriptive error if invalid
    - Pass `StarTreeField` to `TransportStarTreeUpgradeAction` via `StarTreeUpgradeRequest`
    - _Requirements: 3.1, 3.2, 4.1_
  - [ ]* 5.4 Write property test for star tree config parsing round-trip
    - **Property 3: Star tree config parsing round-trip**
    - **Validates: Requirements 3.1**
  - [ ]* 5.5 Write property test for invalid config rejection
    - **Property 4: Invalid config rejection**
    - **Validates: Requirements 3.2**
  - [ ]* 5.6 Write unit tests for transport action
    - Test request serialization round-trip including StarTreeField
    - Test response construction with mixed success/failure shards
    - Test primary shard availability validation
    - Test mapping update bypass mechanism
    - _Requirements: 4.2, 4.3, 4.4, 4.5_

- [ ] 6. Integration testing
  - [ ]* 6.1 Write integration tests for end-to-end upgrade flow
    - Index documents into a non-star-tree index → upgrade via API with star tree config in request body → verify star tree data files exist and segments declare Composite912Codec
    - Test that write operations are blocked while read/search operations continue serving from the read-only engine during upgrade
    - Test post-upgrade operation resumption (reads and writes succeed)
    - Test idempotent upgrade (run twice, verify no changes on second run)
    - Test concurrent upgrade rejection
    - Test upgrade with invalid/missing star tree config in request body (expect error)
    - Test upgrade on index without `index.composite_index` enabled (should succeed — mapping update uses bypass)
    - Test read/search availability during upgrade (queries should return results from last committed state via read-only engine)
    - Test that aggregation queries use star tree path after upgrade (mapping update enables query routing)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 3.2, 3.4, 3.5, 5.1, 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 7. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- The implementation reuses existing `StarTreesBuilder`, `Composite912DocValuesFormat`, `LuceneDocValuesConsumerFactory`, and `IndexShard` operation permits infrastructure
- Phase 2 uses direct `SegmentInfos` manipulation (same pattern as `Store.bootstrapNewHistory()`) — no `IndexUpgrader`, no `IndexWriter`, no force merge
- `MapperService` is required by `BaseStarTreeBuilder.generateMetricAggregatorInfos()` to resolve `FieldValueConverter` for metric fields
- The mapping update happens in `TransportStarTreeUpgradeAction.doExecute()` before broadcasting `shardOperation()`, using the same bypass mechanism as the existing star-tree-upgrade-via-mapping feature
- The read-write engine is swapped to a read-only engine during upgrade — reads remain available from the last committed state, writes are blocked
- `resetEngineToGlobalCheckpoint()` replaces the read-only engine with a new read-write engine after upgrade, picking up the new `segments_N+1` with Composite912Codec
- Star tree files are written via raw `IndexOutput` in Phase 1 — no composite codec needed for writing. The composite codec is only needed at read time by `Composite912DocValuesReader`
- Failed segments in Phase 1 are excluded from `upgradedSegmentNames`, so Phase 2 leaves them with their original codec — no corrupt state
