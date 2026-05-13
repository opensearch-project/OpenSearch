# Implementation Plan: Star Tree Upgrade via Mapping

## Overview

Implement retroactive star tree building on existing indices by updating the mapping (bypassing Final setting checks), restarting the engine for composite codec selection, enhancing the merge path to build star trees from raw doc values, and triggering forceMerge. Changes span ObjectMapper, CompositeIndexValidator, Composite912DocValuesWriter, IndexShard, and new transport/REST action classes.

## Tasks

- [x] 1. Add STAR_TREE_UPGRADE MergeReason and ParserContext bypass flag
  - [x] 1.1 Add `STAR_TREE_UPGRADE` to `MapperService.MergeReason` enum
    - Add the new enum value in `server/src/main/java/org/opensearch/index/mapper/MapperService.java`
    - _Requirements: 2.3_

  - [x] 1.2 Add `allowCompositeFieldWithoutSettings` flag to `Mapper.TypeParser.ParserContext`
    - Add a boolean field, getter, and setter to `ParserContext` in `server/src/main/java/org/opensearch/index/mapper/Mapper.java`
    - _Requirements: 2.1_

  - [x] 1.3 Modify `ObjectMapper.parseCompositeField()` to check the bypass flag
    - In `server/src/main/java/org/opensearch/index/mapper/ObjectMapper.java`, wrap the `IS_COMPOSITE_INDEX_SETTING` and `INDEX_APPEND_ONLY_ENABLED_SETTING` checks with `if (parserContext.isAllowCompositeFieldWithoutSettings() == false)`
    - _Requirements: 2.1_

  - [x] 1.4 Modify `CompositeIndexValidator.validate()` to accept MergeReason and skip restriction for STAR_TREE_UPGRADE
    - In `server/src/main/java/org/opensearch/index/compositeindex/CompositeIndexValidator.java`, add an overload or modify the 4-arg `validate()` to accept a `MergeReason` parameter and skip the "no new composite fields during update" check when `mergeReason == STAR_TREE_UPGRADE`
    - Still call `StarTreeValidator.validate()` to validate dims/metrics against existing fields
    - _Requirements: 2.2_

  - [ ]* 1.5 Write unit tests for bypass logic
    - Test `parseCompositeField()` succeeds with `allowCompositeFieldWithoutSettings=true` when `IS_COMPOSITE_INDEX_SETTING=false`
    - Test `CompositeIndexValidator.validate()` allows composite field addition with `STAR_TREE_UPGRADE` merge reason
    - Test `parseCompositeField()` still rejects when flag is false and setting is false
    - **Property 4: Upgrade merge reason bypasses setting checks**
    - **Validates: Requirements 2.1, 2.2**
    - _Requirements: 2.1, 2.2_

- [x] 2. Enhance Composite912DocValuesWriter merge path
  - [x] 2.1 Modify `mergeStarTreeFields()` to fall back to building from raw doc values
    - In `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesWriter.java`, after collecting `starTreeSubsPerField`, check if it's empty and `compositeMappedFieldTypes` is non-empty
    - If so, build a `fieldProducerMap` from the merge state's doc values producers and call `StarTreesBuilder.build()` instead of `buildDuringMerge()`
    - _Requirements: 4.1, 4.2_

  - [x] 2.2 Implement `buildFieldProducerMapFromMergeState()` helper method
    - Add a private method to `Composite912DocValuesWriter` that constructs a `Map<String, DocValuesProducer>` from the merged segment's doc values
    - Iterate over the composite field set and map each field to the appropriate doc values producer from the merge state
    - Handle empty fields using the existing `addDocValuesForEmptyField()` pattern
    - _Requirements: 4.4_

  - [x] 2.3 Handle mixed segments (some with star tree data, some without)
    - In `mergeStarTreeFields()`, when `starTreeSubsPerField` is non-empty but has fewer entries than the number of source segments, handle the mixed case
    - Use `buildDuringMerge()` for the segments that have star tree data
    - _Requirements: 4.3_

  - [ ]* 2.4 Write unit tests for merge path enhancement
    - Test merge with all non-star-tree source segments produces star tree data
    - Test merge with mixed source segments produces correct star tree data
    - **Property 7: Merge builds star trees from raw doc values when no source star tree data exists**
    - **Validates: Requirements 4.1**
    - _Requirements: 4.1, 4.3_

- [x] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement StarTreeUpgradeRequest and response classes
  - [x] 4.1 Create `StarTreeUpgradeRequest` class
    - New class in `server/src/main/java/org/opensearch/action/admin/indices/startree/`
    - Extends `BroadcastRequest<StarTreeUpgradeRequest>`
    - Contains `StarTreeField starTreeField`
    - Implement `writeTo(StreamOutput)` and constructor from `StreamInput`
    - Parse star tree config from XContent request body
    - _Requirements: 1.1, 1.2, 9.1_

  - [x] 4.2 Create `StarTreeUpgradeResponse` class
    - New class in same package
    - Extends `BroadcastResponse`
    - Contains per-shard results (total, successful, failed shards)
    - _Requirements: 6.1, 6.2, 6.3_

  - [x] 4.3 Create `ShardStarTreeUpgradeResult` class
    - New class in same package
    - Extends `TransportBroadcastByNodeAction.EmptyResult` or custom result
    - Contains `ShardId` and `boolean primary`
    - _Requirements: 6.1, 6.2_

  - [ ]* 4.4 Write unit tests for request/response serialization
    - Test StarTreeUpgradeRequest serialization round-trip
    - Test StarTreeUpgradeResponse serialization round-trip
    - Test invalid request body rejection
    - **Property 2: Star tree config transport serialization round-trip**
    - **Validates: Requirements 9.2**
    - **Property 3: Invalid config rejection**
    - **Validates: Requirements 1.3**
    - _Requirements: 1.3, 9.2_

- [x] 5. Implement IndexShard.upgradeToStarTree() method
  - [x] 5.1 Add `upgradeToStarTree()` method to `IndexShard`
    - In `server/src/main/java/org/opensearch/index/shard/IndexShard.java`
    - Block operations, call `resetEngineToGlobalCheckpoint()`, unblock, then call `forceMerge(1)`
    - _Requirements: 3.1, 3.3, 5.1_

- [x] 6. Implement transport action and REST handler
  - [x] 6.1 Create `StarTreeUpgradeAction` action type
    - New class defining the action name and instance
    - _Requirements: 1.1_

  - [x] 6.2 Create `TransportStarTreeUpgradeAction`
    - Extends `TransportBroadcastByNodeAction`
    - Override `doExecute()` to submit mapping update first (with `MergeReason.STAR_TREE_UPGRADE` and bypass flag), then delegate to super for per-shard broadcast
    - Override `shardOperation()` to call `indexShard.upgradeToStarTree()`
    - Implement validation: resolve indices, check primary shards, check for existing star tree config
    - Handle idempotency: skip mapping update if star tree already in mapping, skip force merge if all segments have star tree data
    - _Requirements: 1.1, 1.4, 1.5, 2.5, 5.1, 7.1, 7.2, 8.1, 8.2, 8.3_

  - [x] 6.3 Create mapping update logic in transport action
    - Implement a `ClusterStateTaskExecutor` that creates a `MapperService`, sets the bypass flag on `ParserContext`, merges the star tree mapping with `MergeReason.STAR_TREE_UPGRADE`, and calls `CompositeIndexValidator.validate()` with the upgrade merge reason
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 6.4 Create `RestStarTreeUpgradeAction` REST handler
    - Register `POST /{index}/_star_tree/upgrade` route
    - Parse star tree config from request body
    - Create `StarTreeUpgradeRequest` and delegate to transport action
    - _Requirements: 1.1, 1.2_

  - [x] 6.5 Register the action and REST handler in `ActionModule`
    - Add `StarTreeUpgradeAction` to the action registry
    - Add `RestStarTreeUpgradeAction` to the REST handler registry
    - _Requirements: 1.1_

- [x] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Integration tests
  - [ ]* 8.1 Write internal cluster test for full upgrade flow
    - Create a non-star-tree index, index documents, call upgrade API, verify star tree data exists in segments
    - **Property 6: Index settings invariant during upgrade**
    - **Validates: Requirements 2.7, 2.8**
    - _Requirements: 1.1, 2.7, 2.8, 4.1, 5.3_

  - [ ]* 8.2 Write internal cluster test for idempotent upgrade
    - Upgrade an index, then upgrade again, verify second call succeeds with no additional work
    - _Requirements: 8.1_

  - [ ]* 8.3 Write internal cluster test for error cases
    - Test invalid config rejection, already-configured rejection, missing field rejection
    - _Requirements: 1.3, 1.4, 1.6, 2.6_

  - [ ]* 8.4 Write internal cluster test for partial retry
    - Upgrade with mapping update succeeding but force merge not completing all segments, then retry
    - _Requirements: 8.2_

- [x] 9. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- The implementation language is Java (OpenSearch server codebase)
- Test classes must end with "Tests" (unit) or "IT" (integration) per AGENTS.md conventions
- Run `./gradlew spotlessApply` after code changes to fix formatting
