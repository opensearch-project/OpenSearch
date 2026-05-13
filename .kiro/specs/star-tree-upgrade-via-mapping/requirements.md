# Requirements Document

## Introduction

This feature enables retroactive star tree building on existing OpenSearch indices by working with the existing merge infrastructure. The approach updates the index mapping to add star tree field configuration, restarts the engine so the composite codec is selected, and runs `forceMerge(1)` so the codec's merge path builds star tree data from raw doc values.

The critical constraint is that `index.composite_index` and other Final settings must NOT be changed. Instead, the validation in `ObjectMapper.parseCompositeField()` that checks `IS_COMPOSITE_INDEX_SETTING` must be bypassed for this upgrade path. Since `CodecService` selects the composite codec based on `MapperService.isCompositeIndexPresent()` (which only checks whether composite field types exist in the mapping, not the setting value), no setting change is needed for codec selection. Similarly, the `CompositeIndexValidator` restriction that blocks addition of composite fields during mapping updates must be bypassed for this controlled upgrade operation.

The merge path in `Composite912DocValuesWriter.mergeStarTreeFields()` must also be enhanced: currently it only re-merges existing star tree data from source segments. When source segments have no star tree data (because they were written before the mapping update), the merge path must fall back to building star trees from the raw doc values of the merged segment.

## Glossary

- **Star_Tree_Upgrade_API**: The new REST API endpoint and transport action that orchestrates the star tree upgrade process on an existing index.
- **MapperService**: The OpenSearch service that manages index field mappings and provides composite field types to the codec.
- **Composite912DocValuesWriter**: The doc values writer in the composite codec that builds star tree data during flush and merge operations.
- **StarTreesBuilder**: The existing builder infrastructure that constructs star tree data structures from segment doc values or merges existing star tree data.
- **CompositeIndexValidator**: The validator in `MetadataMappingService` that blocks addition of composite fields during mapping updates on existing indices.
- **CodecService**: The service that selects the codec for an index; uses composite codec when `MapperService.isCompositeIndexPresent()` returns true.
- **IS_COMPOSITE_INDEX_SETTING**: The `index.composite_index` setting (`Setting.Property.Final`) checked in `ObjectMapper.parseCompositeField()` during mapping parsing.
- **MergeReason**: The enum in `MapperService` that indicates why a mapping merge is happening (e.g., `MAPPING_UPDATE`, `MAPPING_RECOVERY`).
- **ParseCompositeField**: The method in `ObjectMapper.TypeParser` that validates and parses composite field definitions, including the `IS_COMPOSITE_INDEX_SETTING` check.
- **MergeStarTreeFields**: The method in `Composite912DocValuesWriter` that collects existing `StarTreeValues` from source segments via `CompositeIndexReader` and delegates to `StarTreesBuilder.buildDuringMerge()`.

## Requirements

### Requirement 1: Star Tree Upgrade API Endpoint

**User Story:** As a cluster operator, I want to trigger a star tree upgrade on an existing index through a dedicated API, so that I can add star tree acceleration to historical data without reindexing.

#### Acceptance Criteria

1. WHEN a `POST /{index}/_star_tree/upgrade` request is received with a valid star tree configuration in the request body, THE Star_Tree_Upgrade_API SHALL initiate the star tree upgrade process on the target index.
2. WHEN the API request body contains the star tree configuration, THE Star_Tree_Upgrade_API SHALL parse it into a StarTreeField object containing ordered dimensions, metrics with stats, and optional build parameters.
3. IF the API request body does not contain a valid star tree configuration (missing dimensions, missing metrics, or malformed structure), THEN THE Star_Tree_Upgrade_API SHALL reject the request with a descriptive error message before any index modifications occur.
4. WHEN the target index already has a star tree field configured in its mapping, THE Star_Tree_Upgrade_API SHALL reject the request with an error indicating the index already has star tree configuration.
5. WHEN the API request specifies target indices, THE Star_Tree_Upgrade_API SHALL resolve the concrete indices and validate that all primary shards are available before proceeding.
6. THE Star_Tree_Upgrade_API SHALL validate the star tree configuration against the index's existing field mappings (dimensions and metric fields must exist and have compatible types) before making any mapping changes.

### Requirement 2: Mapping Update Bypassing Final Setting Checks

**User Story:** As a cluster operator, I want the upgrade API to add the star tree field to the index mapping without requiring `index.composite_index` to be true, so that I can upgrade indexes that were created without composite index settings.

#### Acceptance Criteria

1. WHEN adding the star tree field to the mapping, THE Star_Tree_Upgrade_API SHALL bypass the `IS_COMPOSITE_INDEX_SETTING` check in `ObjectMapper.parseCompositeField()` so that the composite field is parsed even though `index.composite_index` is false.
2. WHEN adding the star tree field to the mapping, THE Star_Tree_Upgrade_API SHALL bypass the `CompositeIndexValidator` restriction that blocks addition of composite fields during mapping updates.
3. WHEN bypassing these validations, THE Star_Tree_Upgrade_API SHALL use a dedicated `MergeReason` (e.g., `STAR_TREE_UPGRADE`) that the parsing and validation code can check to allow the upgrade path.
4. WHEN the mapping update succeeds, THE MapperService SHALL return the star tree field type from `getCompositeFieldTypes()` so that `CodecService` selects the composite codec.
5. WHEN the mapping update succeeds, THE Star_Tree_Upgrade_API SHALL ensure the updated mapping is propagated to all nodes via the cluster state before proceeding with the engine restart.
6. IF the mapping update fails (e.g., dimension fields do not exist in the index, incompatible field types), THEN THE Star_Tree_Upgrade_API SHALL abort the upgrade and return a descriptive error without any side effects.
7. THE Star_Tree_Upgrade_API SHALL NOT modify the `index.composite_index` setting value.
8. THE Star_Tree_Upgrade_API SHALL NOT modify the `index.append_only_enabled` setting value.

### Requirement 3: Engine Restart for Codec Selection

**User Story:** As a cluster operator, I want the engine to be restarted after the mapping update, so that the new CodecService picks up the composite codec for subsequent merge operations.

#### Acceptance Criteria

1. WHEN the mapping update has propagated to all nodes, THE Star_Tree_Upgrade_API SHALL trigger an engine restart on each primary shard so that a new `CodecService` is constructed.
2. WHEN the new `CodecService` is constructed, THE CodecService SHALL select the composite codec because `MapperService.isCompositeIndexPresent()` returns true (the mapping now contains composite field types).
3. WHEN restarting the engine, THE Star_Tree_Upgrade_API SHALL use the existing `resetEngineToGlobalCheckpoint()` mechanism or an equivalent engine close-and-reopen pattern.
4. WHILE the engine restart is in progress on a shard, THE index shard SHALL block write operations and drain in-flight operations.

### Requirement 4: Merge Path Enhancement for Building Star Trees from Raw Doc Values

**User Story:** As a cluster operator, I want the force merge to build star tree data even when source segments have no existing star tree data, so that the upgrade produces star tree indexes from raw doc values.

#### Acceptance Criteria

1. WHEN `Composite912DocValuesWriter.mergeStarTreeFields()` is called during a merge and the MapperService has star tree field configuration but no source segments contain existing star tree data (no `CompositeIndexReader` instances found), THE Composite912DocValuesWriter SHALL fall back to building star tree data from the raw doc values of the merged segment.
2. WHEN falling back to building from raw doc values during merge, THE Composite912DocValuesWriter SHALL construct a `fieldProducerMap` from the merged segment's doc values producers and use the `StarTreesBuilder.build()` path (the same path used during flush).
3. WHEN some source segments have existing star tree data and some do not, THE Composite912DocValuesWriter SHALL use the existing `buildDuringMerge()` path for the segments that have star tree data and handle the mixed case appropriately.
4. WHEN the star tree configuration in MapperService specifies dimensions or metrics that reference fields present in the merged segment's doc values, THE Composite912DocValuesWriter SHALL include those fields in the `fieldProducerMap` for star tree construction.

### Requirement 5: Force Merge Trigger

**User Story:** As a cluster operator, I want the upgrade API to trigger a force merge after the engine restart, so that all segments are rebuilt with star tree data via the codec's merge path.

#### Acceptance Criteria

1. WHEN the engine has been restarted with the composite codec, THE Star_Tree_Upgrade_API SHALL trigger a `forceMerge(1)` on each primary shard of the target index to merge all segments into one.
2. WHEN the force merge executes, THE composite codec SHALL be used for the merged segment because the new engine's `CodecService` selected it during construction.
3. WHEN the force merge completes on a shard, the resulting segment SHALL contain star tree data files built from the raw doc values of the merged documents.
4. WHILE the force merge is in progress on a shard, THE index SHALL remain available for read and search operations serving from the current segments until the merge completes and a refresh makes the new segment visible.

### Requirement 6: Per-Shard Results and Response

**User Story:** As a cluster operator, I want the upgrade API to return per-shard results, so that I can verify the upgrade succeeded on all shards.

#### Acceptance Criteria

1. WHEN a shard upgrade succeeds, THE Star_Tree_Upgrade_API SHALL report the shard identifier and success status in the response.
2. WHEN a shard upgrade fails, THE Star_Tree_Upgrade_API SHALL report the shard identifier, failure status, and error details in the response.
3. WHEN the upgrade completes, THE Star_Tree_Upgrade_API SHALL return a response containing the total number of shards, successful shards, and failed shards.

### Requirement 7: Error Handling and Consistency

**User Story:** As a cluster operator, I want the upgrade to handle errors gracefully and leave the index in a consistent state, so that a failed upgrade does not corrupt the index or leave it partially upgraded.

#### Acceptance Criteria

1. IF the force merge fails on one or more shards, THEN THE Star_Tree_Upgrade_API SHALL report the failures but leave the mapping changes in place, since the mapping update is already committed to the cluster state and a retry of the force merge can complete the upgrade.
2. IF the mapping update fails, THEN THE Star_Tree_Upgrade_API SHALL return an error without any side effects (no settings or mapping changes committed).
3. WHEN a force merge fails on a shard, THE index shard SHALL remain in a consistent state with its pre-merge segments intact, since `forceMerge` uses Lucene's atomic commit protocol.

### Requirement 8: Idempotency and Concurrent Upgrade Safety

**User Story:** As a cluster operator, I want the upgrade to be safe to retry and to reject concurrent upgrades, so that operational errors do not cause data corruption.

#### Acceptance Criteria

1. WHEN a star tree upgrade is requested on an index that already has the star tree field in its mapping and all segments already contain star tree data, THE Star_Tree_Upgrade_API SHALL return a success response indicating no work was needed.
2. WHEN a star tree upgrade is requested on an index that has the star tree field in its mapping but some segments lack star tree data (e.g., from a previous partial failure), THE Star_Tree_Upgrade_API SHALL skip the mapping update and trigger a force merge to complete the upgrade.
3. IF a second upgrade request arrives while an upgrade is already in progress on the same index, THEN THE Star_Tree_Upgrade_API SHALL reject the second request with an appropriate error.

### Requirement 9: Star Tree Configuration Serialization

**User Story:** As a developer, I want the star tree configuration to be correctly serialized and deserialized in the API request, so that the configuration is faithfully transmitted from the client to the upgrade service.

#### Acceptance Criteria

1. THE Star_Tree_Upgrade_API SHALL serialize the star tree configuration from the request body into a StarTreeField object and deserialize it back to an equivalent representation for transport across nodes.
2. FOR ALL valid StarTreeField configurations, serializing to transport wire format and deserializing back SHALL produce an equivalent StarTreeField object (round-trip property).
