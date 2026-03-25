# Implementation Plan: Selective Segment Star Tree Building

## Overview

This plan implements a segment rewrite mechanism that adds Star Tree indices to existing Lucene segments retroactively. The approach maximally reuses existing code by:

1. Opening existing segments and wrapping their doc values as `DocValuesProducer`
2. Feeding these producers into the existing `StarTreesBuilder.build()` method
3. Writing new segment files with star tree data appended
4. Atomically replacing old segments with new ones

**Key Insight**: The existing `BaseStarTreeBuilder.build(Map<String, DocValuesProducer>, ...)` method doesn't care where doc values come from - it just needs the `DocValuesProducer` interface. We'll create an adapter that wraps on-disk doc values from existing segments.

## Prerequisites - Code Understanding

Based on codebase analysis:

- `StarTreesBuilder.build()` accepts `Map<String, DocValuesProducer>` and builds star trees
- `BaseStarTreeBuilder` has two implementations: `OnHeapStarTreeBuilder` and `OffHeapStarTreeBuilder`
- `Composite912DocValuesWriter` writes star tree structures during flush
- Lucene's `SegmentReader` provides access to existing segment doc values via `DocValuesProducer`
- Star tree configuration is stored in `StarTreeMapper` and accessed via `MapperService`

## Implementation Tasks

### Phase 1: Core Segment Rewrite Infrastructure

#### Task 1.1: Create SegmentDocValuesAdapter
**File**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/builder/SegmentDocValuesAdapter.java`

Create an adapter that wraps an existing segment's doc values as a `DocValuesProducer` map suitable for `StarTreesBuilder.build()`.

```java
/**
 * Adapts existing segment doc values to the format expected by StarTreesBuilder.
 * Opens a SegmentReader for an existing segment and exposes its doc values.
 */
public class SegmentDocValuesAdapter {
    private final SegmentReader segmentReader;
    private final Map<String, DocValuesProducer> fieldProducerMap;
    
    public SegmentDocValuesAdapter(Directory directory, SegmentCommitInfo segmentInfo) throws IOException {
        // Open segment reader for existing segment
        // Extract doc values producers for all fields
        // Build map of field name -> DocValuesProducer
    }
    
    public Map<String, DocValuesProducer> getFieldProducerMap() {
        return fieldProducerMap;
    }
    
    public void close() throws IOException {
        // Close segment reader
    }
}
```

**Validates**: Requirement 1.7 (doc values only construction)

---

#### Task 1.2: Create SegmentStarTreeRewriter
**File**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/builder/SegmentStarTreeRewriter.java`

Core component that orchestrates the segment rewrite process.

```java
/**
 * Rewrites an existing segment to add star tree structures.
 * Creates new segment files with incremented generation number.
 */
public class SegmentStarTreeRewriter {
    private final StarTreeField starTreeField;
    private final MapperService mapperService;
    
    /**
     * Rewrites a segment to include star tree structures.
     * 
     * @param segmentInfo existing segment to rewrite
     * @param directory segment directory
     * @return new SegmentCommitInfo with star tree
     */
    public SegmentCommitInfo rewriteSegment(
        SegmentCommitInfo segmentInfo,
        Directory directory
    ) throws IOException {
        // 1. Validate segment eligibility (has required doc values)
        validateSegmentEligibility(segmentInfo);
        
        // 2. Open existing segment doc values via adapter
        try (SegmentDocValuesAdapter adapter = new SegmentDocValuesAdapter(directory, segmentInfo)) {
            
            // 3. Create new SegmentInfo with same name but new generation
            SegmentInfo newSegmentInfo = createNewSegmentInfo(segmentInfo);
            
            // 4. Create SegmentWriteState for star tree builder
            SegmentWriteState writeState = createWriteState(newSegmentInfo, directory);
            
            // 5. Build star tree using existing StarTreesBuilder
            buildStarTree(adapter.getFieldProducerMap(), writeState, directory);
            
            // 6. Copy original segment files to new generation (if needed)
            // Note: Star tree is additive - original data remains unchanged
            
            // 7. Return new SegmentCommitInfo
            return new SegmentCommitInfo(newSegmentInfo, 0, 0, -1, -1, -1, StringHelper.randomId());
        }
    }
    
    private void validateSegmentEligibility(SegmentCommitInfo segmentInfo) throws IOException {
        // Check all dimension fields have doc values
        // Check all metric fields have doc values
        // Throw IllegalArgumentException if validation fails
    }
    
    private void buildStarTree(
        Map<String, DocValuesProducer> fieldProducerMap,
        SegmentWriteState writeState,
        Directory directory
    ) throws IOException {
        // Create output files for star tree
        IndexOutput metaOut = directory.createOutput(
            getStarTreeMetaFileName(writeState.segmentInfo.name),
            IOContext.DEFAULT
        );
        IndexOutput dataOut = directory.createOutput(
            getStarTreeDataFileName(writeState.segmentInfo.name),
            IOContext.DEFAULT
        );
        
        try {
            // Create StarTreesBuilder and build
            AtomicInteger fieldNumber = new AtomicInteger(0);
            StarTreesBuilder builder = new StarTreesBuilder(writeState, mapperService, fieldNumber);
            
            // Create doc values consumer for star tree doc values
            DocValuesConsumer consumer = createDocValuesConsumer(writeState, directory);
            
            // Build star tree - reuses existing logic!
            builder.build(metaOut, dataOut, fieldProducerMap, consumer);
        } finally {
            IOUtils.close(metaOut, dataOut);
        }
    }
}
```

**Validates**: Requirements 1.1, 1.2, 1.6, 5.1, 5.2, 5.3

---

#### Task 1.3: Implement Segment Eligibility Validation
**Location**: Within `SegmentStarTreeRewriter.validateSegmentEligibility()`

Validate that a segment has all required doc values before attempting to build star tree.

```java
private void validateSegmentEligibility(SegmentCommitInfo segmentInfo) throws IOException {
    FieldInfos fieldInfos = segmentInfo.info.getFieldInfos();
    
    // Check dimensions
    for (Dimension dim : starTreeField.getDimensions()) {
        FieldInfo fieldInfo = fieldInfos.fieldInfo(dim.getField());
        if (fieldInfo == null || fieldInfo.getDocValuesType() == DocValuesType.NONE) {
            throw new IllegalArgumentException(
                "Dimension field " + dim.getField() + " missing doc values in segment " + segmentInfo.info.name
            );
        }
    }
    
    // Check metrics
    for (Metric metric : starTreeField.getMetrics()) {
        FieldInfo fieldInfo = fieldInfos.fieldInfo(metric.getField());
        if (fieldInfo == null || fieldInfo.getDocValuesType() == DocValuesType.NONE) {
            throw new IllegalArgumentException(
                "Metric field " + metric.getField() + " missing doc values in segment " + segmentInfo.info.name
            );
        }
    }
}
```

**Validates**: Requirements 1.1, 1.2, 1.3, 1.4, 1.5

---

### Phase 2: Integration with Index Engine

#### Task 2.1: Add Segment Rewrite API to InternalEngine
**File**: `server/src/main/java/org/opensearch/index/engine/InternalEngine.java`

Add a method to trigger segment rewrite for specific segments.

```java
/**
 * Rewrites specified segments to add star tree structures.
 * 
 * @param segmentNames names of segments to rewrite
 * @return list of rewritten segment names
 */
public List<String> rewriteSegmentsWithStarTree(List<String> segmentNames) throws IOException {
    ensureOpen();
    
    List<String> rewrittenSegments = new ArrayList<>();
    
    try (ReleasableLock lock = readLock.acquire()) {
        SegmentInfos segmentInfos = getLastCommittedSegmentInfos();
        
        for (SegmentCommitInfo segmentInfo : segmentInfos) {
            if (segmentNames.contains(segmentInfo.info.name)) {
                // Get star tree configuration from mapper service
                StarTreeField starTreeField = getStarTreeFieldFromMapper();
                
                if (starTreeField != null) {
                    // Rewrite segment
                    SegmentStarTreeRewriter rewriter = new SegmentStarTreeRewriter(
                        starTreeField,
                        mapperService
                    );
                    
                    SegmentCommitInfo newSegmentInfo = rewriter.rewriteSegment(
                        segmentInfo,
                        store.directory()
                    );
                    
                    // Replace old segment with new one in segment infos
                    replaceSegment(segmentInfos, segmentInfo, newSegmentInfo);
                    rewrittenSegments.add(newSegmentInfo.info.name);
                }
            }
        }
        
        // Commit new segment infos
        if (!rewrittenSegments.isEmpty()) {
            commitSegmentInfos(segmentInfos);
        }
    }
    
    return rewrittenSegments;
}

private StarTreeField getStarTreeFieldFromMapper() {
    // Extract star tree configuration from MapperService
    for (CompositeMappedFieldType fieldType : mapperService.getCompositeFieldTypes()) {
        if (fieldType.unwrap() instanceof StarTreeMapper.StarTreeFieldType starTreeFieldType) {
            return new StarTreeField(
                starTreeFieldType.name(),
                starTreeFieldType.getDimensions(),
                starTreeFieldType.getMetrics(),
                starTreeFieldType.getStarTreeConfig()
            );
        }
    }
    return null;
}
```

**Validates**: Requirements 5.5, 5.6

---

#### Task 2.2: Add REST API Endpoint
**File**: `server/src/main/java/org/opensearch/rest/action/admin/indices/RestBuildStarTreeAction.java`

Create REST endpoint `POST /{index}/_build_star_tree` to trigger segment rewrite.

```java
/**
 * REST handler for building star tree on existing segments.
 * 
 * POST /{index}/_build_star_tree
 * {
 *   "segments": ["_0", "_1"]  // optional: specific segments
 * }
 */
public class RestBuildStarTreeAction extends BaseRestHandler {
    
    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_build_star_tree")
        );
    }
    
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String index = request.param("index");
        String[] segments = request.paramAsStringArray("segments", Strings.EMPTY_ARRAY);
        
        BuildStarTreeRequest buildRequest = new BuildStarTreeRequest(index, segments);
        
        return channel -> client.admin().indices().buildStarTree(
            buildRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
```

**Validates**: Requirement 3.1

---

#### Task 2.3: Create Transport Action
**File**: `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportBuildStarTreeAction.java`

Transport action to coordinate segment rewrite across shards.

```java
/**
 * Transport action for building star tree on existing segments.
 */
public class TransportBuildStarTreeAction extends TransportBroadcastAction<
    BuildStarTreeRequest,
    BuildStarTreeResponse,
    ShardBuildStarTreeRequest,
    ShardBuildStarTreeResponse> {
    
    @Override
    protected ShardBuildStarTreeResponse shardOperation(
        ShardBuildStarTreeRequest request,
        Task task
    ) throws IOException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.shardId().getIndex())
            .getShard(request.shardId().id());
        
        // Trigger segment rewrite on this shard
        List<String> rewrittenSegments = indexShard.rewriteSegmentsWithStarTree(
            Arrays.asList(request.getSegments())
        );
        
        return new ShardBuildStarTreeResponse(request.shardId(), rewrittenSegments);
    }
}
```

**Validates**: Requirements 3.2, 3.3

---

### Phase 3: Segment Discovery and Selection

#### How Users Discover and Select Segments

The existing `GET /{index}/_segments` API already exposes all the information users need. Here's what a typical response looks like:

```json
GET /my-index/_segments
{
  "indices": {
    "my-index": {
      "shards": {
        "0": [{
          "routing": { "state": "STARTED", "primary": true, "node": "node1" },
          "segments": {
            "_0": {
              "generation": 0,
              "num_docs": 50000,
              "deleted_docs": 0,
              "size_in_bytes": 104857600,
              "committed": true,
              "search": true,
              "attributes": {}
            },
            "_1": {
              "generation": 1,
              "num_docs": 30000,
              "deleted_docs": 100,
              "size_in_bytes": 62914560,
              "committed": true,
              "search": true,
              "attributes": { "star_tree.present": "true" }
            },
            "_2": {
              "generation": 2,
              "num_docs": 10000,
              "deleted_docs": 0,
              "size_in_bytes": 20971520,
              "committed": true,
              "search": true,
              "attributes": {}
            }
          }
        }]
      }
    }
  }
}
```

Key fields for segment selection:
- **`name`** (e.g., `_0`, `_1`, `_2`): The Lucene segment name — this is the identifier users pass to the build API
- **`size_in_bytes`**: Segment size — users can prioritize large segments (more benefit from star tree)
- **`num_docs`**: Document count — another way to gauge segment importance
- **`attributes.star_tree.present`**: Whether star tree already exists — segments with this set to `"true"` are skipped automatically

#### Segment Selection Modes

The `_build_star_tree` API supports three selection modes:

**Mode 1: Explicit segment names** (most precise)
```json
POST /my-index/_build_star_tree
{
  "segments": ["_0", "_2"]
}
```
User looked at `_segments` output, picked `_0` and `_2` (the ones without `star_tree.present`).

**Mode 2: All eligible segments** (default — no `segments` field)
```json
POST /my-index/_build_star_tree
{}
```
Automatically selects all segments that don't already have star tree. This is the simplest path.

**Mode 3: Filtered selection** (size-based)
```json
POST /my-index/_build_star_tree
{
  "min_size_bytes": 52428800
}
```
Only rewrite segments larger than 50MB that don't already have star tree. Useful for skipping tiny segments where star tree overhead isn't worth it.

#### Task 3.1: Implement Segment Selection Logic
**File**: `server/src/main/java/org/opensearch/action/admin/indices/startree/SegmentSelector.java`

```java
/**
 * Selects segments for star tree building.
 * 
 * Selection logic:
 * 1. If explicit segment names provided → use those (still skip if already has star tree)
 * 2. If no names provided → select all segments without star tree
 * 3. Apply optional size filter on top of either mode
 */
public class SegmentSelector {
    
    public List<SegmentCommitInfo> selectSegments(
        SegmentInfos segmentInfos,
        @Nullable List<String> requestedSegmentNames,
        long minSizeBytes
    ) {
        List<SegmentCommitInfo> selected = new ArrayList<>();
        
        for (SegmentCommitInfo segmentInfo : segmentInfos) {
            // Always skip segments that already have star tree
            if (hasStarTree(segmentInfo)) {
                continue;
            }
            
            // If explicit names provided, only include those
            if (requestedSegmentNames != null && !requestedSegmentNames.isEmpty()) {
                if (!requestedSegmentNames.contains(segmentInfo.info.name)) {
                    continue;
                }
            }
            
            // Apply size filter
            if (minSizeBytes > 0) {
                try {
                    if (segmentInfo.sizeInBytes() < minSizeBytes) {
                        continue;
                    }
                } catch (IOException e) {
                    // If we can't determine size, include the segment
                }
            }
            
            selected.add(segmentInfo);
        }
        
        return selected;
    }
    
    private boolean hasStarTree(SegmentCommitInfo segmentInfo) {
        Map<String, String> attributes = segmentInfo.info.getAttributes();
        return attributes != null 
            && Boolean.parseBoolean(attributes.getOrDefault("star_tree.present", "false"));
    }
}
```

**Validates**: Requirements 3.4, 3.5

#### Validation: What happens with bad segment names?

If a user requests a segment name that doesn't exist (e.g., `"segments": ["_99"]`), the API returns an error listing the unknown segments:

```json
{
  "error": {
    "type": "illegal_argument_exception",
    "reason": "Unknown segments: [_99]. Use GET /{index}/_segments to see available segments."
  }
}
```

If a user requests a segment that already has star tree, it's silently skipped and reported in the response:

```json
{
  "acknowledged": true,
  "segments_rewritten": 1,
  "segments_skipped": 1,
  "skipped_details": {
    "_1": "already has star tree"
  }
}
```

---

### Phase 4: Atomic Segment Replacement

#### Task 4.1: Implement Atomic Segment Replacement
**Location**: Within `InternalEngine.rewriteSegmentsWithStarTree()`

Ensure segment replacement is atomic and crash-safe.

```java
private void replaceSegment(
    SegmentInfos segmentInfos,
    SegmentCommitInfo oldSegment,
    SegmentCommitInfo newSegment
) throws IOException {
    // Find and replace old segment with new one
    int index = segmentInfos.asList().indexOf(oldSegment);
    if (index >= 0) {
        segmentInfos.remove(index);
        segmentInfos.add(index, newSegment);
    }
}

private void commitSegmentInfos(SegmentInfos segmentInfos) throws IOException {
    // Write new segments_N file
    // This is atomic - either fully written or not at all
    segmentInfos.commit(store.directory());
    
    // Delete old segment files after successful commit
    // Lucene's IndexFileDeleter handles this automatically
}
```

**Validates**: Requirements 5.5, 5.6, 5.7

---

### Phase 5: Segment Metadata Management

#### Task 5.1: Add Star Tree Metadata to Segments
**Location**: Within `SegmentStarTreeRewriter`

Store star tree presence and configuration in segment attributes.

```java
private SegmentInfo createNewSegmentInfo(SegmentCommitInfo oldSegmentInfo) {
    SegmentInfo oldInfo = oldSegmentInfo.info;
    
    // Create new segment info with same data
    SegmentInfo newInfo = new SegmentInfo(
        oldInfo.dir,
        oldInfo.getVersion(),
        oldInfo.getMinVersion(),
        oldInfo.name,
        oldInfo.maxDoc(),
        oldInfo.getUseCompoundFile(),
        oldInfo.getCodec(),
        oldInfo.getDiagnostics(),
        oldInfo.getId(),
        new HashMap<>(oldInfo.getAttributes()),
        oldInfo.getIndexSort()
    );
    
    // Add star tree metadata
    Map<String, String> attributes = new HashMap<>(newInfo.getAttributes());
    attributes.put("star_tree.present", "true");
    attributes.put("star_tree.config_hash", computeConfigHash(starTreeField));
    attributes.put("star_tree.build_timestamp", String.valueOf(System.currentTimeMillis()));
    newInfo.setAttributes(attributes);
    
    return newInfo;
}

private String computeConfigHash(StarTreeField starTreeField) {
    // Hash star tree configuration for version tracking
    try {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        // Hash dimensions, metrics, and config
        return Base64.getEncoder().encodeToString(digest.digest());
    } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
    }
}
```

**Validates**: Requirements 5.4, 6.1, 6.2

---

### Phase 6: Testing and Validation

#### Task 6.1: Unit Tests for SegmentDocValuesAdapter
**File**: `server/src/test/java/org/opensearch/index/compositeindex/datacube/startree/builder/SegmentDocValuesAdapterTests.java`

Test that adapter correctly wraps existing segment doc values.

```java
public class SegmentDocValuesAdapterTests extends OpenSearchTestCase {
    
    public void testAdapterWrapsDocValues() throws IOException {
        // Create test segment with doc values
        // Open adapter
        // Verify field producer map contains expected fields
        // Verify doc values can be read
    }
    
    public void testAdapterHandlesMissingFields() throws IOException {
        // Create segment missing some fields
        // Verify adapter handles gracefully
    }
}
```

---

#### Task 6.2: Integration Test for Segment Rewrite
**File**: `server/src/test/java/org/opensearch/index/compositeindex/datacube/startree/builder/SegmentStarTreeRewriterIT.java`

End-to-end test of segment rewrite process.

```java
public class SegmentStarTreeRewriterIT extends OpenSearchIntegTestCase {
    
    public void testRewriteSegmentWithStarTree() throws Exception {
        // 1. Create index without star tree
        // 2. Index documents
        // 3. Flush to create segments
        // 4. Add star tree configuration
        // 5. Trigger segment rewrite via API
        // 6. Verify segments now have star tree
        // 7. Verify queries can use star tree
    }
    
    public void testRewritePreservesOriginalData() throws Exception {
        // Verify all original documents still searchable after rewrite
    }
}
```

---

#### Task 6.3: Test Segment Selection Logic
**File**: `server/src/test/java/org/opensearch/action/admin/indices/startree/SegmentSelectorTests.java`

Test segment selection criteria.

```java
public class SegmentSelectorTests extends OpenSearchTestCase {
    
    public void testDefaultSelectsAllWithoutStarTree() {
        // No explicit names, no size filter → selects all segments without star_tree.present
    }
    
    public void testExplicitSegmentNames() {
        // Only selects named segments, skips others
    }
    
    public void testSkipsSegmentsWithStarTree() {
        // Segments with star_tree.present=true are always skipped, even if explicitly named
    }
    
    public void testMinSizeFilter() {
        // Segments below min_size_bytes are skipped
    }
    
    public void testExplicitNamesWithSizeFilter() {
        // Both filters applied together: explicit names AND size filter
    }
    
    public void testEmptySegmentInfos() {
        // No segments → empty result
    }
}
```

---

### Phase 6.5: Optional Async Processing (Recommended for Production)

#### Task 6.5: Add Async Task Support
**File**: `server/src/main/java/org/opensearch/action/admin/indices/startree/StarTreeBuildTask.java`

Make segment rewrite asynchronous for better scalability.

```java
/**
 * Persistent task for building star tree on segments.
 * Allows long-running builds without blocking API calls.
 */
public class StarTreeBuildTask extends AllocatedPersistentTask {
    private final String indexName;
    private final List<String> targetSegments;
    private final AtomicInteger segmentsCompleted = new AtomicInteger(0);
    private final AtomicInteger segmentsFailed = new AtomicInteger(0);
    
    public StarTreeBuildTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        String indexName,
        List<String> targetSegments
    ) {
        super(id, type, action, description, parentTaskId, headers);
        this.indexName = indexName;
        this.targetSegments = targetSegments;
    }
    
    @Override
    public Status getStatus() {
        return new StarTreeBuildTaskStatus(
            segmentsCompleted.get(),
            targetSegments.size(),
            segmentsFailed.get()
        );
    }
    
    public void incrementCompleted() {
        segmentsCompleted.incrementAndGet();
    }
    
    public void incrementFailed() {
        segmentsFailed.incrementAndGet();
    }
}
```

**Modify Task 2.2**: Update REST API to support async mode:

```java
@Override
protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    String index = request.param("index");
    String[] segments = request.paramAsStringArray("segments", Strings.EMPTY_ARRAY);
    boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
    
    BuildStarTreeRequest buildRequest = new BuildStarTreeRequest(index, segments);
    buildRequest.setWaitForCompletion(waitForCompletion);
    
    return channel -> client.admin().indices().buildStarTree(
        buildRequest,
        new RestToXContentListener<>(channel)
    );
}
```

**Modify Task 2.3**: Update transport action to create persistent task:

```java
@Override
protected void doExecute(Task task, BuildStarTreeRequest request, ActionListener<BuildStarTreeResponse> listener) {
    if (request.waitForCompletion()) {
        // Synchronous mode - process immediately
        super.doExecute(task, request, listener);
    } else {
        // Async mode - create persistent task
        PersistentTasksService persistentTasksService = // inject
        
        persistentTasksService.sendStartRequest(
            UUIDs.base64UUID(),
            StarTreeBuildTask.TASK_NAME,
            new StarTreeBuildTaskParams(request.index(), Arrays.asList(request.segments())),
            ActionListener.wrap(
                persistentTask -> {
                    listener.onResponse(new BuildStarTreeResponse(
                        persistentTask.getId(),
                        true,
                        0,
                        0
                    ));
                },
                listener::onFailure
            )
        );
    }
}
```

**Benefits**:
- API returns immediately with task ID
- Users can query task progress via `GET /_tasks/{task_id}`
- Long-running builds don't block API calls
- Task survives node restarts

**Usage**:
```bash
# Start async build
POST /my-index/_build_star_tree
{
  "wait_for_completion": false
}

# Response
{
  "task": "node1:12345",
  "acknowledged": true
}

# Check progress
GET /_tasks/node1:12345
```

---

### Phase 7: Documentation and Configuration

#### Task 7.1: Add Configuration Settings
**File**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeIndexSettings.java`

Add settings for segment rewrite behavior.

```java
public static final Setting<Boolean> STAR_TREE_SEGMENT_REWRITE_ENABLED = Setting.boolSetting(
    "index.star_tree.segment_rewrite.enabled",
    true,
    Setting.Property.IndexScope,
    Setting.Property.Dynamic
);

public static final Setting<ByteSizeValue> STAR_TREE_MIN_SEGMENT_SIZE = Setting.byteSizeSetting(
    "index.star_tree.segment_rewrite.min_segment_size",
    new ByteSizeValue(100, ByteSizeUnit.MB),
    Setting.Property.IndexScope,
    Setting.Property.Dynamic
);
```

---

#### Task 7.2: API Documentation
**File**: `docs/reference/indices/build-star-tree.md`

Document the new API endpoint.

```markdown
# Build Star Tree API

Builds star tree indices on existing segments retroactively.

## Request

`POST /{index}/_build_star_tree`

## Request Body

- `segments` (optional): Array of segment names to rewrite. If not specified, all eligible segments are rewritten.

## Example

```json
POST /my-index/_build_star_tree
{
  "segments": ["_0", "_1"]
}
```

## Response

```json
{
  "acknowledged": true,
  "segments_rewritten": 2,
  "segments_skipped": 0
}
```
```

---

## Implementation Order

### Minimal MVP (Synchronous)
1. **Phase 1** (Core Infrastructure): Tasks 1.1, 1.2, 1.3
2. **Phase 5** (Metadata): Task 5.1 (needed for Phase 2)
3. **Phase 2** (Engine Integration): Tasks 2.1, 2.2, 2.3
4. **Phase 3** (Selection): Task 3.1
5. **Phase 4** (Atomicity): Task 4.1
6. **Phase 6** (Testing): Tasks 6.1, 6.2, 6.3
7. **Phase 7** (Documentation): Tasks 7.1, 7.2

**Result**: Working segment rewrite, but synchronous (blocks API call)

### Production-Ready (Async)
8. **Phase 6.5** (Async Support): Task 6.5

**Result**: Non-blocking API with task management for production use

### Recommended Approach
- Start with MVP to validate the core mechanism works
- Add async support (Phase 6.5) before deploying to production
- This allows testing the rewrite logic without the complexity of task management

## Key Design Decisions

1. **Maximum Code Reuse**: Use existing `StarTreesBuilder.build()` method unchanged
2. **Adapter Pattern**: Wrap existing segment doc values as `DocValuesProducer` map
3. **Additive Approach**: Star tree files are added; original segment data unchanged
4. **Atomic Replacement**: Use Lucene's segment commit mechanism for atomicity
5. **Minimal API Surface**: Single REST endpoint with simple request/response

## Success Criteria

- Segments without star tree can have star tree added retroactively
- Original segment data remains unchanged and searchable
- Star tree building reuses 100% of existing `BaseStarTreeBuilder` logic
- Segment replacement is atomic and crash-safe
- API is simple and intuitive

## Performance Considerations

**Problem**: Processing many segments sequentially would block the API call for too long.

**Solutions Implemented**:

1. **Selective Processing**: The API allows specifying exactly which segments to process, so users can batch the work themselves
2. **Size Filtering**: Built-in `min_segment_size` setting skips small segments that don't benefit from star tree
3. **Segment Selection**: Users can target specific segments (e.g., largest ones first) rather than all at once

**Recommended Usage Pattern**:
```bash
# Process only large segments (>1GB)
POST /my-index/_build_star_tree
{
  "min_size_bytes": 1073741824
}

# Or process specific segments in batches
POST /my-index/_build_star_tree
{
  "segments": ["_0", "_1", "_2"]  # Batch 1
}

POST /my-index/_build_star_tree
{
  "segments": ["_3", "_4", "_5"]  # Batch 2
}
```

## Out of Scope (Future Work)

The following features would further improve scalability but are not in the initial implementation:

- **Async/background processing with task management**: Return task ID immediately, process in background
- **Parallel segment processing**: Process multiple segments concurrently
- **Merge-based star tree building**: Build during merges to avoid separate rewrite step
- **Replica synchronization**: Coordinate rewrite across replicas
- **Resource throttling**: Limit CPU/memory usage during builds
- **Verification API**: Validate star tree correctness after building
- **Snapshot/restore integration**: Handle star trees in snapshots

These can be added incrementally after the core mechanism is proven.
