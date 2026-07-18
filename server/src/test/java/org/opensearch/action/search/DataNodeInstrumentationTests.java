/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for data-node instrumentation of SearchLatencyBreakdown.
 * <p>
 * Verifies:
 * <ul>
 *   <li>SearchLatencyBreakdownNode tree construction from query/fetch phase instrumentation</li>
 *   <li>Max-merge logic when multiple shard results arrive</li>
 *   <li>Null handling for missing breakdown node (mixed-version clusters)</li>
 *   <li>QuerySearchResult shard timing recording via recordShardTiming()</li>
 * </ul>
 * <p>
 * <b>Validates: Requirements 5.1–5.7, 6.1–6.6, 7.1–7.4, 9.1–9.5, 15.1–15.5, 16.1–16.3</b>
 */
public class DataNodeInstrumentationTests extends OpenSearchTestCase {

    // ========== 1. SearchLatencyBreakdownNode tree construction (Query Phase) ==========

    /**
     * Simulates what QueryPhase.execute() does: creates a "Query Phase" node with CATEGORY_PHASE
     * and adds children for query sub-operations.
     * <p>
     * Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 6.1, 6.2, 6.3
     */
    public void testQueryPhaseTreeConstruction_correctParentChildRelationships() {
        // Simulate QueryPhase.execute() building the breakdown tree
        SearchLatencyBreakdownNode queryPhaseNode = new SearchLatencyBreakdownNode(
            "Query Phase",
            SearchLatencyBreakdownNode.CATEGORY_PHASE,
            40_000_000L // 40ms total query phase
        );

        // Add query sub-operation children (like QueryPhase does)
        queryPhaseNode.addChild("Agg Pre-Process", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 5_000_000L);
        queryPhaseNode.addChild("Query Execution", SearchLatencyBreakdownNode.CATEGORY_QUERY, 25_000_000L);
        queryPhaseNode.addChild("Agg Post-Process", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 8_000_000L);

        // Verify parent node
        assertEquals("Query Phase", queryPhaseNode.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_PHASE, queryPhaseNode.getCategory());
        assertEquals(40_000_000L, queryPhaseNode.getDurationNanos());

        // Verify children
        List<SearchLatencyBreakdownNode> children = queryPhaseNode.getChildren();
        assertEquals("Query phase node should have 3 children", 3, children.size());

        // First child: Agg Pre-Process
        SearchLatencyBreakdownNode aggPre = children.get(0);
        assertEquals("Agg Pre-Process", aggPre.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, aggPre.getCategory());
        assertEquals(5_000_000L, aggPre.getDurationNanos());

        // Second child: Query Execution
        SearchLatencyBreakdownNode queryExec = children.get(1);
        assertEquals("Query Execution", queryExec.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_QUERY, queryExec.getCategory());
        assertEquals(25_000_000L, queryExec.getDurationNanos());

        // Third child: Agg Post-Process
        SearchLatencyBreakdownNode aggPost = children.get(2);
        assertEquals("Agg Post-Process", aggPost.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, aggPost.getCategory());
        assertEquals(8_000_000L, aggPost.getDurationNanos());
    }

    /**
     * Tests deeper query phase tree with nested children (e.g., agg sub-timings within Agg Pre-Process).
     * <p>
     * Validates: Requirements 6.1, 6.4, 6.5
     */
    public void testQueryPhaseTreeConstruction_withNestedAggregationChildren() {
        SearchLatencyBreakdownNode queryPhaseNode = new SearchLatencyBreakdownNode(
            "Query Phase",
            SearchLatencyBreakdownNode.CATEGORY_PHASE,
            50_000_000L
        );

        // Add Agg Pre-Process with sub-children
        SearchLatencyBreakdownNode aggPreProcess = new SearchLatencyBreakdownNode(
            "Agg Pre-Process",
            SearchLatencyBreakdownNode.CATEGORY_AGGREGATION,
            10_000_000L
        );
        aggPreProcess.addChild("Agg Initialize", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 3_000_000L);
        aggPreProcess.addChild("Agg Build Leaf Collector", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 7_000_000L);
        queryPhaseNode.addChild(aggPreProcess);

        // Add Agg Post-Process with sub-children
        SearchLatencyBreakdownNode aggPostProcess = new SearchLatencyBreakdownNode(
            "Agg Post-Process",
            SearchLatencyBreakdownNode.CATEGORY_AGGREGATION,
            15_000_000L
        );
        aggPostProcess.addChild("Agg Post Collection", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 5_000_000L);
        aggPostProcess.addChild("Agg Build Aggregation", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 8_000_000L);
        aggPostProcess.addChild("Global Agg Separate Pass", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 2_000_000L);
        queryPhaseNode.addChild(aggPostProcess);

        // Verify hierarchy
        assertEquals(2, queryPhaseNode.getChildren().size());

        SearchLatencyBreakdownNode preNode = queryPhaseNode.getChildren().get(0);
        assertEquals("Agg Pre-Process", preNode.getName());
        assertEquals(2, preNode.getChildren().size());
        assertEquals("Agg Initialize", preNode.getChildren().get(0).getName());
        assertEquals("Agg Build Leaf Collector", preNode.getChildren().get(1).getName());

        SearchLatencyBreakdownNode postNode = queryPhaseNode.getChildren().get(1);
        assertEquals("Agg Post-Process", postNode.getName());
        assertEquals(3, postNode.getChildren().size());
        assertEquals("Agg Post Collection", postNode.getChildren().get(0).getName());
        assertEquals("Agg Build Aggregation", postNode.getChildren().get(1).getName());
        assertEquals("Global Agg Separate Pass", postNode.getChildren().get(2).getName());
    }

    /**
     * Tests that categories are correctly assigned across different query phase operations.
     * <p>
     * Validates: Requirements 5.1–5.6, 7.1–7.4
     */
    public void testQueryPhaseTreeConstruction_correctCategories() {
        SearchLatencyBreakdownNode queryPhaseNode = new SearchLatencyBreakdownNode(
            "Query Phase",
            SearchLatencyBreakdownNode.CATEGORY_PHASE
        );

        // Search context operations
        queryPhaseNode.addChild("Search Ctx Creation", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, 12_000_000L);
        queryPhaseNode.addChild("Acquire Searcher", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, 2_000_000L);

        // Cache operations
        queryPhaseNode.addChild("Request Cache Lookup", SearchLatencyBreakdownNode.CATEGORY_CACHE, 1_000_000L);
        queryPhaseNode.addChild("Query Cache Lookup", SearchLatencyBreakdownNode.CATEGORY_CACHE, 500_000L);

        // Aggregation operations
        queryPhaseNode.addChild("Agg Collect", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, 8_000_000L);

        List<SearchLatencyBreakdownNode> children = queryPhaseNode.getChildren();
        assertEquals(5, children.size());

        // Verify categories
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, children.get(0).getCategory());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, children.get(1).getCategory());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_CACHE, children.get(2).getCategory());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_CACHE, children.get(3).getCategory());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, children.get(4).getCategory());
    }

    // ========== 2. Fetch phase tree construction ==========

    /**
     * Simulates what FetchPhase.execute() does: creates a "Fetch Phase" node with CATEGORY_FETCH
     * and adds children for fetch sub-operations.
     * <p>
     * Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5
     */
    public void testFetchPhaseTreeConstruction_correctParentChildRelationships() {
        SearchLatencyBreakdownNode fetchPhaseNode = new SearchLatencyBreakdownNode(
            "Fetch Phase",
            SearchLatencyBreakdownNode.CATEGORY_FETCH,
            15_000_000L // 15ms total fetch phase
        );

        // Add fetch sub-operation children (like FetchPhase does)
        fetchPhaseNode.addChild("Stored Fields", SearchLatencyBreakdownNode.CATEGORY_FETCH, 5_000_000L);
        fetchPhaseNode.addChild("Source Loading", SearchLatencyBreakdownNode.CATEGORY_FETCH, 3_000_000L);
        fetchPhaseNode.addChild("Highlighting", SearchLatencyBreakdownNode.CATEGORY_FETCH, 4_000_000L);
        fetchPhaseNode.addChild("Script Fields", SearchLatencyBreakdownNode.CATEGORY_FETCH, 2_000_000L);
        fetchPhaseNode.addChild("Inner Hits", SearchLatencyBreakdownNode.CATEGORY_FETCH, 1_000_000L);

        // Verify parent node
        assertEquals("Fetch Phase", fetchPhaseNode.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_FETCH, fetchPhaseNode.getCategory());
        assertEquals(15_000_000L, fetchPhaseNode.getDurationNanos());

        // Verify children
        List<SearchLatencyBreakdownNode> children = fetchPhaseNode.getChildren();
        assertEquals("Fetch phase node should have 5 children", 5, children.size());

        assertEquals("Stored Fields", children.get(0).getName());
        assertEquals(5_000_000L, children.get(0).getDurationNanos());

        assertEquals("Source Loading", children.get(1).getName());
        assertEquals(3_000_000L, children.get(1).getDurationNanos());

        assertEquals("Highlighting", children.get(2).getName());
        assertEquals(4_000_000L, children.get(2).getDurationNanos());

        assertEquals("Script Fields", children.get(3).getName());
        assertEquals(2_000_000L, children.get(3).getDurationNanos());

        assertEquals("Inner Hits", children.get(4).getName());
        assertEquals(1_000_000L, children.get(4).getDurationNanos());

        // All children should have CATEGORY_FETCH
        for (SearchLatencyBreakdownNode child : children) {
            assertEquals(SearchLatencyBreakdownNode.CATEGORY_FETCH, child.getCategory());
        }
    }

    /**
     * Tests that fetch phase node uses timing information (startOffset and endOffset).
     * <p>
     * Validates: Requirements 9.1–9.5, 16.1
     */
    public void testFetchPhaseTreeConstruction_withTimingInfo() {
        long startOffset = 70_000_000L; // 70ms into the request
        long duration = 15_000_000L;    // 15ms

        SearchLatencyBreakdownNode fetchPhaseNode = new SearchLatencyBreakdownNode(
            "Fetch Phase",
            SearchLatencyBreakdownNode.CATEGORY_FETCH,
            startOffset,
            duration
        );

        assertEquals(startOffset, fetchPhaseNode.getStartOffsetNanos());
        assertEquals(startOffset + duration, fetchPhaseNode.getEndOffsetNanos());
        assertEquals(duration, fetchPhaseNode.getDurationNanos());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(startOffset), fetchPhaseNode.getStartOffsetMicros());
        assertEquals(TimeUnit.NANOSECONDS.toMicros(startOffset + duration), fetchPhaseNode.getEndOffsetMicros());
    }

    // ========== 3. Max-merge logic ==========

    /**
     * Simulates AbstractSearchAsyncAction.onShardResult() calling max-aggregation methods
     * with multiple shard data — verifies the final value is the maximum.
     * <p>
     * Validates: Requirements 5.7, 6.6, 7.1–7.4, 9.1–9.5, 16.2
     */
    public void testMaxMergeLogic_selectsMaximumAcrossShards() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Simulate 5 shards returning search_context_creation timings
        long[] shardContextCreationTimes = { 12_000_000L, 8_000_000L, 20_000_000L, 5_000_000L, 15_000_000L };
        for (long time : shardContextCreationTimes) {
            breakdown.recordSearchContextCreation(time);
        }

        // Simulate shard acquire_searcher timings
        long[] shardAcquireSearcherTimes = { 2_000_000L, 5_000_000L, 1_000_000L, 3_000_000L, 4_000_000L };
        for (long time : shardAcquireSearcherTimes) {
            breakdown.recordAcquireSearcher(time);
        }

        // Simulate fetch_stored_fields timings
        long[] shardFetchStoredFieldsTimes = { 3_000_000L, 7_000_000L, 4_000_000L, 9_000_000L, 2_000_000L };
        for (long time : shardFetchStoredFieldsTimes) {
            breakdown.recordFetchStoredFields(time);
        }

        // Get unified map and verify maximums
        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        breakdown.markFirstPhaseStart(absoluteStart + 10_000_000L);
        breakdown.recordQueryPhase(50_000_000L);
        breakdown.recordFetchPhase(20_000_000L);
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // search_context_creation max = 20ms
        assertEquals("search_context_creation should be max (20ms)",
            20L, (long) map.get("search_context_creation"));

        // acquire_searcher max = 5ms
        assertEquals("acquire_searcher should be max (5ms)",
            5L, (long) map.get("acquire_searcher"));

        // fetch_stored_fields max = 9ms
        assertEquals("fetch_stored_fields should be max (9ms)",
            9L, (long) map.get("fetch_stored_fields"));
    }

    /**
     * Tests max-merge for all data node query phase internal metrics.
     * <p>
     * Validates: Requirements 5.1–5.7
     */
    public void testMaxMergeLogic_queryPhaseInternals() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Shard 1: higher values for some metrics
        breakdown.recordSearchIdleReactivation(10_000_000L);
        breakdown.recordReadLockAcquisition(3_000_000L);
        breakdown.recordGlobalOrdinalsLoading(15_000_000L);
        breakdown.recordFielddataLoading(5_000_000L);
        breakdown.recordScriptCompilation(8_000_000L);
        breakdown.recordNestedBitsetConstruction(2_000_000L);
        breakdown.recordStarTreeSetup(4_000_000L);
        breakdown.recordDerivedFieldScript(6_000_000L);

        // Shard 2: higher values for different metrics
        breakdown.recordSearchIdleReactivation(7_000_000L);  // lower
        breakdown.recordReadLockAcquisition(5_000_000L);     // higher
        breakdown.recordGlobalOrdinalsLoading(10_000_000L);  // lower
        breakdown.recordFielddataLoading(12_000_000L);       // higher
        breakdown.recordScriptCompilation(4_000_000L);       // lower
        breakdown.recordNestedBitsetConstruction(9_000_000L);// higher
        breakdown.recordStarTreeSetup(1_000_000L);           // lower
        breakdown.recordDerivedFieldScript(11_000_000L);     // higher

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // Each should be the maximum of the two shard values
        assertEquals(10L, (long) map.get("search_idle_reactivation"));
        assertEquals(5L, (long) map.get("read_lock_acquisition"));
        assertEquals(15L, (long) map.get("global_ordinals_loading"));
        assertEquals(12L, (long) map.get("fielddata_loading"));
        assertEquals(8L, (long) map.get("script_compilation"));
        assertEquals(9L, (long) map.get("nested_bitset_construction"));
        assertEquals(4L, (long) map.get("star_tree_setup"));
        assertEquals(11L, (long) map.get("derived_field_script"));
    }

    /**
     * Tests max-merge for cache metrics.
     * <p>
     * Validates: Requirements 7.1–7.4
     */
    public void testMaxMergeLogic_cacheMetrics() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Shard 1
        breakdown.recordRequestCacheLookup(3_000_000L);
        breakdown.recordRequestCacheWrite(5_000_000L);
        breakdown.recordQueryCacheLookup(2_000_000L);
        breakdown.recordQueryCacheWrite(4_000_000L);

        // Shard 2 (higher for lookup, lower for write)
        breakdown.recordRequestCacheLookup(7_000_000L);
        breakdown.recordRequestCacheWrite(1_000_000L);
        breakdown.recordQueryCacheLookup(8_000_000L);
        breakdown.recordQueryCacheWrite(2_000_000L);

        // Shard 3 (mixed)
        breakdown.recordRequestCacheLookup(4_000_000L);
        breakdown.recordRequestCacheWrite(9_000_000L);
        breakdown.recordQueryCacheLookup(1_000_000L);
        breakdown.recordQueryCacheWrite(6_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals(7L, (long) map.get("request_cache_lookup"));
        assertEquals(9L, (long) map.get("request_cache_write"));
        assertEquals(8L, (long) map.get("query_cache_lookup"));
        assertEquals(6L, (long) map.get("query_cache_write"));
    }

    /**
     * Tests max-merge for fetch phase internal metrics.
     * <p>
     * Validates: Requirements 9.1–9.5
     */
    public void testMaxMergeLogic_fetchPhaseInternals() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Shard 1
        breakdown.recordFetchStoredFields(10_000_000L);
        breakdown.recordFetchSourceLoading(5_000_000L);
        breakdown.recordFetchHighlighting(8_000_000L);
        breakdown.recordFetchScriptFields(3_000_000L);
        breakdown.recordFetchInnerHits(2_000_000L);

        // Shard 2 (higher for some)
        breakdown.recordFetchStoredFields(15_000_000L);
        breakdown.recordFetchSourceLoading(3_000_000L);
        breakdown.recordFetchHighlighting(12_000_000L);
        breakdown.recordFetchScriptFields(1_000_000L);
        breakdown.recordFetchInnerHits(7_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        breakdown.recordFetchPhase(30_000_000L);
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals(15L, (long) map.get("fetch_stored_fields"));
        assertEquals(5L, (long) map.get("fetch_source_loading"));
        assertEquals(12L, (long) map.get("fetch_highlighting"));
        assertEquals(3L, (long) map.get("fetch_script_fields"));
        assertEquals(7L, (long) map.get("fetch_inner_hits"));
    }

    /**
     * Tests max-merge for concurrent segment search metrics.
     * <p>
     * Validates: Requirements 15.1–15.5
     */
    public void testMaxMergeLogic_concurrentSegmentSearchMetrics() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // slice_max_execution uses max-aggregation
        breakdown.recordSliceMaxExecution(20_000_000L);
        breakdown.recordSliceMaxExecution(30_000_000L);
        breakdown.recordSliceMaxExecution(25_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("slice_max_execution should be max (30ms)",
            30L, (long) map.get("slice_max_execution"));
    }

    /**
     * Tests that max-merge with a single shard returns that shard's value.
     * <p>
     * Validates: Requirements 5.7, 16.2
     */
    public void testMaxMergeLogic_singleShard() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        breakdown.recordSearchContextCreation(18_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("Single shard value should be returned directly",
            18L, (long) map.get("search_context_creation"));
    }

    // ========== 4. Null handling for missing breakdown node ==========

    /**
     * Verifies that when getShardLatencyBreakdownNanos() returns null (mixed-version clusters),
     * the coordinator continues without error and existing coordinator metrics still work.
     * <p>
     * Validates: Requirements 16.3, 14.4
     */
    public void testNullHandling_missingBreakdownNodeDoesNotCauseError() {
        // Simulate a QuerySearchResult from an older data node that has no breakdown
        QuerySearchResult queryResult = QuerySearchResult.nullInstance();

        // getShardLatencyBreakdownNanos should return null
        assertNull("Mixed-version node should return null breakdown",
            queryResult.getShardLatencyBreakdownNanos());

        // Simulate what the coordinator does in AbstractSearchAsyncAction.onShardResult()
        // The null check should prevent any NPE
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
        breakdown.recordQueryRewrite(5_000_000L);
        breakdown.recordQueryPhase(40_000_000L);

        // Simulate the coordinator's null check
        Map<String, Long> shardBreakdown = queryResult.getShardLatencyBreakdownNanos();
        if (shardBreakdown != null) {
            // This should NOT execute for mixed-version clusters
            fail("Should not enter shard breakdown merge path when breakdown is null");
        }

        // Coordinator metrics should still work correctly
        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        breakdown.markFirstPhaseStart(absoluteStart + 10_000_000L);
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals("Coordinator metrics should still work: query_rewrite",
            5L, (long) map.get("query_rewrite"));
        assertEquals("Coordinator metrics should still work: query",
            40L, (long) map.get("query"));
        assertEquals("Pre-phase overhead should still be computed",
            10L, (long) map.get("pre_phase_overhead"));
    }

    /**
     * Verifies that getLatencyBreakdownNode() returns null when not set
     * and that the coordinator handles it gracefully.
     * <p>
     * Validates: Requirements 16.3
     */
    public void testNullHandling_latencyBreakdownNodeNotSet() {
        QuerySearchResult queryResult = QuerySearchResult.nullInstance();

        // getLatencyBreakdownNode should return null when not set
        assertNull("Breakdown node should be null when not set",
            queryResult.getLatencyBreakdownNode());

        // The coordinator should handle null gracefully (no NPE)
        SearchLatencyBreakdownNode node = queryResult.getLatencyBreakdownNode();
        if (node != null) {
            fail("Should not enter breakdown node merge path when node is null");
        }

        // The coordinator can still produce a valid breakdown without data-node metrics
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
        breakdown.recordQueryPhase(40_000_000L);
        breakdown.recordFetchPhase(15_000_000L);
        breakdown.recordReduceTopDocs(4_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 200_000_000L;
        breakdown.markFirstPhaseStart(absoluteStart + 5_000_000L);
        breakdown.markPhaseEnd("fetch", requestEnd - 3_000_000L);

        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);
        assertEquals(40L, (long) map.get("query"));
        assertEquals(15L, (long) map.get("fetch"));
        assertEquals(4L, (long) map.get("reduce_top_docs"));

        // Data-node metrics should all be absent
        assertFalse(map.containsKey("search_context_creation"));
        assertFalse(map.containsKey("acquire_searcher"));
        assertFalse(map.containsKey("fetch_stored_fields"));
    }

    /**
     * Tests partial breakdown scenario: some shards have data, some don't.
     * Coordinator should use max from shards that provided data and ignore missing ones.
     * <p>
     * Validates: Requirements 14.4, 16.3
     */
    public void testNullHandling_partialShardData() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Shard 1 has breakdown data
        breakdown.recordSearchContextCreation(12_000_000L);
        breakdown.recordAcquireSearcher(3_000_000L);

        // Shard 2 is null (older node) — we simply skip it (no recording)

        // Shard 3 has breakdown data
        breakdown.recordSearchContextCreation(8_000_000L);
        breakdown.recordAcquireSearcher(5_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // Max should be from the shards that provided data
        assertEquals("search_context_creation max from reporting shards",
            12L, (long) map.get("search_context_creation"));
        assertEquals("acquire_searcher max from reporting shards",
            5L, (long) map.get("acquire_searcher"));
    }

    // ========== 5. QuerySearchResult shard timing recording ==========

    /**
     * Tests recordShardTiming() records multiple timings and getShardLatencyBreakdownNanos()
     * returns correct accumulated values (using Long::sum merge).
     * <p>
     * Validates: Requirements 5.1–5.6, 7.1–7.4, 9.1–9.5, 15.1–15.5
     */
    public void testRecordShardTiming_accumulatesCorrectly() {
        QuerySearchResult result = QuerySearchResult.nullInstance();

        // Record query phase timings
        result.recordShardTiming("search_context_creation", 12_000_000L);
        result.recordShardTiming("acquire_searcher", 3_000_000L);
        result.recordShardTiming("query_execution", 25_000_000L);
        result.recordShardTiming("agg_initialize", 5_000_000L);

        Map<String, Long> breakdown = result.getShardLatencyBreakdownNanos();
        assertNotNull("Breakdown should not be null after recording timings", breakdown);

        assertEquals(12_000_000L, (long) breakdown.get("search_context_creation"));
        assertEquals(3_000_000L, (long) breakdown.get("acquire_searcher"));
        assertEquals(25_000_000L, (long) breakdown.get("query_execution"));
        assertEquals(5_000_000L, (long) breakdown.get("agg_initialize"));
    }

    /**
     * Tests that recordShardTiming() accumulates values for the same key using Long::sum.
     * This happens when the same timing is recorded from multiple operations (e.g., agg_collect
     * called multiple times for different aggregation types).
     * <p>
     * Validates: Requirements 6.2 (agg_collect across multiple aggregators)
     */
    public void testRecordShardTiming_accumulatesSameKeyUsingSum() {
        QuerySearchResult result = QuerySearchResult.nullInstance();

        // Record the same key multiple times (simulates multiple agg collectors)
        result.recordShardTiming("agg_collect", 10_000_000L);
        result.recordShardTiming("agg_collect", 8_000_000L);
        result.recordShardTiming("agg_collect", 5_000_000L);

        Map<String, Long> breakdown = result.getShardLatencyBreakdownNanos();
        assertEquals("agg_collect should be the sum: 23ms in nanos",
            23_000_000L, (long) breakdown.get("agg_collect"));
    }

    /**
     * Tests full shard timing recording flow simulating what QueryPhase and FetchPhase do.
     * <p>
     * Validates: Requirements 5.1–5.6, 6.1–6.5, 7.1–7.4, 9.1–9.5
     */
    public void testRecordShardTiming_fullQueryAndFetchFlow() {
        QuerySearchResult result = QuerySearchResult.nullInstance();

        // === Query Phase timings (simulating SearchService + QueryPhase) ===
        result.recordShardTiming("data_node_queue_wait", 2_000_000L);
        result.recordShardTiming("acquire_reader_context", 1_500_000L);
        result.recordShardTiming("search_context_creation", 5_000_000L);
        result.recordShardTiming("query_pre_process", 3_000_000L);
        result.recordShardTiming("query_internal_execution", 20_000_000L);
        result.recordShardTiming("agg_post_process", 8_000_000L);
        result.recordShardTiming("star_tree_setup", 1_000_000L);
        result.recordShardTiming("agg_initialize", 2_000_000L);
        result.recordShardTiming("agg_collect", 6_000_000L);
        result.recordShardTiming("agg_post_collection", 3_000_000L);
        result.recordShardTiming("agg_build_aggregation", 4_000_000L);

        // === Fetch Phase timings (simulating FetchPhase) ===
        result.recordShardTiming("fetch_execution", 12_000_000L);
        result.recordShardTiming("fetch_stored_fields", 5_000_000L);
        result.recordShardTiming("fetch_source_loading", 3_000_000L);
        result.recordShardTiming("fetch_highlighting", 2_000_000L);
        result.recordShardTiming("fetch_script_fields", 1_000_000L);
        result.recordShardTiming("fetch_inner_hits", 500_000L);

        Map<String, Long> breakdown = result.getShardLatencyBreakdownNanos();
        assertNotNull(breakdown);

        // Verify all query phase timings
        assertEquals(2_000_000L, (long) breakdown.get("data_node_queue_wait"));
        assertEquals(1_500_000L, (long) breakdown.get("acquire_reader_context"));
        assertEquals(5_000_000L, (long) breakdown.get("search_context_creation"));
        assertEquals(3_000_000L, (long) breakdown.get("query_pre_process"));
        assertEquals(20_000_000L, (long) breakdown.get("query_internal_execution"));
        assertEquals(8_000_000L, (long) breakdown.get("agg_post_process"));
        assertEquals(1_000_000L, (long) breakdown.get("star_tree_setup"));
        assertEquals(2_000_000L, (long) breakdown.get("agg_initialize"));
        assertEquals(6_000_000L, (long) breakdown.get("agg_collect"));
        assertEquals(3_000_000L, (long) breakdown.get("agg_post_collection"));
        assertEquals(4_000_000L, (long) breakdown.get("agg_build_aggregation"));

        // Verify all fetch phase timings
        assertEquals(12_000_000L, (long) breakdown.get("fetch_execution"));
        assertEquals(5_000_000L, (long) breakdown.get("fetch_stored_fields"));
        assertEquals(3_000_000L, (long) breakdown.get("fetch_source_loading"));
        assertEquals(2_000_000L, (long) breakdown.get("fetch_highlighting"));
        assertEquals(1_000_000L, (long) breakdown.get("fetch_script_fields"));
        assertEquals(500_000L, (long) breakdown.get("fetch_inner_hits"));
    }

    /**
     * Tests that getShardLatencyBreakdownNanos() returns null before any timing is recorded.
     * <p>
     * Validates: Requirements 16.3
     */
    public void testRecordShardTiming_nullBeforeFirstRecord() {
        QuerySearchResult result = QuerySearchResult.nullInstance();
        assertNull("Breakdown should be null before any timing is recorded",
            result.getShardLatencyBreakdownNanos());
    }

    /**
     * Tests setting and getting the SearchLatencyBreakdownNode on QuerySearchResult.
     * <p>
     * Validates: Requirements 16.1
     */
    public void testQuerySearchResult_setAndGetLatencyBreakdownNode() {
        QuerySearchResult result = QuerySearchResult.nullInstance();

        // Initially null
        assertNull(result.getLatencyBreakdownNode());

        // Set a breakdown node
        SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode(
            "Query Phase",
            SearchLatencyBreakdownNode.CATEGORY_PHASE,
            40_000_000L
        );
        node.addChild("Search Ctx Creation", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, 12_000_000L);

        result.setLatencyBreakdownNode(node);

        // Retrieve and verify
        SearchLatencyBreakdownNode retrieved = result.getLatencyBreakdownNode();
        assertNotNull(retrieved);
        assertEquals("Query Phase", retrieved.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_PHASE, retrieved.getCategory());
        assertEquals(40_000_000L, retrieved.getDurationNanos());
        assertEquals(1, retrieved.getChildren().size());
        assertEquals("Search Ctx Creation", retrieved.getChildren().get(0).getName());
    }

    // ========== 6. Aggregation metrics max-merge ==========

    /**
     * Tests max-merge for aggregation metrics across multiple shards.
     * <p>
     * Validates: Requirements 6.1–6.6
     */
    public void testMaxMergeLogic_aggregationMetrics() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Shard 1
        breakdown.recordAggInitialize(5_000_000L);
        breakdown.recordAggBuildLeafCollector(3_000_000L);
        breakdown.recordAggCollect(20_000_000L);
        breakdown.recordAggPostCollection(4_000_000L);
        breakdown.recordAggDeferredReplay(2_000_000L);
        breakdown.recordAggBuildAggregation(6_000_000L);
        breakdown.recordGlobalAggSeparatePass(8_000_000L);

        // Shard 2 (different max values)
        breakdown.recordAggInitialize(3_000_000L);
        breakdown.recordAggBuildLeafCollector(7_000_000L);
        breakdown.recordAggCollect(15_000_000L);
        breakdown.recordAggPostCollection(9_000_000L);
        breakdown.recordAggDeferredReplay(1_000_000L);
        breakdown.recordAggBuildAggregation(10_000_000L);
        breakdown.recordGlobalAggSeparatePass(4_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 500_000_000L;
        Map<String, Object> map = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        assertEquals(5L, (long) map.get("agg_initialize"));
        assertEquals(7L, (long) map.get("agg_build_leaf_collector"));
        assertEquals(20L, (long) map.get("agg_collect"));
        assertEquals(9L, (long) map.get("agg_post_collection"));
        assertEquals(2L, (long) map.get("agg_deferred_replay"));
        assertEquals(10L, (long) map.get("agg_build_aggregation"));
        assertEquals(8L, (long) map.get("global_agg_separate_pass"));
    }

    // ========== 7. Breakdown tree from coordinator with data-node metrics ==========

    /**
     * Verifies that toBreakdownTree() produces correct tree structure when data-node
     * metrics (query internals, fetch internals) have been merged via max-aggregation.
     * <p>
     * Validates: Requirements 5.7, 9.1–9.5, 16.2
     */
    public void testBreakdownTree_includesDataNodeMetrics() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // Coordinator phase timing
        breakdown.recordQueryPhase(40_000_000L);
        breakdown.recordFetchPhase(15_000_000L);

        // Data-node metrics (max-merged)
        breakdown.recordSearchContextCreation(12_000_000L);
        breakdown.recordAcquireSearcher(3_000_000L);
        breakdown.recordReadLockAcquisition(2_000_000L);
        breakdown.recordGlobalOrdinalsLoading(5_000_000L);
        breakdown.recordAggCollect(8_000_000L);
        breakdown.recordFetchStoredFields(5_000_000L);
        breakdown.recordFetchHighlighting(4_000_000L);

        long absoluteStart = 1_000_000_000L;
        long requestEnd = absoluteStart + 100_000_000L;
        int totalShards = 5;

        SearchLatencyBreakdownNode tree = breakdown.toBreakdownTree(absoluteStart, requestEnd, totalShards);

        assertNotNull(tree);
        assertEquals("Total Request", tree.getName());
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_COORDINATOR, tree.getCategory());

        // Find the Query node
        SearchLatencyBreakdownNode queryNode = null;
        SearchLatencyBreakdownNode fetchNode = null;
        for (SearchLatencyBreakdownNode child : tree.getChildren()) {
            if ("Query".equals(child.getName())) queryNode = child;
            if ("Fetch".equals(child.getName())) fetchNode = child;
        }

        assertNotNull("Tree should contain Query node", queryNode);
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_PHASE, queryNode.getCategory());
        assertTrue("Query node should have children from data-node metrics",
            queryNode.getChildren().size() > 0);

        assertNotNull("Tree should contain Fetch node", fetchNode);
        assertEquals(SearchLatencyBreakdownNode.CATEGORY_FETCH, fetchNode.getCategory());
        assertTrue("Fetch node should have children",
            fetchNode.getChildren().size() > 0);
    }
}
