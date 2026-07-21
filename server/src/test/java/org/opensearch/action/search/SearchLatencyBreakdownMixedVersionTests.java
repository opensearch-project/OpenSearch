/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for mixed-version cluster behavior with SearchLatencyBreakdown.
 * <p>
 * Simulates the scenario where some data nodes run newer versions (returning breakdown data)
 * and others run older versions (returning no breakdown data). The coordinator must produce
 * a valid partial breakdown without errors, merging only available shard data.
 * <p>
 * <b>Validates: Requirements 14.2, 14.4, 16.3</b>
 */
public class SearchLatencyBreakdownMixedVersionTests extends OpenSearchTestCase {

    /**
     * Data-node metric keys that are populated via max-aggregation from shard responses.
     */
    private static final Set<String> DATA_NODE_METRIC_KEYS = new HashSet<>(Arrays.asList(
        "search_context_creation",
        "acquire_searcher",
        "global_ordinals_loading",
        "script_compilation",
        "nested_bitset_construction",
        "star_tree_setup",
        "request_cache_lookup",
        "request_cache_write",
        "query_cache_lookup",
        "query_cache_write",
        "fetch_stored_fields",
        "fetch_source_loading",
        "fetch_highlighting",
        "fetch_script_fields",
        "fetch_inner_hits",
        "agg_initialize",
        "agg_collect",
        "agg_post_collection",
        "agg_build_aggregation",
        "global_agg_separate_pass"
    ));

    /**
     * Test 1: Mixed-version cluster with partial breakdown.
     * <p>
     * Simulates a scenario where 5 shards participate in search:
     * - 2 shards are on new data nodes that return breakdown data (merged via max-aggregation)
     * - 3 shards are on old data nodes that return null breakdown data (skipped gracefully)
     * <p>
     * Verifies the coordinator produces a valid partial breakdown containing:
     * - Coordinator-level metrics (pre-phase, phases, gaps, reduce)
     * - Data-node metrics from only the shards that reported them (new nodes)
     * - No nulls or exceptions thrown
     */
    public void testMixedVersionCluster_partialBreakdown() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        // --- Coordinator-level metrics ---
        long absoluteStart = 1_000_000_000L;

        // Pre-phase: pipeline transform, query rewrite, shard routing
        long pipelineTransformNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordPipelineRequestTransform(pipelineTransformNanos);

        long queryRewriteNanos = TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordQueryRewrite(queryRewriteNanos);

        long shardRoutingNanos = TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordShardRouting(shardRoutingNanos);

        long indexResolutionNanos = TimeUnit.MILLISECONDS.toNanos(1);
        breakdown.recordIndexResolution(indexResolutionNanos);

        // Mark first phase start
        long prePhaseTotal = pipelineTransformNanos + queryRewriteNanos + shardRoutingNanos + indexResolutionNanos;
        long firstPhaseStart = absoluteStart + prePhaseTotal;
        breakdown.markFirstPhaseStart(firstPhaseStart);

        // Query phase (coordinator-measured phase duration)
        long queryPhaseNanos = TimeUnit.MILLISECONDS.toNanos(60);
        breakdown.recordQueryPhase(queryPhaseNanos);
        long queryPhaseEnd = firstPhaseStart + queryPhaseNanos;
        breakdown.markPhaseEnd("query", queryPhaseEnd);

        // Inter-phase gap: query → fetch
        long queryToFetchGapNanos = TimeUnit.MILLISECONDS.toNanos(4);
        breakdown.recordInterPhaseGap("query", "fetch", queryToFetchGapNanos);

        // Fetch phase
        long fetchPhaseNanos = TimeUnit.MILLISECONDS.toNanos(18);
        breakdown.recordFetchPhase(fetchPhaseNanos);
        long fetchPhaseEnd = queryPhaseEnd + queryToFetchGapNanos + fetchPhaseNanos;
        breakdown.markPhaseEnd("fetch", fetchPhaseEnd);

        // Reduce
        long reduceTopDocsNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordReduceTopDocs(reduceTopDocsNanos);
        long reduceAggsNanos = TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordReduceAggregations(reduceAggsNanos);

        // --- Simulate shard results from NEW data nodes (2 out of 5 shards) ---
        // Shard 1 (new node): reports breakdown data
        long shard1ContextCreation = TimeUnit.MILLISECONDS.toNanos(8);
        long shard1AcquireSearcher = TimeUnit.MILLISECONDS.toNanos(2);
        long shard1FetchStoredFields = TimeUnit.MILLISECONDS.toNanos(4);
        long shard1AggCollect = TimeUnit.MILLISECONDS.toNanos(12);

        breakdown.recordSearchContextCreation(shard1ContextCreation);
        breakdown.recordAcquireSearcher(shard1AcquireSearcher);
        breakdown.recordFetchStoredFields(shard1FetchStoredFields);
        breakdown.recordAggCollect(shard1AggCollect);

        // Shard 2 (new node): reports breakdown data (different values, max-aggregated)
        long shard2ContextCreation = TimeUnit.MILLISECONDS.toNanos(15); // Higher → becomes the max
        long shard2AcquireSearcher = TimeUnit.MILLISECONDS.toNanos(1);  // Lower → shard 1 stays
        long shard2FetchStoredFields = TimeUnit.MILLISECONDS.toNanos(6); // Higher → becomes the max
        long shard2AggCollect = TimeUnit.MILLISECONDS.toNanos(9);        // Lower → shard 1 stays

        breakdown.recordSearchContextCreation(shard2ContextCreation);
        breakdown.recordAcquireSearcher(shard2AcquireSearcher);
        breakdown.recordFetchStoredFields(shard2FetchStoredFields);
        breakdown.recordAggCollect(shard2AggCollect);

        // --- Simulate shard results from OLD data nodes (3 out of 5 shards) ---
        // Old nodes return null breakdown data. The coordinator simply doesn't call any
        // recordXxx() methods for these shards — this is the graceful skip behavior.
        // No additional action needed here; the null check happens at the transport layer.

        // Request end
        long requestEnd = fetchPhaseEnd + reduceTopDocsNanos + reduceAggsNanos + TimeUnit.MILLISECONDS.toNanos(2);

        // --- Produce the unified breakdown map ---
        Map<String, Object> breakdownMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // --- ASSERTIONS ---

        // 1. Map is not null and not empty
        assertNotNull("Breakdown map should not be null in mixed-version scenario", breakdownMap);
        assertFalse("Breakdown map should not be empty in mixed-version scenario", breakdownMap.isEmpty());

        // 2. No values in the map are null
        for (Map.Entry<String, Object> entry : breakdownMap.entrySet()) {
            assertNotNull("Value for key '" + entry.getKey() + "' should not be null", entry.getValue());
            if (entry.getValue() instanceof Long) {
                assertTrue("Value for key '" + entry.getKey() + "' must be positive", (Long) entry.getValue() > 0);
            }
        }

        // 3. Coordinator metrics are correct
        assertEquals("pipeline_request_transform should be 3ms", 3L, (long) breakdownMap.get("pipeline_request_transform"));
        assertEquals("query_rewrite should be 5ms", 5L, (long) breakdownMap.get("query_rewrite"));
        assertEquals("shard_routing should be 2ms", 2L, (long) breakdownMap.get("shard_routing"));
        assertEquals("index_resolution should be 1ms", 1L, (long) breakdownMap.get("index_resolution"));
        assertEquals("query phase should be 60ms", 60L, (long) breakdownMap.get("query"));
        assertEquals("fetch phase should be 18ms", 18L, (long) breakdownMap.get("fetch"));
        assertEquals("query_to_fetch_gap should be 4ms", 4L, (long) breakdownMap.get("query_to_fetch_gap"));
        assertEquals("reduce_top_docs should be 3ms", 3L, (long) breakdownMap.get("reduce_top_docs"));
        assertEquals("reduce_aggregations should be 2ms", 2L, (long) breakdownMap.get("reduce_aggregations"));

        // 4. Data-node metrics reflect ONLY shards that reported them (max-aggregated)
        // search_context_creation: max(8ms, 15ms) = 15ms
        assertEquals("search_context_creation should be max across reporting shards (15ms)",
            15L, (long) breakdownMap.get("search_context_creation"));
        // acquire_searcher: max(2ms, 1ms) = 2ms
        assertEquals("acquire_searcher should be max across reporting shards (2ms)",
            2L, (long) breakdownMap.get("acquire_searcher"));
        // fetch_stored_fields: max(4ms, 6ms) = 6ms
        assertEquals("fetch_stored_fields should be max across reporting shards (6ms)",
            6L, (long) breakdownMap.get("fetch_stored_fields"));
        // agg_collect: max(12ms, 9ms) = 12ms
        assertEquals("agg_collect should be max across reporting shards (12ms)",
            12L, (long) breakdownMap.get("agg_collect"));

        // 5. Data-node metrics NOT reported by any shard should be absent
        assertNull("script_compilation should be absent (no shard reported it)",
            breakdownMap.get("script_compilation"));
        assertNull("global_ordinals_loading should be absent (no shard reported it)",
            breakdownMap.get("global_ordinals_loading"));
        assertNull("nested_bitset_construction should be absent (no shard reported it)",
            breakdownMap.get("nested_bitset_construction"));

        // 6. Pre-phase overhead should be present and correct
        long expectedPrePhaseOverhead = TimeUnit.NANOSECONDS.toMillis(prePhaseTotal);
        assertEquals("pre_phase_overhead should equal time before first phase",
            expectedPrePhaseOverhead, (long) breakdownMap.get("pre_phase_overhead"));
    }

    /**
     * Test 2: All old data nodes — coordinator-only breakdown.
     * <p>
     * Simulates a scenario where ALL data nodes are on older versions that do not support
     * breakdown instrumentation. The coordinator should still produce a valid breakdown
     * containing only coordinator-measured metrics (phases, gaps, reduce, overhead).
     * No data-node metrics should appear in the output.
     */
    public void testAllOldDataNodes_coordinatorOnlyBreakdown() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 2_000_000_000L;

        // Record coordinator-level metrics
        long pipelineTransformNanos = TimeUnit.MILLISECONDS.toNanos(4);
        breakdown.recordPipelineRequestTransform(pipelineTransformNanos);

        long queryRewriteNanos = TimeUnit.MILLISECONDS.toNanos(7);
        breakdown.recordQueryRewrite(queryRewriteNanos);

        long indexResolutionNanos = TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordIndexResolution(indexResolutionNanos);

        long shardRoutingNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordShardRouting(shardRoutingNanos);

        // Phase start
        long prePhaseNanos = pipelineTransformNanos + queryRewriteNanos + indexResolutionNanos + shardRoutingNanos;
        long firstPhaseStart = absoluteStart + prePhaseNanos;
        breakdown.markFirstPhaseStart(firstPhaseStart);

        // Query phase
        long queryPhaseNanos = TimeUnit.MILLISECONDS.toNanos(80);
        breakdown.recordQueryPhase(queryPhaseNanos);
        long queryEnd = firstPhaseStart + queryPhaseNanos;
        breakdown.markPhaseEnd("query", queryEnd);

        // Inter-phase gap: query → fetch
        long queryToFetchGapNanos = TimeUnit.MILLISECONDS.toNanos(6);
        breakdown.recordInterPhaseGap("query", "fetch", queryToFetchGapNanos);

        // Fetch phase
        long fetchPhaseNanos = TimeUnit.MILLISECONDS.toNanos(25);
        breakdown.recordFetchPhase(fetchPhaseNanos);
        long fetchEnd = queryEnd + queryToFetchGapNanos + fetchPhaseNanos;
        breakdown.markPhaseEnd("fetch", fetchEnd);

        // Reduce (coordinator)
        long reduceTopDocsNanos = TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordReduceTopDocs(reduceTopDocsNanos);

        long reduceSuggestionsNanos = TimeUnit.MILLISECONDS.toNanos(1);
        breakdown.recordReduceSuggestions(reduceSuggestionsNanos);

        // Transport metrics (coordinator-observed)
        long networkRoundtripMax = TimeUnit.MILLISECONDS.toNanos(10);
        breakdown.recordNetworkRoundtripMax(networkRoundtripMax);

        long requestSerNanos = TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordRequestSerialization(requestSerNanos);

        long responseDeserNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordResponseDeserialization(responseDeserNanos);

        // NO data-node metrics recorded — ALL shards are on old nodes

        long requestEnd = fetchEnd + reduceTopDocsNanos + reduceSuggestionsNanos + TimeUnit.MILLISECONDS.toNanos(3);

        // Produce the unified breakdown map
        Map<String, Object> breakdownMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // --- ASSERTIONS ---

        // 1. Map is valid and non-empty
        assertNotNull("Breakdown map should not be null with all old data nodes", breakdownMap);
        assertFalse("Breakdown map should not be empty with all old data nodes", breakdownMap.isEmpty());

        // 2. Coordinator metrics are present and correct
        assertEquals("pipeline_request_transform", 4L, (long) breakdownMap.get("pipeline_request_transform"));
        assertEquals("query_rewrite", 7L, (long) breakdownMap.get("query_rewrite"));
        assertEquals("index_resolution", 2L, (long) breakdownMap.get("index_resolution"));
        assertEquals("shard_routing", 3L, (long) breakdownMap.get("shard_routing"));
        assertEquals("query", 80L, (long) breakdownMap.get("query"));
        assertEquals("fetch", 25L, (long) breakdownMap.get("fetch"));
        assertEquals("query_to_fetch_gap", 6L, (long) breakdownMap.get("query_to_fetch_gap"));
        assertEquals("reduce_top_docs", 5L, (long) breakdownMap.get("reduce_top_docs"));
        assertEquals("reduce_suggestions", 1L, (long) breakdownMap.get("reduce_suggestions"));
        assertEquals("network_roundtrip_max", 10L, (long) breakdownMap.get("network_roundtrip_max"));
        assertEquals("request_serialization", 2L, (long) breakdownMap.get("request_serialization"));
        assertEquals("response_deserialization", 3L, (long) breakdownMap.get("response_deserialization"));

        // 3. No data-node metric keys are present
        for (String dataNodeKey : DATA_NODE_METRIC_KEYS) {
            assertNull(
                "Data-node metric '" + dataNodeKey + "' should NOT be present when all data nodes are old",
                breakdownMap.get(dataNodeKey)
            );
        }

        // 4. Pre-phase overhead is correct
        long expectedPrePhaseOverhead = TimeUnit.NANOSECONDS.toMillis(prePhaseNanos);
        assertEquals("pre_phase_overhead should be 16ms", expectedPrePhaseOverhead, (long) breakdownMap.get("pre_phase_overhead"));

        // 5. Post-phase overhead is present
        assertTrue("post_phase_overhead should be present", breakdownMap.containsKey("post_phase_overhead"));
        assertTrue("post_phase_overhead should be positive", (Long) breakdownMap.get("post_phase_overhead") > 0);

        // 6. No null values in map
        for (Map.Entry<String, Object> entry : breakdownMap.entrySet()) {
            assertNotNull("Value for '" + entry.getKey() + "' must not be null", entry.getValue());
        }
    }

    /**
     * Test 3: Phase latency map is still populated correctly in mixed-version clusters.
     * <p>
     * Even when some/all data nodes don't return breakdown data, the coordinator-measured
     * phase durations (query, fetch, can_match) should be correctly captured in the breakdown.
     * This verifies backward compatibility with phase_latency_map semantics.
     */
    public void testPhaseLatencyMapStillPopulated() {
        SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

        long absoluteStart = 3_000_000_000L;

        // Pre-phase setup
        long prePhaseNanos = TimeUnit.MILLISECONDS.toNanos(8);
        breakdown.recordQueryRewrite(prePhaseNanos);
        long firstPhaseStart = absoluteStart + prePhaseNanos;
        breakdown.markFirstPhaseStart(firstPhaseStart);

        // Can_match phase
        long canMatchNanos = TimeUnit.MILLISECONDS.toNanos(5);
        breakdown.recordCanMatchPhase(canMatchNanos);
        long canMatchEnd = firstPhaseStart + canMatchNanos;
        breakdown.markPhaseEnd("can_match", canMatchEnd);

        // Inter-phase gap: can_match → query
        long canMatchToQueryGapNanos = TimeUnit.MILLISECONDS.toNanos(2);
        breakdown.recordInterPhaseGap("can_match", "query", canMatchToQueryGapNanos);

        // Query phase
        long queryNanos = TimeUnit.MILLISECONDS.toNanos(55);
        breakdown.recordQueryPhase(queryNanos);
        long queryEnd = canMatchEnd + canMatchToQueryGapNanos + queryNanos;
        breakdown.markPhaseEnd("query", queryEnd);

        // Inter-phase gap: query → fetch
        long queryToFetchGapNanos = TimeUnit.MILLISECONDS.toNanos(3);
        breakdown.recordInterPhaseGap("query", "fetch", queryToFetchGapNanos);

        // Fetch phase
        long fetchNanos = TimeUnit.MILLISECONDS.toNanos(20);
        breakdown.recordFetchPhase(fetchNanos);
        long fetchEnd = queryEnd + queryToFetchGapNanos + fetchNanos;
        breakdown.markPhaseEnd("fetch", fetchEnd);

        // Simulate that only 1 shard out of 4 returned breakdown data (mixed version)
        long shard1ContextCreation = TimeUnit.MILLISECONDS.toNanos(10);
        breakdown.recordSearchContextCreation(shard1ContextCreation);

        // No other shards report breakdown (they are old nodes)

        // Reduce
        long reduceNanos = TimeUnit.MILLISECONDS.toNanos(4);
        breakdown.recordReduceTopDocs(reduceNanos);

        long requestEnd = fetchEnd + reduceNanos + TimeUnit.MILLISECONDS.toNanos(2);

        // Get breakdown map
        Map<String, Object> breakdownMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

        // --- ASSERTIONS: Phase latency entries are correct ---

        // 1. Phase durations match what was recorded (these come from coordinator measurement)
        assertEquals("can_match phase should be 5ms", 5L, (long) breakdownMap.get("can_match"));
        assertEquals("query phase should be 55ms", 55L, (long) breakdownMap.get("query"));
        assertEquals("fetch phase should be 20ms", 20L, (long) breakdownMap.get("fetch"));

        // 2. Inter-phase gaps are correct
        assertEquals("can_match_to_query_gap should be 2ms", 2L, (long) breakdownMap.get("can_match_to_query_gap"));
        assertEquals("query_to_fetch_gap should be 3ms", 3L, (long) breakdownMap.get("query_to_fetch_gap"));

        // 3. Pre-phase overhead is correct (time from absoluteStart to firstPhaseStart)
        long expectedPrePhaseOverhead = TimeUnit.NANOSECONDS.toMillis(prePhaseNanos);
        assertEquals("pre_phase_overhead should be 8ms", expectedPrePhaseOverhead, (long) breakdownMap.get("pre_phase_overhead"));

        // 4. The one shard that DID report data-node metrics should appear
        assertEquals("search_context_creation from single reporting shard should be 10ms",
            10L, (long) breakdownMap.get("search_context_creation"));

        // 5. Data-node metrics NOT reported by any shard should be absent
        assertNull("acquire_searcher should be absent (no shard reported it)",
            breakdownMap.get("acquire_searcher"));
        assertNull("script_compilation should be absent (no shard reported it)",
            breakdownMap.get("script_compilation"));
        assertNull("fetch_stored_fields should be absent (no shard reported it)",
            breakdownMap.get("fetch_stored_fields"));

        // 6. Reduce metric is present
        assertEquals("reduce_top_docs should be 4ms", 4L, (long) breakdownMap.get("reduce_top_docs"));

        // 7. Post-phase overhead is present
        assertTrue("post_phase_overhead should be present", breakdownMap.containsKey("post_phase_overhead"));

        // 8. No nulls in the map
        for (Map.Entry<String, Object> entry : breakdownMap.entrySet()) {
            assertNotNull("Value for '" + entry.getKey() + "' must not be null", entry.getValue());
            if (entry.getValue() instanceof Long) {
                assertTrue("Value for '" + entry.getKey() + "' must be positive", (Long) entry.getValue() > 0);
            }
        }

        // 9. The sum of top-level entries should approximate total request duration
        long tookMillis = TimeUnit.NANOSECONDS.toMillis(requestEnd - absoluteStart);
        long sumTopLevel = 0;
        if (breakdownMap.containsKey("pre_phase_overhead")) sumTopLevel += (Long) breakdownMap.get("pre_phase_overhead");
        if (breakdownMap.containsKey("can_match")) sumTopLevel += (Long) breakdownMap.get("can_match");
        if (breakdownMap.containsKey("can_match_to_query_gap")) sumTopLevel += (Long) breakdownMap.get("can_match_to_query_gap");
        if (breakdownMap.containsKey("query")) sumTopLevel += (Long) breakdownMap.get("query");
        if (breakdownMap.containsKey("query_to_fetch_gap")) sumTopLevel += (Long) breakdownMap.get("query_to_fetch_gap");
        if (breakdownMap.containsKey("fetch")) sumTopLevel += (Long) breakdownMap.get("fetch");
        if (breakdownMap.containsKey("post_phase_overhead")) sumTopLevel += (Long) breakdownMap.get("post_phase_overhead");

        assertTrue(
            "Sum of top-level entries (" + sumTopLevel + "ms) should approximate took (" + tookMillis + "ms)",
            Math.abs(tookMillis - sumTopLevel) <= 5
        );
    }
}
