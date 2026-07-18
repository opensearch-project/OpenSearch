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

/**
 * Property-based test for SearchLatencyBreakdown.
 * <p>
 * Feature: search-latency-breakdown, Property 8: Graceful partial breakdown (missing data-node metrics)
 * <p>
 * For any breakdown instance where only coordinator metrics are populated (no data-node metrics merged),
 * toUnifiedBreakdownMap SHALL produce a valid map containing only coordinator metrics with no nulls,
 * no exceptions, and no data-node metric keys present.
 * <p>
 * <b>Validates: Requirements 14.4, 16.3</b>
 */
public class SearchLatencyBreakdownProperty8Tests extends OpenSearchTestCase {

    /**
     * Data-node metric keys that should NOT appear when no data-node metrics are merged.
     * These correspond to metrics populated via accumulateAndGet(n, Math::max) from shard responses.
     */
    private static final Set<String> DATA_NODE_METRIC_KEYS = new HashSet<>(Arrays.asList(
        // Query phase internals (data node, max across shards)
        "search_idle_reactivation",
        "search_context_creation",
        "read_lock_acquisition",
        "acquire_searcher",
        "global_ordinals_loading",
        "fielddata_loading",
        "script_compilation",
        "nested_bitset_construction",
        "star_tree_setup",
        "derived_field_script",
        // Cache operations (data node)
        "request_cache_lookup",
        "request_cache_write",
        "query_cache_lookup",
        "query_cache_write",
        // Segment execution (data node)
        "lucene_create_weight",
        "lucene_build_scorer",
        "lucene_next_doc_advance",
        "lucene_score",
        "page_cache_miss",
        "remote_store_fetch",
        "merge_io_contention",
        // Aggregation (data node)
        "agg_initialize",
        "agg_build_leaf_collector",
        "agg_collect",
        "agg_post_collection",
        "agg_deferred_replay",
        "agg_build_aggregation",
        "global_agg_separate_pass",
        // Concurrent segment search (data node)
        "slice_creation",
        "slice_scheduling",
        "slice_max_execution",
        "slice_min_execution",
        "slice_result_aggregation",
        // Fetch phase internals (data node)
        "fetch_stored_fields",
        "fetch_source_loading",
        "fetch_highlighting",
        "fetch_script_fields",
        "fetch_inner_hits",
        // Data node queue wait (reported by data nodes)
        "data_node_queue_wait_max",
        "data_node_queue_wait_avg"
    ));

    /**
     * Property 8: Graceful partial breakdown with missing data-node metrics.
     * <p>
     * Simulates a mixed-version cluster scenario where data nodes do not return breakdown data.
     * Only coordinator-level metrics are recorded (random non-zero values). All data-node metrics
     * remain at their default zero. Verifies:
     * <ul>
     *   <li>toUnifiedBreakdownMap returns a valid non-null map</li>
     *   <li>No values in the map are null</li>
     *   <li>Coordinator metrics appear correctly (non-zero values present)</li>
     *   <li>No data-node metric keys are present in the map</li>
     *   <li>No exceptions are thrown during the entire operation</li>
     * </ul>
     */
    public void testPartialBreakdownWithOnlyCoordinatorMetrics() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Coordinator timing context
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(50_000_000L, 500_000_000L);

            // Record ONLY coordinator-level metrics with random non-zero values
            // Pre-search (coordinator side)
            long pipelineTransform = randomLongBetween(1_000_000L, 20_000_000L);
            breakdown.recordPipelineRequestTransform(pipelineTransform);

            long queryRewrite = randomLongBetween(1_000_000L, 50_000_000L);
            breakdown.recordQueryRewrite(queryRewrite);

            long indexResolution = randomLongBetween(1_000_000L, 10_000_000L);
            breakdown.recordIndexResolution(indexResolution);

            long shardRouting = randomLongBetween(1_000_000L, 15_000_000L);
            breakdown.recordShardRouting(shardRouting);

            if (randomBoolean()) {
                long termsLookup = randomLongBetween(1_000_000L, 30_000_000L);
                breakdown.recordTermsLookupSubQuery(termsLookup);
            }

            // Queue wait (coordinator)
            long queueWait = randomLongBetween(1_000_000L, 100_000_000L);
            breakdown.recordCoordinatorQueueWait(queueWait);

            // Phase durations (measured on coordinator as phase lifecycle)
            long queryPhase = randomLongBetween(5_000_000L, 200_000_000L);
            breakdown.recordQueryPhase(queryPhase);

            if (randomBoolean()) {
                long canMatch = randomLongBetween(1_000_000L, 20_000_000L);
                breakdown.recordCanMatchPhase(canMatch);
            }

            if (randomBoolean()) {
                long fetchPhase = randomLongBetween(2_000_000L, 100_000_000L);
                breakdown.recordFetchPhase(fetchPhase);
            }

            // Inter-phase gaps (coordinator computed)
            if (randomBoolean()) {
                breakdown.recordInterPhaseGap("can_match", "query", randomLongBetween(1_000_000L, 10_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordInterPhaseGap("query", "fetch", randomLongBetween(1_000_000L, 15_000_000L));
            }

            // Reduce metrics (coordinator)
            if (randomBoolean()) {
                breakdown.recordReduceTopDocs(randomLongBetween(1_000_000L, 20_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordReduceAggregations(randomLongBetween(1_000_000L, 15_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordReducePipelineAggs(randomLongBetween(1_000_000L, 10_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordReduceSuggestions(randomLongBetween(1_000_000L, 8_000_000L));
            }

            // Transport metrics (coordinator measured)
            if (randomBoolean()) {
                breakdown.recordNetworkRoundtripMax(randomLongBetween(1_000_000L, 50_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordNetworkRoundtripAvg(randomLongBetween(1_000_000L, 30_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordRequestSerialization(randomLongBetween(1_000_000L, 10_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordResponseDeserialization(randomLongBetween(1_000_000L, 12_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordInboundNetworkTime(randomLongBetween(1_000_000L, 20_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordOutboundNetworkTime(randomLongBetween(1_000_000L, 18_000_000L));
            }

            // Phase tracking for overhead computation
            long phaseStart = absoluteStart + randomLongBetween(5_000_000L, 30_000_000L);
            breakdown.markFirstPhaseStart(phaseStart);
            long phaseEnd = phaseStart + queryPhase;
            breakdown.markPhaseEnd("query", phaseEnd);

            // DO NOT record any data-node metrics — simulating old data nodes that
            // don't return breakdown data. All data-node metrics stay at zero.

            // Generate the unified breakdown map — this should NOT throw
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // ASSERTION 1: Map is not null
            assertNotNull("toUnifiedBreakdownMap should never return null, iteration " + i, resultMap);

            // ASSERTION 2: No values are null
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                assertNotNull(
                    "Value for key '" + entry.getKey() + "' should not be null, iteration " + i,
                    entry.getValue()
                );
            }

            // ASSERTION 3: No data-node metric keys should be present (they are all zero)
            for (String dataNodeKey : DATA_NODE_METRIC_KEYS) {
                assertFalse(
                    "Data-node metric key '" + dataNodeKey + "' should NOT be present when no "
                        + "data-node metrics are merged (mixed-version cluster scenario), iteration " + i,
                    resultMap.containsKey(dataNodeKey)
                );
            }

            // ASSERTION 4: At least some coordinator metrics should appear
            // (we recorded values >= 1ms for several coordinator metrics)
            assertFalse(
                "Partial breakdown with coordinator metrics should produce a non-empty map, iteration " + i,
                resultMap.isEmpty()
            );

            // ASSERTION 5: All values in the map are positive (no zeros, no negatives)
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                assertTrue(
                    "Entry '" + entry.getKey() + "' has non-positive value " + entry.getValue()
                        + " in iteration " + i + ". All map values must be positive.",
                    (entry.getValue() instanceof Long) && (Long) entry.getValue() > 0
                );
            }

            // ASSERTION 6: Verify specific coordinator metrics we know we recorded appear
            // (pipeline_request_transform, query_rewrite, index_resolution, shard_routing, query)
            // These all had values >= 1_000_000 nanos (>= 1ms), so they must appear.
            assertTrue(
                "pipeline_request_transform should appear in partial breakdown, iteration " + i,
                resultMap.containsKey("pipeline_request_transform")
            );
            assertTrue(
                "query_rewrite should appear in partial breakdown, iteration " + i,
                resultMap.containsKey("query_rewrite")
            );
            assertTrue(
                "index_resolution should appear in partial breakdown, iteration " + i,
                resultMap.containsKey("index_resolution")
            );
            assertTrue(
                "shard_routing should appear in partial breakdown, iteration " + i,
                resultMap.containsKey("shard_routing")
            );
            assertTrue(
                "query should appear in partial breakdown, iteration " + i,
                resultMap.containsKey("query")
            );
            assertTrue(
                "coordinator_queue_wait should appear in partial breakdown, iteration " + i,
                resultMap.containsKey("coordinator_queue_wait")
            );
        }
    }

    /**
     * Property 8 (Sub-property): Completely empty breakdown (no metrics at all) produces valid empty map.
     * <p>
     * This verifies the extreme case where neither coordinator nor data-node metrics are recorded,
     * representing a scenario where a request ends before any instrumentation runs. The method
     * should still return a valid (empty) map without exceptions.
     */
    public void testCompletelyEmptyBreakdownProducesValidEmptyMap() {
        final int iterations = 100;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(1_000_000L, 500_000_000L);

            // No metrics recorded at all
            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // Should return valid non-null map
            assertNotNull("Empty breakdown should return non-null map, iteration " + i, resultMap);

            // No null values
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                assertNotNull(
                    "Value for key '" + entry.getKey() + "' should not be null in empty breakdown, iteration " + i,
                    entry.getValue()
                );
            }

            // No data-node keys
            for (String dataNodeKey : DATA_NODE_METRIC_KEYS) {
                assertFalse(
                    "Data-node key '" + dataNodeKey + "' should not appear in empty breakdown, iteration " + i,
                    resultMap.containsKey(dataNodeKey)
                );
            }
        }
    }

    /**
     * Property 8 (Sub-property): Partial breakdown with only pre-search coordinator metrics.
     * <p>
     * Simulates a request that was rejected or failed very early — only pre-search timing
     * was captured. Verifies the map is still valid and contains only those coordinator metrics.
     */
    public void testPartialBreakdownWithOnlyPreSearchMetrics() {
        final int iterations = 100;

        for (int i = 0; i < iterations; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();
            long absoluteStart = randomLongBetween(1_000_000_000L, 5_000_000_000L);
            long requestEnd = absoluteStart + randomLongBetween(5_000_000L, 50_000_000L);

            // Only record pre-search coordinator metrics
            breakdown.recordPipelineRequestTransform(randomLongBetween(1_000_000L, 10_000_000L));
            breakdown.recordQueryRewrite(randomLongBetween(1_000_000L, 20_000_000L));
            breakdown.recordIndexResolution(randomLongBetween(1_000_000L, 5_000_000L));
            breakdown.recordShardRouting(randomLongBetween(1_000_000L, 8_000_000L));

            if (randomBoolean()) {
                breakdown.recordTermsLookupSubQuery(randomLongBetween(1_000_000L, 15_000_000L));
            }
            if (randomBoolean()) {
                breakdown.recordClusterStateCheck(randomLongBetween(1_000_000L, 5_000_000L));
            }

            // No phases started, no data-node metrics

            Map<String, Object> resultMap = breakdown.toUnifiedBreakdownMap(absoluteStart, requestEnd);

            // Valid non-null map
            assertNotNull("Pre-search only breakdown should return non-null map, iteration " + i, resultMap);

            // No null values
            for (Map.Entry<String, Object> entry : resultMap.entrySet()) {
                assertNotNull(
                    "Value for key '" + entry.getKey() + "' should not be null, iteration " + i,
                    entry.getValue()
                );
                assertTrue(
                    "Value for key '" + entry.getKey() + "' must be positive, iteration " + i,
                    (entry.getValue() instanceof Long) && (Long) entry.getValue() > 0
                );
            }

            // No data-node keys
            for (String dataNodeKey : DATA_NODE_METRIC_KEYS) {
                assertFalse(
                    "Data-node key '" + dataNodeKey + "' should not appear in pre-search only breakdown, iteration " + i,
                    resultMap.containsKey(dataNodeKey)
                );
            }

            // Should contain at least some pre-search metrics
            assertFalse(
                "Pre-search only breakdown should produce a non-empty map, iteration " + i,
                resultMap.isEmpty()
            );
        }
    }
}
