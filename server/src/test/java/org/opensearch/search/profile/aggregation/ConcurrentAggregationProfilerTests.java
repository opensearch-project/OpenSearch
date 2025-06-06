/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.profile.aggregation;

import org.opensearch.search.profile.ProfileResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConcurrentAggregationProfilerTests extends OpenSearchTestCase {

    public static List<ProfileResult> createConcurrentSearchProfileTree() {
        List<ProfileResult> tree = new ArrayList<>();
        // Aggregation
        tree.add(
            new ProfileResult(
                "NumericTermsAggregator",
                "test_scoped_agg",
                new LinkedHashMap<>(),
                new HashMap<>(),
                List.of(
                    new ProfileResult(
                        "GlobalOrdinalsStringTermsAggregator",
                        "test_terms",
                        new LinkedHashMap<>(),
                        new HashMap<>(),
                        List.of(),
                        new HashMap<>()
                    )
                ),
                new HashMap<>()
            )
        );
        tree.add(
            new ProfileResult(
                "NumericTermsAggregator",
                "test_scoped_agg",
                new LinkedHashMap<>(),
                new HashMap<>(),
                List.of(
                    new ProfileResult(
                        "GlobalOrdinalsStringTermsAggregator",
                        "test_terms",
                        new LinkedHashMap<>(),
                        new HashMap<>(),
                        List.of(),
                        new HashMap<>()
                    )
                ),
                new HashMap<>()
            )
        );
        // Global Aggregation
        tree.add(
            new ProfileResult(
                "GlobalAggregator",
                "test_global_agg",
                new LinkedHashMap<>(),
                new HashMap<>(),
                List.of(),
                new HashMap<>()
            )
        );
        tree.add(
            new ProfileResult(
                "GlobalAggregator",
                "test_global_agg",
                new LinkedHashMap<>(),
                new HashMap<>(),
                List.of(),
                new HashMap<>()
            )
        );
        return tree;
    }

    public void testBuildTimeStatsBreakdownMap() {
        List<ProfileResult> tree = createConcurrentSearchProfileTree();
        Map<String, Long> breakdown = new HashMap<>();
        Map<String, Long> timeStatsMap = new HashMap<>();
        timeStatsMap.put("max_initialize", 30L);
        timeStatsMap.put("min_initialize", 10L);
        timeStatsMap.put("avg_initialize", 60L);
        ConcurrentAggregationProfiler.buildBreakdownMap(tree.size(), breakdown, timeStatsMap, "initialize");
        assertTrue(breakdown.containsKey("max_initialize"));
        assertTrue(breakdown.containsKey("min_initialize"));
        assertTrue(breakdown.containsKey("avg_initialize"));
        assertEquals(30L, (long) breakdown.get("max_initialize"));
        assertEquals(10L, (long) breakdown.get("min_initialize"));
        assertEquals(15L, (long) breakdown.get("avg_initialize"));
    }

    public void testBuildCountStatsBreakdownMap() {
        List<ProfileResult> tree = createConcurrentSearchProfileTree();
        Map<String, Long> breakdown = new HashMap<>();
        Map<String, Long> countStatsMap = new HashMap<>();
        countStatsMap.put("max_collect_count", 3L);
        countStatsMap.put("min_collect_count", 1L);
        countStatsMap.put("avg_collect_count", 6L);
        ConcurrentAggregationProfiler.buildBreakdownMap(tree.size(), breakdown, countStatsMap, "collect_count");
        assertTrue(breakdown.containsKey("max_collect_count"));
        assertTrue(breakdown.containsKey("min_collect_count"));
        assertTrue(breakdown.containsKey("avg_collect_count"));
        assertEquals(3L, (long) breakdown.get("max_collect_count"));
        assertEquals(1L, (long) breakdown.get("min_collect_count"));
        assertEquals(1L, (long) breakdown.get("avg_collect_count"));
    }

    public void testBuildBreakdownStatsMap() {
        Map<String, Long> statsMap = new HashMap<>();
        ConcurrentAggregationProfiler.buildBreakdownStatsMap(
            statsMap,
            new ProfileResult("NumericTermsAggregator", "desc", Map.of("initialize", 100L), Map.of(), List.of(), Map.of()),
            "initialize"
        );
        assertTrue(statsMap.containsKey("max_initialize"));
        assertTrue(statsMap.containsKey("min_initialize"));
        assertTrue(statsMap.containsKey("avg_initialize"));
        assertEquals(100L, (long) statsMap.get("max_initialize"));
        assertEquals(100L, (long) statsMap.get("min_initialize"));
        assertEquals(100L, (long) statsMap.get("avg_initialize"));
        ConcurrentAggregationProfiler.buildBreakdownStatsMap(
            statsMap,
            new ProfileResult("NumericTermsAggregator", "desc", Map.of("initialize", 50L), Map.of(), List.of(), Map.of()),
            "initialize"
        );
        assertEquals(100L, (long) statsMap.get("max_initialize"));
        assertEquals(50L, (long) statsMap.get("min_initialize"));
        assertEquals(150L, (long) statsMap.get("avg_initialize"));
    }

    public void testGetSliceLevelAggregationMap() {
        List<ProfileResult> tree = createConcurrentSearchProfileTree();
        Map<String, List<ProfileResult>> aggregationMap = ConcurrentAggregationProfiler.getSliceLevelAggregationMap(tree);
        assertEquals(2, aggregationMap.size());
        assertTrue(aggregationMap.containsKey("test_scoped_agg"));
        assertTrue(aggregationMap.containsKey("test_global_agg"));
        assertEquals(2, aggregationMap.get("test_scoped_agg").size());
        assertEquals(2, aggregationMap.get("test_global_agg").size());
        for (int slice_id : new int[] { 0, 1 }) {
            assertEquals(1, aggregationMap.get("test_scoped_agg").get(slice_id).getProfiledChildren().size());
            assertEquals(
                "test_terms",
                aggregationMap.get("test_scoped_agg").get(slice_id).getProfiledChildren().get(0).getLuceneDescription()
            );
            assertEquals(0, aggregationMap.get("test_global_agg").get(slice_id).getProfiledChildren().size());
        }
    }
}
