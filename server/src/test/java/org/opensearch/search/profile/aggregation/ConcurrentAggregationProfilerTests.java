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
                10847417L,
                List.of(
                    new ProfileResult(
                        "GlobalOrdinalsStringTermsAggregator",
                        "test_terms",
                        new LinkedHashMap<>(),
                        new HashMap<>(),
                        3359835L,
                        List.of(),
                        1490667L,
                        1180123L,
                        1240676L
                    )
                ),
                94582L,
                18667L,
                211749L
            )
        );
        tree.add(
            new ProfileResult(
                "NumericTermsAggregator",
                "test_scoped_agg",
                new LinkedHashMap<>(),
                new HashMap<>(),
                10776655L,
                List.of(
                    new ProfileResult(
                        "GlobalOrdinalsStringTermsAggregator",
                        "test_terms",
                        new LinkedHashMap<>(),
                        new HashMap<>(),
                        3359567L,
                        List.of(),
                        1390554L,
                        1180321L,
                        1298776L
                    )
                ),
                94560L,
                11237L,
                236440L
            )
        );
        // Global Aggregation
        tree.add(
            new ProfileResult(
                "GlobalAggregator",
                "test_global_agg",
                new LinkedHashMap<>(),
                new HashMap<>(),
                19631335L,
                List.of(),
                563002L,
                142210L,
                1216631L
            )
        );
        tree.add(
            new ProfileResult(
                "GlobalAggregator",
                "test_global_agg",
                new LinkedHashMap<>(),
                new HashMap<>(),
                19634567L,
                List.of(),
                563333L,
                146783L,
                1496600L
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
            new ProfileResult("NumericTermsAggregator", "desc", Map.of("initialize", 100L), Map.of(), 130L, List.of()),
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
            new ProfileResult("NumericTermsAggregator", "desc", Map.of("initialize", 50L), Map.of(), 120L, List.of()),
            "initialize"
        );
        assertEquals(100L, (long) statsMap.get("max_initialize"));
        assertEquals(50L, (long) statsMap.get("min_initialize"));
        assertEquals(150L, (long) statsMap.get("avg_initialize"));
    }

    public void testMergeDebugInfoSumsNumericValuesAcrossSlices() {
        List<ProfileResult> profileResultsAcrossSlices = List.of(
            new ProfileResult("NumericTermsAggregator", "n_terms", new LinkedHashMap<>(), Map.of("total_buckets", 3L), 100L, List.of()),
            new ProfileResult("NumericTermsAggregator", "n_terms", new LinkedHashMap<>(), Map.of("total_buckets", 2L), 100L, List.of()),
            new ProfileResult("NumericTermsAggregator", "n_terms", new LinkedHashMap<>(), Map.of("total_buckets", 4L), 100L, List.of())
        );
        Map<String, Object> mergedDebug = ConcurrentAggregationProfiler.mergeDebugInfo(profileResultsAcrossSlices);
        assertEquals(9L, mergedDebug.get("total_buckets"));
    }

    public void testMergeDebugInfoIsDeterministicRegardlessOfSliceOrder() {
        Map<String, Object> sliceADebug = Map.of("segments_with_single_valued_ords", 2, "segments_with_multi_valued_ords", 1);
        Map<String, Object> sliceBDebug = Map.of("segments_with_single_valued_ords", 1, "segments_with_multi_valued_ords", 3);

        List<ProfileResult> forwardOrder = List.of(
            new ProfileResult("GlobalOrdinalsStringTermsAggregator", "str_terms", new LinkedHashMap<>(), sliceADebug, 100L, List.of()),
            new ProfileResult("GlobalOrdinalsStringTermsAggregator", "str_terms", new LinkedHashMap<>(), sliceBDebug, 100L, List.of())
        );
        List<ProfileResult> reverseOrder = List.of(
            new ProfileResult("GlobalOrdinalsStringTermsAggregator", "str_terms", new LinkedHashMap<>(), sliceBDebug, 100L, List.of()),
            new ProfileResult("GlobalOrdinalsStringTermsAggregator", "str_terms", new LinkedHashMap<>(), sliceADebug, 100L, List.of())
        );

        Map<String, Object> mergedForward = ConcurrentAggregationProfiler.mergeDebugInfo(forwardOrder);
        Map<String, Object> mergedReverse = ConcurrentAggregationProfiler.mergeDebugInfo(reverseOrder);

        assertEquals(3, mergedForward.get("segments_with_single_valued_ords"));
        assertEquals(4, mergedForward.get("segments_with_multi_valued_ords"));
        assertEquals(mergedForward, mergedReverse);
    }

    public void testMergeDebugInfoPreservesIntegerTypeWhenSummingAcrossSlices() {
        // AggregationProfilerIT unboxes segments_with_single/multi_valued_ords with a narrowing
        // (int) cast, so summing two Integer-valued slices must stay Integer, not get promoted
        // to Long the way a mixed-type or single-Long-source sum would.
        List<ProfileResult> profileResultsAcrossSlices = List.of(
            new ProfileResult(
                "GlobalOrdinalsStringTermsAggregator",
                "str_terms",
                new LinkedHashMap<>(),
                Map.of("segments_with_single_valued_ords", 2),
                100L,
                List.of()
            ),
            new ProfileResult(
                "GlobalOrdinalsStringTermsAggregator",
                "str_terms",
                new LinkedHashMap<>(),
                Map.of("segments_with_single_valued_ords", 3),
                100L,
                List.of()
            )
        );
        Map<String, Object> mergedDebug = ConcurrentAggregationProfiler.mergeDebugInfo(profileResultsAcrossSlices);
        assertEquals(5, mergedDebug.get("segments_with_single_valued_ords"));
        assertTrue(mergedDebug.get("segments_with_single_valued_ords") instanceof Integer);
    }

    public void testMergeDebugInfoPreservesOriginalTypeForSingleSlice() {
        List<ProfileResult> profileResultsAcrossSlices = List.of(
            new ProfileResult(
                "GlobalOrdinalsStringTermsAggregator",
                "str_terms",
                new LinkedHashMap<>(),
                Map.of("segments_with_single_valued_ords", 2),
                100L,
                List.of()
            )
        );
        Map<String, Object> mergedDebug = ConcurrentAggregationProfiler.mergeDebugInfo(profileResultsAcrossSlices);
        assertEquals(2, mergedDebug.get("segments_with_single_valued_ords"));
        assertTrue(mergedDebug.get("segments_with_single_valued_ords") instanceof Integer);
    }

    public void testMergeDebugInfoKeepsStaticNonNumericValues() {
        List<ProfileResult> profileResultsAcrossSlices = List.of(
            new ProfileResult(
                "GlobalOrdinalsStringTermsAggregator",
                "str_terms",
                new LinkedHashMap<>(),
                Map.of("collection_strategy", "dense", "has_filter", false, "deferred_aggregators", List.of("max_number")),
                100L,
                List.of()
            ),
            new ProfileResult(
                "GlobalOrdinalsStringTermsAggregator",
                "str_terms",
                new LinkedHashMap<>(),
                Map.of("collection_strategy", "dense", "has_filter", false, "deferred_aggregators", List.of("max_number")),
                100L,
                List.of()
            )
        );
        Map<String, Object> mergedDebug = ConcurrentAggregationProfiler.mergeDebugInfo(profileResultsAcrossSlices);
        assertEquals("dense", mergedDebug.get("collection_strategy"));
        assertEquals(false, mergedDebug.get("has_filter"));
        assertEquals(List.of("max_number"), mergedDebug.get("deferred_aggregators"));
    }

    public void testMergeDebugInfoHandlesEmptyDebugMaps() {
        List<ProfileResult> profileResultsAcrossSlices = List.of(
            new ProfileResult("GlobalAggregator", "test_global_agg", new LinkedHashMap<>(), Map.of(), 100L, List.of()),
            new ProfileResult("GlobalAggregator", "test_global_agg", new LinkedHashMap<>(), Map.of(), 100L, List.of())
        );
        Map<String, Object> mergedDebug = ConcurrentAggregationProfiler.mergeDebugInfo(profileResultsAcrossSlices);
        assertTrue(mergedDebug.isEmpty());
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
