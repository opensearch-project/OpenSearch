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
import org.opensearch.search.profile.Timer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Main class to profile aggregations with concurrent execution
 *
 * @opensearch.internal
 */
public class ConcurrentAggregationProfiler extends AggregationProfiler {

    private static final String MAX_PREFIX = "max_";
    private static final String MIN_PREFIX = "min_";
    private static final String AVG_PREFIX = "avg_";
    private static final String START_TIME_KEY = AggregationTimingType.INITIALIZE + Timer.TIMING_TYPE_START_TIME_SUFFIX;
    private static final String[] breakdownCountStatsTypes = { "build_leaf_collector_count", "collect_count" };

    @Override
    public List<ProfileResult> getTree() {
        List<ProfileResult> tree = profileTree.getTree();
        List<ProfileResult> reducedTree = new LinkedList<>();
        Map<String, List<ProfileResult>> sliceLevelAggregationMap = getSliceLevelAggregationMap(tree);
        for (List<ProfileResult> profileResultsAcrossSlices : sliceLevelAggregationMap.values()) {
            reducedTree.addAll(reduceProfileResultsTree(profileResultsAcrossSlices));
        }
        return reducedTree;
    }

    private List<ProfileResult> reduceProfileResultsTree(List<ProfileResult> profileResultsAcrossSlices) {
        String type = profileResultsAcrossSlices.get(0).getQueryName();
        String description = profileResultsAcrossSlices.get(0).getLuceneDescription();
        long maxSliceNodeEndTime = Long.MIN_VALUE;
        long minSliceNodeStartTime = Long.MAX_VALUE;
        long maxSliceNodeTime = Long.MIN_VALUE;
        long minSliceNodeTime = Long.MAX_VALUE;
        long avgSliceNodeTime = 0L;
        Map<String, Long> breakdown = new HashMap<>();
        Map<String, Long> timeStatsMap = new HashMap<>();
        Map<String, Long> minSliceStartTimeMap = new HashMap<>();
        Map<String, Long> maxSliceEndTimeMap = new HashMap<>();
        Map<String, Long> countStatsMap = new HashMap<>();
        Map<String, Object> debug;
        List<ProfileResult> children = new LinkedList<>();

        for (ProfileResult profileResult : profileResultsAcrossSlices) {
            long profileNodeTime = profileResult.getTime();
            long sliceStartTime = profileResult.getTimeBreakdown().get(START_TIME_KEY);

            // Profiled total time
            maxSliceNodeEndTime = Math.max(maxSliceNodeEndTime, sliceStartTime + profileNodeTime);
            minSliceNodeStartTime = Math.min(minSliceNodeStartTime, sliceStartTime);

            // Profiled total time stats
            maxSliceNodeTime = Math.max(maxSliceNodeTime, profileNodeTime);
            minSliceNodeTime = Math.min(minSliceNodeTime, profileNodeTime);
            avgSliceNodeTime += profileNodeTime;

            // Profiled breakdown time stats
            for (AggregationTimingType timingType : AggregationTimingType.values()) {
                buildBreakdownStatsMap(timeStatsMap, profileResult, timingType.toString());
            }

            // Profiled breakdown total time
            for (AggregationTimingType timingType : AggregationTimingType.values()) {
                String breakdownTimingType = timingType.toString();
                Long startTime = profileResult.getTimeBreakdown().get(breakdownTimingType + Timer.TIMING_TYPE_START_TIME_SUFFIX);
                Long endTime = startTime + profileResult.getTimeBreakdown().get(breakdownTimingType);
                minSliceStartTimeMap.put(
                    breakdownTimingType,
                    Math.min(minSliceStartTimeMap.getOrDefault(breakdownTimingType, Long.MAX_VALUE), startTime)
                );
                maxSliceEndTimeMap.put(
                    breakdownTimingType,
                    Math.max(maxSliceEndTimeMap.getOrDefault(breakdownTimingType, Long.MIN_VALUE), endTime)
                );
            }

            // Profiled breakdown count stats
            for (String breakdownCountType : breakdownCountStatsTypes) {
                buildBreakdownStatsMap(countStatsMap, profileResult, breakdownCountType);
            }

            // Profiled breakdown count
            for (AggregationTimingType timingType : AggregationTimingType.values()) {
                String breakdownType = timingType.toString();
                String breakdownTypeCount = breakdownType + Timer.TIMING_TYPE_COUNT_SUFFIX;
                breakdown.put(
                    breakdownTypeCount,
                    breakdown.getOrDefault(breakdownTypeCount, 0L) + profileResult.getTimeBreakdown().get(breakdownTypeCount)
                );
            }

            children.addAll(profileResult.getProfiledChildren());
        }
        debug = mergeDebugInfo(profileResultsAcrossSlices);
        // nodeTime
        long nodeTime = maxSliceNodeEndTime - minSliceNodeStartTime;
        avgSliceNodeTime /= profileResultsAcrossSlices.size();

        // Profiled breakdown time stats
        for (AggregationTimingType breakdownTimingType : AggregationTimingType.values()) {
            buildBreakdownMap(profileResultsAcrossSlices.size(), breakdown, timeStatsMap, breakdownTimingType.toString());
        }

        // Profiled breakdown total time
        for (AggregationTimingType breakdownTimingType : AggregationTimingType.values()) {
            String breakdownType = breakdownTimingType.toString();
            breakdown.put(breakdownType, maxSliceEndTimeMap.get(breakdownType) - minSliceStartTimeMap.get(breakdownType));
        }

        // Profiled breakdown count stats
        for (String breakdownCountType : breakdownCountStatsTypes) {
            buildBreakdownMap(profileResultsAcrossSlices.size(), breakdown, countStatsMap, breakdownCountType);
        }

        // children
        List<ProfileResult> reducedChildrenTree = new LinkedList<>();
        if (!children.isEmpty()) {
            Map<String, List<ProfileResult>> sliceLevelAggregationMap = getSliceLevelAggregationMap(children);
            for (List<ProfileResult> profileResults : sliceLevelAggregationMap.values()) {
                reducedChildrenTree.addAll(reduceProfileResultsTree(profileResults));
            }
        }

        ProfileResult reducedResult = new ProfileResult(
            type,
            description,
            breakdown,
            debug,
            nodeTime,
            reducedChildrenTree,
            maxSliceNodeTime,
            minSliceNodeTime,
            avgSliceNodeTime
        );
        return List.of(reducedResult);
    }

    /**
     * Merges the per-slice debug maps for an aggregator into a single, deterministic map.
     * <p>
     * Debug info is an untyped {@code Map<String, Object>}, so there is no generic way to combine
     * two slices' values for an arbitrary key. In practice, aggregators only ever populate two
     * kinds of debug values:
     * <ul>
     *     <li>Numeric counters (e.g. {@code total_buckets}, {@code segments_with_single_valued_ords})
     *     that measure per-slice work and should be summed across slices.</li>
     *     <li>Static configuration flags (e.g. {@code collection_strategy}, {@code has_filter}) that
     *     reflect the aggregator's configured behavior and are identical across every slice.</li>
     * </ul>
     * Numeric values are summed, preserving the reported type ({@code Integer}
     * stays {@code Integer}, since some callers unbox debug values with a narrowing
     * {@code (int)} cast, e.g. {@code segments_with_single_valued_ords}); any other
     * value is taken from whichever slice is encountered first, since it is expected
     * to be the same on every slice.
     */
    static Map<String, Object> mergeDebugInfo(List<ProfileResult> profileResultsAcrossSlices) {
        Map<String, Object> mergedDebug = new HashMap<>();
        for (ProfileResult profileResult : profileResultsAcrossSlices) {
            for (Map.Entry<String, Object> entry : profileResult.getDebugInfo().entrySet()) {
                mergedDebug.merge(entry.getKey(), entry.getValue(), ConcurrentAggregationProfiler::mergeDebugValue);
            }
        }
        return mergedDebug;
    }

    private static Object mergeDebugValue(Object fromOtherSlices, Object fromThisSlice) {
        if (fromOtherSlices instanceof Integer && fromThisSlice instanceof Integer) {
            return (Integer) fromOtherSlices + (Integer) fromThisSlice;
        }
        if (fromOtherSlices instanceof Number && fromThisSlice instanceof Number) {
            return ((Number) fromOtherSlices).longValue() + ((Number) fromThisSlice).longValue();
        }
        assert Objects.equals(fromOtherSlices, fromThisSlice) : "Non-numeric debug values differ across slices: "
            + fromOtherSlices
            + " vs "
            + fromThisSlice;
        return fromOtherSlices;
    }

    static void buildBreakdownMap(int treeSize, Map<String, Long> breakdown, Map<String, Long> statsMap, String breakdownType) {
        String maxBreakdownType = MAX_PREFIX + breakdownType;
        String minBreakdownType = MIN_PREFIX + breakdownType;
        String avgBreakdownType = AVG_PREFIX + breakdownType;
        breakdown.put(maxBreakdownType, statsMap.get(maxBreakdownType));
        breakdown.put(minBreakdownType, statsMap.get(minBreakdownType));
        breakdown.put(avgBreakdownType, statsMap.get(avgBreakdownType) / treeSize);
    }

    static void buildBreakdownStatsMap(Map<String, Long> statsMap, ProfileResult result, String breakdownType) {
        String maxBreakdownType = MAX_PREFIX + breakdownType;
        String minBreakdownType = MIN_PREFIX + breakdownType;
        String avgBreakdownType = AVG_PREFIX + breakdownType;
        statsMap.put(
            maxBreakdownType,
            Math.max(statsMap.getOrDefault(maxBreakdownType, Long.MIN_VALUE), result.getTimeBreakdown().get(breakdownType))
        );
        statsMap.put(
            minBreakdownType,
            Math.min(statsMap.getOrDefault(minBreakdownType, Long.MAX_VALUE), result.getTimeBreakdown().get(breakdownType))
        );
        statsMap.put(avgBreakdownType, statsMap.getOrDefault(avgBreakdownType, 0L) + result.getTimeBreakdown().get(breakdownType));
    }

    /**
     * @return a slice level aggregation map where the key is the description of the aggregation and
     * the value is a list of ProfileResult across all slices.
     */
    static Map<String, List<ProfileResult>> getSliceLevelAggregationMap(List<ProfileResult> tree) {
        Map<String, List<ProfileResult>> sliceLevelAggregationMap = new HashMap<>();
        for (ProfileResult result : tree) {
            String description = result.getLuceneDescription();
            final List<ProfileResult> sliceLevelAggregationList = sliceLevelAggregationMap.computeIfAbsent(
                description,
                k -> new LinkedList<>()
            );
            sliceLevelAggregationList.add(result);
        }
        return sliceLevelAggregationMap;
    }
}
