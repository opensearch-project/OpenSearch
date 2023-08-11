/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.ContextualProfileBreakdown;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc). The class supports profiling the concurrent search over segments.
 *
 * @opensearch.internal
 */
public final class ConcurrentQueryProfileBreakdown extends ContextualProfileBreakdown<QueryTimingType> {
    public static final String MAX_END_TIME_SUFFIX = "_max_end_time";
    public static final String MIN_START_TIME_SUFFIX = "_min_start_time";
    private static final String AT = "@";
    private static final String LATEST_LEAF_END_TIME = "latest_leaf_end_time";
    private static final String MAX_PREFIX = "max_";
    private static final String MIN_PREFIX = "min_";
    private static final String AVG_PREFIX = "avg_";
    private static long breakdownMaxEndTime = 0;
    private static long breakdownMinStartTime = Long.MAX_VALUE;
    private static long maxSliceNodeTime = Long.MIN_VALUE;
    private static long minSliceNodeTime = Long.MAX_VALUE;
    private static long avgSliceNodeTime = 0L;
    private static long nodeTimeSum = 0L;
    private final Map<Object, AbstractProfileBreakdown<QueryTimingType>> contexts = new ConcurrentHashMap<>();
    private List<String> collectorList = new ArrayList<>();
    private Map<LeafReaderContext, String> leafToCollector = new HashMap<>();

    /** Sole constructor. */
    public ConcurrentQueryProfileBreakdown() {
        super(QueryTimingType.class);
    }

    @Override
    public AbstractProfileBreakdown<QueryTimingType> context(Object context) {
        // See please https://bugs.openjdk.java.net/browse/JDK-8161372
        final AbstractProfileBreakdown<QueryTimingType> profile = contexts.get(context);

        if (profile != null) {
            return profile;
        }

        return contexts.computeIfAbsent(context, ctx -> new QueryProfileBreakdown());
    }

    @Override
    public Map<String, Long> toBreakdownMap() {
        final Map<String, Long> map = new HashMap<>(super.toBreakdownMap());

        for (final Map.Entry<Object, AbstractProfileBreakdown<QueryTimingType>> context : contexts.entrySet()) {
            Map<String, Long> breakdown = context.getValue().toBreakdownMap();
            String collector = leafToCollector.get(context.getKey());
            for (final Map.Entry<String, Long> entry : breakdown.entrySet()) {
                String breakdownType = entry.getKey();
                Long breakdownValue = entry.getValue();
                // Adding breakdown type count
                if (breakdownType.contains(TIMING_TYPE_COUNT_SUFFIX)) {
                    map.merge(breakdownType, breakdownValue, Long::sum);
                }
                // Adding slice level breakdown
                if (!breakdownType.contains(TIMING_TYPE_START_TIME_SUFFIX)) {
                    String sliceLevelBreakdownType = breakdownType + AT + collector;
                    map.put(sliceLevelBreakdownType, map.getOrDefault(sliceLevelBreakdownType, 0L) + breakdownValue);
                }
            }
            addMaxEndTimeAndMinStartTime(map, breakdown);
            // Adding slice level latest leaf end time
            long currentLeafEndTime = 0L;
            for (QueryTimingType queryTimingType : QueryTimingType.values()) {
                String timingType = queryTimingType.toString();
                String timingTypeStartTime = timingType + AbstractProfileBreakdown.TIMING_TYPE_START_TIME_SUFFIX;
                currentLeafEndTime = Math.max(currentLeafEndTime, breakdown.get(timingTypeStartTime) + breakdown.get(timingType));
            }
            String latestLeafEndTime = LATEST_LEAF_END_TIME + AT + collector;
            map.put(latestLeafEndTime, Math.max(map.getOrDefault(latestLeafEndTime, 0L), currentLeafEndTime));
        }
        return buildQueryProfileBreakdownMap(map);
    }

    /**
     * This method is used to bring the max end time and min start time fields to the query profile breakdown.
     *
     * @param  map  the query level timing count breakdown for current instance
     * @param  breakdown the segment level timing count breakdown for current instance
     */
    void addMaxEndTimeAndMinStartTime(Map<String, Long> map, Map<String, Long> breakdown) {
        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
            String timingType = queryTimingType.toString();
            String timingTypeStartTime = timingType + AbstractProfileBreakdown.TIMING_TYPE_START_TIME_SUFFIX;
            long currentMaxEndTime = map.getOrDefault(timingType + MAX_END_TIME_SUFFIX, 0L);
            long currentMinStartTime = map.getOrDefault(timingType + MIN_START_TIME_SUFFIX, Long.MAX_VALUE);
            // Only "create_weight" is performed at the query level
            Map<String, Long> source = timingType.equals(QueryTimingType.CREATE_WEIGHT.toString()) ? map : breakdown;
            long maxEndTime = Math.max(currentMaxEndTime, source.get(timingTypeStartTime) + source.get(timingType));
            long minStartTime = Math.min(currentMinStartTime, source.get(timingTypeStartTime));
            map.put(timingType + MAX_END_TIME_SUFFIX, maxEndTime);
            map.put(timingType + MIN_START_TIME_SUFFIX, minStartTime);
        }
    }

    /**
     * This method is used to construct the final query profile breakdown map for return.
     *
     * @param  map  a timing count breakdown map with the max end time and min start time information
     */
    Map<String, Long> buildQueryProfileBreakdownMap(Map<String, Long> map) {
        final Map<String, Long> breakdownMap = new HashMap<>();
        int collectorListSize = collectorList.size();
        // Calculating the total time stats for the query
        if (collectorList != null && !collectorList.isEmpty()) {
            for (String collector : collectorList) {
                Long latestLeafEndTime = map.get(LATEST_LEAF_END_TIME + AT + collector);
                if (latestLeafEndTime != null) {
                    Long currentSliceNodeTime = latestLeafEndTime - map.get(
                        QueryTimingType.CREATE_WEIGHT + AbstractProfileBreakdown.TIMING_TYPE_START_TIME_SUFFIX
                    );
                    maxSliceNodeTime = Math.max(maxSliceNodeTime, currentSliceNodeTime);
                    minSliceNodeTime = Math.min(minSliceNodeTime, currentSliceNodeTime);
                    nodeTimeSum += currentSliceNodeTime;
                }
            }
            avgSliceNodeTime = nodeTimeSum / collectorListSize;
        }
        for (QueryTimingType timingType : QueryTimingType.values()) {
            // Computing the total time for each breakdown including any wait time
            String type = timingType.toString();
            String timingTypeStartTime = type + AbstractProfileBreakdown.TIMING_TYPE_START_TIME_SUFFIX;
            Long timingTypeMaxEndTime = map.getOrDefault(type + MAX_END_TIME_SUFFIX, map.get(timingTypeStartTime) + map.get(type));
            Long timingTypeMinStartTime = map.getOrDefault(type + MIN_START_TIME_SUFFIX, map.get(timingTypeStartTime));
            breakdownMap.put(type, timingTypeMaxEndTime - timingTypeMinStartTime);
            breakdownMap.put(type + TIMING_TYPE_COUNT_SUFFIX, map.get(type + TIMING_TYPE_COUNT_SUFFIX));
            // Computing the max end time and min start time for the query
            breakdownMaxEndTime = Math.max(breakdownMaxEndTime, timingTypeMaxEndTime);
            if (timingTypeMaxEndTime != 0L && timingTypeMinStartTime != 0L) {
                breakdownMinStartTime = Math.min(breakdownMinStartTime, timingTypeMinStartTime);
            }
            // Computing the time/count stats for each breakdown across all slices
            if (collectorList != null && !collectorList.isEmpty() && !type.contains(QueryTimingType.CREATE_WEIGHT.toString())) {
                String maxSliceTypeTime = MAX_PREFIX + type;
                String minSliceTypeTime = MIN_PREFIX + type;
                String avgSliceTypeTime = AVG_PREFIX + type;
                String maxSliceTypeCount = maxSliceTypeTime + TIMING_TYPE_COUNT_SUFFIX;
                String minSliceTypeCount = minSliceTypeTime + TIMING_TYPE_COUNT_SUFFIX;
                String avgSliceTypeCount = avgSliceTypeTime + TIMING_TYPE_COUNT_SUFFIX;
                Long totalBreakdownTime = 0L;
                Long totalBreakdownCount = 0L;
                for (String collector : collectorList) {
                    Long breakdownTime = map.get(type + AT + collector);
                    Long breakdownCount = map.get(type + TIMING_TYPE_COUNT_SUFFIX + AT + collector);
                    totalBreakdownTime = constructStatResults(
                        breakdownMap,
                        maxSliceTypeTime,
                        minSliceTypeTime,
                        totalBreakdownTime,
                        breakdownTime
                    );
                    totalBreakdownCount = constructStatResults(
                        breakdownMap,
                        maxSliceTypeCount,
                        minSliceTypeCount,
                        totalBreakdownCount,
                        breakdownCount
                    );
                }
                breakdownMap.put(avgSliceTypeTime, totalBreakdownTime / collectorListSize);
                breakdownMap.put(avgSliceTypeCount, totalBreakdownCount / collectorListSize);
            }
        }
        return breakdownMap;
    }

    private Long constructStatResults(
        Map<String, Long> breakdownMap,
        String maxSliceTypeName,
        String minSliceTypeName,
        Long totalBreakdown,
        Long breakdown
    ) {
        if (breakdown == null) {
            breakdownMap.put(maxSliceTypeName, null);
            breakdownMap.put(minSliceTypeName, null);
        } else {
            breakdownMap.put(maxSliceTypeName, Math.max(breakdownMap.getOrDefault(maxSliceTypeName, Long.MIN_VALUE), breakdown));
            breakdownMap.put(minSliceTypeName, Math.min(breakdownMap.getOrDefault(minSliceTypeName, Long.MAX_VALUE), breakdown));
            totalBreakdown += breakdown;
        }
        return totalBreakdown;
    }

    @Override
    public long toNodeTime() {
        return breakdownMaxEndTime - breakdownMinStartTime;
    }

    @Override
    public Long getMaxSliceNodeTime() {
        return maxSliceNodeTime;
    }

    @Override
    public Long getMinSliceNodeTime() {
        return minSliceNodeTime;
    }

    @Override
    public Long getAvgSliceNodeTime() {
        return avgSliceNodeTime;
    }

    @Override
    public void associateLeafToCollector(LeafReaderContext leaf, Collector collector) {
        if (collector != null) {
            leafToCollector.put(leaf, collector.toString());
        }
    }

    @Override
    public void buildCollectorList(Collector collector) {
        if (collector != null) {
            collectorList.add(collector.toString());
        }
    }
}
