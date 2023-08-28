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
    private static final String MAX_PREFIX = "max_";
    private static final String MIN_PREFIX = "min_";
    private static final String AVG_PREFIX = "avg_";
    private long breakdownMaxEndTime = Long.MIN_VALUE;
    private long breakdownMinStartTime = Long.MAX_VALUE;
    private long maxSliceNodeTime = Long.MIN_VALUE;
    private long minSliceNodeTime = Long.MAX_VALUE;
    private long avgSliceNodeTime = 0L;
    private long nodeTimeSum = 0L;
    private final Map<Object, AbstractProfileBreakdown<QueryTimingType>> contexts = new ConcurrentHashMap<>();
    private Map<String, List<LeafReaderContext>> collectorToLeaves = new HashMap<>();

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
        Map<String, Map<String, Long>> sliceLevelBreakdown = new HashMap<>();
        if (collectorToLeaves == null || collectorToLeaves.isEmpty() || contexts.isEmpty()) {
            return new HashMap<>(super.toBreakdownMap());
        }
        for (Map.Entry<String, List<LeafReaderContext>> slice : collectorToLeaves.entrySet()) {
            String sliceCollector = slice.getKey();
            List<LeafReaderContext> leaves = slice.getValue();
            long lastLeafEndTime = 0L;
            for (LeafReaderContext leaf : leaves) {
                long currentLeafEndTime = 0L;
                Map<String, Long> currentLeafBreakdownMap = contexts.get(leaf).toBreakdownMap();
                for (Map.Entry<String, Long> breakDownEntry : currentLeafBreakdownMap.entrySet()) {
                    Map<String, Long> currentSliceBreakdown = sliceLevelBreakdown.getOrDefault(
                        sliceCollector,
                        new HashMap<>(super.toBreakdownMap())
                    );
                    String breakdownType = breakDownEntry.getKey();
                    Long breakdownValue = breakDownEntry.getValue();
                    // Adding breakdown type count
                    if (breakdownType.contains(TIMING_TYPE_COUNT_SUFFIX)) {
                        currentSliceBreakdown.merge(breakdownType, breakdownValue, Long::sum);
                    }
                    if (breakdownType.contains(TIMING_TYPE_START_TIME_SUFFIX)) {
                        // Finding the earliest start time and the last end time for each breakdown types within the current slice to
                        // compute the total breakdown time
                        String method = breakdownType.split(TIMING_TYPE_START_TIME_SUFFIX)[0];
                        String maxEndTimeKey = method + MAX_END_TIME_SUFFIX;
                        String minStartTimeKey = method + MIN_START_TIME_SUFFIX;
                        long maxEndTime = Math.max(
                            currentSliceBreakdown.getOrDefault(maxEndTimeKey, Long.MIN_VALUE),
                            breakdownValue + currentLeafBreakdownMap.get(method)
                        );
                        long minStartTime = Math.min(currentSliceBreakdown.getOrDefault(minStartTimeKey, Long.MAX_VALUE), breakdownValue);
                        currentSliceBreakdown.put(maxEndTimeKey, maxEndTime);
                        currentSliceBreakdown.put(minStartTimeKey, minStartTime);
                        // Finding the current leaf end time to compute the last leaf end time
                        currentLeafEndTime = Math.max(currentLeafEndTime, maxEndTime);
                    }
                    sliceLevelBreakdown.put(sliceCollector, currentSliceBreakdown);
                }
                lastLeafEndTime = Math.max(lastLeafEndTime, currentLeafEndTime);
            }
            // Computing breakdown type total time
            for (QueryTimingType queryTimingType : QueryTimingType.values()) {
                String timingType = queryTimingType.toString();
                if (queryTimingType != QueryTimingType.CREATE_WEIGHT) {
                    long currentSliceMaxEndTime = sliceLevelBreakdown.get(sliceCollector).get(timingType + MAX_END_TIME_SUFFIX);
                    long currentSliceMinStartTime = sliceLevelBreakdown.get(sliceCollector).get(timingType + MIN_START_TIME_SUFFIX);
                    sliceLevelBreakdown.get(sliceCollector).put(timingType, currentSliceMaxEndTime - currentSliceMinStartTime);
                }
            }
            // Computing slice level node time and stats
            long createWeightStartTime = sliceLevelBreakdown.get(sliceCollector)
                .get(QueryTimingType.CREATE_WEIGHT + AbstractProfileBreakdown.TIMING_TYPE_START_TIME_SUFFIX);
            breakdownMaxEndTime = Math.max(breakdownMaxEndTime, lastLeafEndTime);
            breakdownMinStartTime = createWeightStartTime;
            long currentSliceNodeTime = lastLeafEndTime - createWeightStartTime;
            maxSliceNodeTime = Math.max(maxSliceNodeTime, currentSliceNodeTime);
            minSliceNodeTime = Math.min(minSliceNodeTime, currentSliceNodeTime);
            nodeTimeSum += currentSliceNodeTime;
        }
        // Computing avg slice node time
        avgSliceNodeTime = nodeTimeSum / collectorToLeaves.size();
        return buildFinalBreakdownMap(sliceLevelBreakdown);
    }

    /**
     * This method is used to construct the final query profile breakdown map for return.
     *
     * @param  sliceLevelBreakdown a timing count breakdown map with the max end time and min start time information
     */
    Map<String, Long> buildFinalBreakdownMap(Map<String, Map<String, Long>> sliceLevelBreakdown) {
        final Map<String, Long> breakdownMap = new HashMap<>();
        final Map<String, Long> totalTimeMap = new HashMap<>();
        int sliceCount = sliceLevelBreakdown.size();
        for (Map.Entry<String, Map<String, Long>> slice : sliceLevelBreakdown.entrySet()) {
            for (QueryTimingType queryTimingType : QueryTimingType.values()) {
                String timingType = queryTimingType.toString();
                // Computing the time/count stats for each breakdown across all slices
                if (queryTimingType != QueryTimingType.CREATE_WEIGHT) {
                    String maxBreakdownTypeTime = MAX_PREFIX + timingType;
                    String minBreakdownTypeTime = MIN_PREFIX + timingType;
                    String avgBreakdownTypeTime = AVG_PREFIX + timingType;
                    String maxBreakdownTypeCount = maxBreakdownTypeTime + TIMING_TYPE_COUNT_SUFFIX;
                    String minBreakdownTypeCount = minBreakdownTypeTime + TIMING_TYPE_COUNT_SUFFIX;
                    String avgBreakdownTypeCount = avgBreakdownTypeTime + TIMING_TYPE_COUNT_SUFFIX;
                    Long breakdownTime = slice.getValue().get(timingType);
                    Long breakdownCount = slice.getValue().get(timingType + TIMING_TYPE_COUNT_SUFFIX);
                    breakdownMap.put(
                        maxBreakdownTypeTime,
                        Math.max(breakdownMap.getOrDefault(maxBreakdownTypeTime, Long.MIN_VALUE), breakdownTime)
                    );
                    breakdownMap.put(
                        minBreakdownTypeTime,
                        Math.min(breakdownMap.getOrDefault(minBreakdownTypeTime, Long.MAX_VALUE), breakdownTime)
                    );
                    breakdownMap.put(avgBreakdownTypeTime, breakdownMap.getOrDefault(avgBreakdownTypeTime, 0L) + breakdownTime);
                    breakdownMap.put(
                        maxBreakdownTypeCount,
                        Math.max(breakdownMap.getOrDefault(maxBreakdownTypeCount, Long.MIN_VALUE), breakdownCount)
                    );
                    breakdownMap.put(
                        minBreakdownTypeCount,
                        Math.min(breakdownMap.getOrDefault(minBreakdownTypeCount, Long.MAX_VALUE), breakdownCount)
                    );
                    breakdownMap.put(avgBreakdownTypeCount, breakdownMap.getOrDefault(avgBreakdownTypeCount, 0L) + breakdownCount);
                }
                // Finding the max slice end time and min slice start time to compute total time
                String maxEndTime = timingType + MAX_END_TIME_SUFFIX;
                String minStartTime = timingType + MIN_START_TIME_SUFFIX;
                Long maxEndTimeValue = slice.getValue().get(timingType + MAX_END_TIME_SUFFIX);
                Long minStartTimeValue = slice.getValue().get(timingType + MIN_START_TIME_SUFFIX);
                totalTimeMap.put(maxEndTime, Math.max(totalTimeMap.getOrDefault(maxEndTime, Long.MIN_VALUE), maxEndTimeValue));
                totalTimeMap.put(minStartTime, Math.min(totalTimeMap.getOrDefault(minStartTime, Long.MAX_VALUE), minStartTimeValue));
                // Computing the total count for each breakdown across all slices
                String breakdownCount = timingType + TIMING_TYPE_COUNT_SUFFIX;
                Long breakdownCountValue = slice.getValue().get(breakdownCount);
                if (queryTimingType != QueryTimingType.CREATE_WEIGHT) {
                    breakdownMap.put(breakdownCount, breakdownMap.getOrDefault(breakdownCount, 0L) + breakdownCountValue);
                } else {
                    breakdownMap.put(breakdownCount, 1L);
                    breakdownMap.put(timingType, slice.getValue().get(timingType));
                }
            }
        }
        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
            String timingType = queryTimingType.toString();
            if (queryTimingType != QueryTimingType.CREATE_WEIGHT) {
                // Computing avg time/count stats
                String avgBreakdownTypeTime = AVG_PREFIX + timingType;
                String avgBreakdownTypeCount = avgBreakdownTypeTime + TIMING_TYPE_COUNT_SUFFIX;
                breakdownMap.put(avgBreakdownTypeTime, breakdownMap.get(avgBreakdownTypeTime) / sliceCount);
                breakdownMap.put(avgBreakdownTypeCount, breakdownMap.get(avgBreakdownTypeCount) / sliceCount);
                // Computing total time for each breakdown including any wait time
                Long maxSliceEndTime = totalTimeMap.get(timingType + MAX_END_TIME_SUFFIX);
                Long mingSliceStartTime = totalTimeMap.get(timingType + MIN_START_TIME_SUFFIX);
                breakdownMap.put(timingType, maxSliceEndTime - mingSliceStartTime);
            }
        }
        return breakdownMap;
    }

    @Override
    public long toNodeTime() {
        return breakdownMaxEndTime - breakdownMinStartTime;
    }

    public Long getMaxSliceNodeTime() {
        return maxSliceNodeTime;
    }

    public Long getMinSliceNodeTime() {
        return minSliceNodeTime;
    }

    public Long getAvgSliceNodeTime() {
        return avgSliceNodeTime;
    }

    @Override
    public void associateCollectorToLeaves(LeafReaderContext leaf, Collector collector) {
        if (collector != null) {
            String collectorManager = collector.toString();
            List<LeafReaderContext> leaves = collectorToLeaves.getOrDefault(collectorManager, new ArrayList<>());
            leaves.add(leaf);
            collectorToLeaves.put(collectorManager, leaves);
        }
    }
}
