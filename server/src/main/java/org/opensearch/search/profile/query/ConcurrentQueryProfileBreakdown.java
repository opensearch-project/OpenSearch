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
import java.util.Collections;
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
    private static final String SLICE_END_TIME_SUFFIX = "_slice_end_time";
    private static final String SLICE_START_TIME_SUFFIX = "_slice_start_time";
    private static final String MAX_PREFIX = "max_";
    private static final String MIN_PREFIX = "min_";
    private static final String AVG_PREFIX = "avg_";
    private long queryEndTime = Long.MIN_VALUE;
    private long queryStartTime = Long.MAX_VALUE;
    private long maxSliceNodeTime = Long.MIN_VALUE;
    private long minSliceNodeTime = Long.MAX_VALUE;
    private long avgSliceNodeTime = 0L;

    // keep track of all breakdown timings per segment
    private final Map<Object, AbstractProfileBreakdown<QueryTimingType>> contexts = new ConcurrentHashMap<>();

    // represents slice to leaves mapping as for each slice a unique collector instance is created
    private final Map<Collector, List<LeafReaderContext>> sliceCollectorToLeaves = new HashMap<>();

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
        if (sliceCollectorToLeaves.isEmpty()) {
            return super.toBreakdownMap();
        }

        final Map<String, Long> topLevelBreakdownMapWithWeightTime = super.toBreakdownMap();
        final long createWeightStartTime = topLevelBreakdownMapWithWeightTime.get(
            QueryTimingType.CREATE_WEIGHT + TIMING_TYPE_START_TIME_SUFFIX
        );
        queryStartTime = createWeightStartTime;
        // first create the slice level breakdowns
        final Map<Collector, Map<String, Long>> sliceLevelBreakdowns = buildSliceLevelBreakdown(createWeightStartTime);
        return buildQueryBreakdownMap(sliceLevelBreakdowns, createWeightStartTime);
    }

    /**
     * Computes the slice level breakdownMap. It uses sliceCollectorToLeaves to figure out all the leaves or segments part of a slice.
     * Then use the breakdown timing stats for each of these leaves to calculate the breakdown stats at slice level.
     * @param createWeightStartTime start time when createWeight is called
     * @return map of collector (or slice) to breakdown map
     */
    private Map<Collector, Map<String, Long>> buildSliceLevelBreakdown(long createWeightStartTime) {
        final Map<Collector, Map<String, Long>> sliceLevelBreakdowns = new HashMap<>();
        long totalSliceNodeTime = 0;
        for (Map.Entry<Collector, List<LeafReaderContext>> slice : sliceCollectorToLeaves.entrySet()) {
            final Collector sliceCollector = slice.getKey();
            // initialize each slice level breakdown
            final Map<String, Long> currentSliceBreakdown = sliceLevelBreakdowns.computeIfAbsent(sliceCollector, k -> new HashMap<>());
            // max slice end time across all timing types
            long sliceMaxEndTime = Long.MIN_VALUE;
            for (QueryTimingType timingType : QueryTimingType.values()) {
                if (timingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                    // do nothing for create weight as that is query level time and not slice level
                    continue;
                }

                // for each timing type compute maxSliceEndTime and minSliceStartTime. Also add the counts of timing type to
                // compute total count at slice level
                final String timingTypeCountKey = timingType + TIMING_TYPE_COUNT_SUFFIX;
                final String timingTypeStartKey = timingType + TIMING_TYPE_START_TIME_SUFFIX;
                final String timingTypeSliceStartTimeKey = timingType + SLICE_START_TIME_SUFFIX;
                final String timingTypeSliceEndTimeKey = timingType + SLICE_END_TIME_SUFFIX;

                for (LeafReaderContext sliceLeaf : slice.getValue()) {
                    final Map<String, Long> currentSliceLeafBreakdownMap = contexts.get(sliceLeaf).toBreakdownMap();
                    // get the count for current leaf timing type
                    currentSliceBreakdown.compute(
                        timingTypeCountKey,
                        (key, value) -> (value == null) ? 0 : value + currentSliceLeafBreakdownMap.get(timingTypeCountKey)
                    );

                    // compute the sliceEndTime for timingType using max of endTime across slice leaves
                    final long sliceLeafTimingTypeEndTime = currentSliceLeafBreakdownMap.get(timingTypeStartKey)
                        + currentSliceLeafBreakdownMap.get(timingType.toString());
                    currentSliceBreakdown.compute(
                        timingTypeSliceEndTimeKey,
                        (key, value) -> (value == null) ? sliceLeafTimingTypeEndTime : Math.max(value, sliceLeafTimingTypeEndTime)
                    );

                    // compute the sliceStartTime for timingType using min of startTime across slice leaves
                    final long sliceLeafTimingTypeStartTime = currentSliceLeafBreakdownMap.get(timingTypeStartKey);
                    currentSliceBreakdown.compute(
                        timingTypeSliceStartTimeKey,
                        (key, value) -> (value == null) ? sliceLeafTimingTypeStartTime : Math.min(value, sliceLeafTimingTypeStartTime)
                    );
                }
                // compute sliceMaxEndTime as max of sliceEndTime across all timing types
                sliceMaxEndTime = Math.max(sliceMaxEndTime, currentSliceBreakdown.get(timingTypeSliceEndTimeKey));
                // compute total time for each timing type at slice level using sliceEndTime and sliceStartTime
                currentSliceBreakdown.put(
                    timingType.toString(),
                    currentSliceBreakdown.get(timingTypeSliceEndTimeKey) - currentSliceBreakdown.get(timingTypeSliceStartTimeKey)
                );
            }
            // currentSliceNodeTime includes the create weight time as well which will be same for all the slices
            long currentSliceNodeTime = sliceMaxEndTime - createWeightStartTime;
            // compute max/min slice times
            maxSliceNodeTime = Math.max(maxSliceNodeTime, currentSliceNodeTime);
            minSliceNodeTime = Math.min(minSliceNodeTime, currentSliceNodeTime);
            // total time at query level
            totalSliceNodeTime += currentSliceNodeTime;
        }
        avgSliceNodeTime = totalSliceNodeTime / sliceCollectorToLeaves.size();
        return sliceLevelBreakdowns;
    }

    /**
     * Computes the query level breakdownMap using the breakdown maps of all the slices. In query level breakdown map, it has the
     * time/count stats for each breakdown type. Total time per breakdown type at query level is computed by subtracting the max of slice
     * end time with min of slice start time for that type. Count for each breakdown type at query level is sum of count of that type
     * across slices. Other than these, there are max/min/avg stats across slices for each breakdown type
     * @param sliceLevelBreakdowns breakdown map for all the slices
     * @param createWeightStartTime start time of create weight
     * @return breakdown map for entire query
     */
    private Map<String, Long> buildQueryBreakdownMap(Map<Collector, Map<String, Long>> sliceLevelBreakdowns, long createWeightStartTime) {
        final Map<String, Long> queryBreakdownMap = new HashMap<>();
        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
            final String timingTypeKey = queryTimingType.toString();
            final String timingTypeCountKey = timingTypeKey + TIMING_TYPE_COUNT_SUFFIX;
            final String sliceEndTimeForTimingType = timingTypeKey + SLICE_END_TIME_SUFFIX;
            final String sliceStartTimeForTimingType = timingTypeKey + SLICE_START_TIME_SUFFIX;

            final String maxBreakdownTypeTime = MAX_PREFIX + timingTypeKey;
            final String minBreakdownTypeTime = MIN_PREFIX + timingTypeKey;
            final String avgBreakdownTypeTime = AVG_PREFIX + timingTypeKey;
            final String maxBreakdownTypeCount = maxBreakdownTypeTime + TIMING_TYPE_COUNT_SUFFIX;
            final String minBreakdownTypeCount = minBreakdownTypeTime + TIMING_TYPE_COUNT_SUFFIX;
            final String avgBreakdownTypeCount = avgBreakdownTypeTime + TIMING_TYPE_COUNT_SUFFIX;

            long queryTimingTypeEndTime = Long.MIN_VALUE;
            long queryTimingTypeStartTime = Long.MAX_VALUE;
            long queryTimingTypeCount = 0L;

            // the create weight time is computed at the query level and is called only once per query
            if (queryTimingType == QueryTimingType.CREATE_WEIGHT) {
                queryBreakdownMap.put(timingTypeCountKey, 1L);
                queryBreakdownMap.put(timingTypeKey, createWeightStartTime);
                continue;
            }

            // for all other timing types, we will compute min/max/avg/total across slices
            for (Map.Entry<Collector, Map<String, Long>> sliceBreakdown : sliceLevelBreakdowns.entrySet()) {
                Long sliceBreakdownTypeTime = sliceBreakdown.getValue().get(timingTypeKey);
                Long sliceBreakdownTypeCount = sliceBreakdown.getValue().get(timingTypeCountKey);
                // compute max/min/avg TimingType time across slices
                queryBreakdownMap.compute(
                    maxBreakdownTypeTime,
                    (key, value) -> (value == null) ? sliceBreakdownTypeTime : Math.max(sliceBreakdownTypeTime, value)
                );
                queryBreakdownMap.compute(
                    minBreakdownTypeTime,
                    (key, value) -> (value == null) ? sliceBreakdownTypeTime : Math.min(sliceBreakdownTypeTime, value)
                );
                queryBreakdownMap.compute(
                    avgBreakdownTypeTime,
                    (key, value) -> (value == null) ? sliceBreakdownTypeTime : sliceBreakdownTypeTime + value
                );

                // compute max/min/avg TimingType count across slices
                queryBreakdownMap.compute(
                    maxBreakdownTypeCount,
                    (key, value) -> (value == null) ? sliceBreakdownTypeCount : Math.max(sliceBreakdownTypeCount, value)
                );
                queryBreakdownMap.compute(
                    minBreakdownTypeCount,
                    (key, value) -> (value == null) ? sliceBreakdownTypeCount : Math.min(sliceBreakdownTypeCount, value)
                );
                queryBreakdownMap.compute(
                    avgBreakdownTypeCount,
                    (key, value) -> (value == null) ? sliceBreakdownTypeCount : sliceBreakdownTypeCount + value
                );

                // query start/end time for a TimingType is min/max of start/end time across slices for that TimingType
                queryTimingTypeEndTime = Math.max(queryTimingTypeEndTime, sliceBreakdown.getValue().get(sliceEndTimeForTimingType));
                queryTimingTypeStartTime = Math.min(queryTimingTypeStartTime, sliceBreakdown.getValue().get(sliceStartTimeForTimingType));
                queryTimingTypeCount += sliceBreakdownTypeCount;
            }
            queryBreakdownMap.put(timingTypeKey, queryTimingTypeEndTime - queryTimingTypeStartTime);
            queryBreakdownMap.put(timingTypeCountKey, queryTimingTypeCount);
            queryBreakdownMap.compute(avgBreakdownTypeTime, (key, value) -> (value == null) ? 0 : value / sliceLevelBreakdowns.size());
            queryBreakdownMap.compute(avgBreakdownTypeCount, (key, value) -> (value == null) ? 0 : value / sliceLevelBreakdowns.size());
            // compute query end time using max of query end time across all timing types
            queryEndTime = Math.max(queryEndTime, queryTimingTypeEndTime);
        }
        return queryBreakdownMap;
    }

    @Override
    public long toNodeTime() {
        return queryEndTime - queryStartTime;
    }

    @Override
    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf) {
        sliceCollectorToLeaves.computeIfAbsent(collector, k -> new ArrayList<>()).add(leaf);
    }

    @Override
    public void associateCollectorToLeaves(Map<Collector, List<LeafReaderContext>> collectorToLeaves) {
        this.sliceCollectorToLeaves.putAll(collectorToLeaves);
    }

    Map<Collector, List<LeafReaderContext>> getSliceCollectorToLeaves() {
        return Collections.unmodifiableMap(sliceCollectorToLeaves);
    }

    Long getMaxSliceNodeTime() {
        return maxSliceNodeTime;
    }

    Long getMinSliceNodeTime() {
        return minSliceNodeTime;
    }

    Long getAvgSliceNodeTime() {
        return avgSliceNodeTime;
    }
}
