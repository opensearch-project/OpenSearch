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
import org.opensearch.OpenSearchException;
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
    static final String SLICE_END_TIME_SUFFIX = "_slice_end_time";
    static final String SLICE_START_TIME_SUFFIX = "_slice_start_time";
    static final String MAX_PREFIX = "max_";
    static final String MIN_PREFIX = "min_";
    static final String AVG_PREFIX = "avg_";
    private long queryNodeTime = Long.MIN_VALUE;
    private long maxSliceNodeTime = Long.MIN_VALUE;
    private long minSliceNodeTime = Long.MAX_VALUE;
    private long avgSliceNodeTime = 0L;

    // keep track of all breakdown timings per segment. package-private for testing
    private final Map<Object, AbstractProfileBreakdown<QueryTimingType>> contexts = new ConcurrentHashMap<>();

    // represents slice to leaves mapping as for each slice a unique collector instance is created
    private final Map<Collector, List<LeafReaderContext>> sliceCollectorsToLeaves = new ConcurrentHashMap<>();

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
        final Map<String, Long> topLevelBreakdownMapWithWeightTime = super.toBreakdownMap();
        final long createWeightStartTime = topLevelBreakdownMapWithWeightTime.get(
            QueryTimingType.CREATE_WEIGHT + TIMING_TYPE_START_TIME_SUFFIX
        );
        final long createWeightTime = topLevelBreakdownMapWithWeightTime.get(QueryTimingType.CREATE_WEIGHT.toString());

        if (contexts.isEmpty()) {
            // If there are no leaf contexts, then return the default concurrent query level breakdown, which will include the
            // create_weight time/count
            queryNodeTime = createWeightTime;
            maxSliceNodeTime = 0L;
            minSliceNodeTime = 0L;
            avgSliceNodeTime = 0L;
            return buildDefaultQueryBreakdownMap(createWeightTime);
        } else if (sliceCollectorsToLeaves.isEmpty()) {
            // This will happen when each slice executes search leaf for its leaves and query is rewritten for the leaf being searched. It
            // creates a new weight and breakdown map for each rewritten query. This new breakdown map captures the timing information for
            // the new rewritten query. The sliceCollectorsToLeaves is empty because this breakdown for rewritten query gets created later
            // in search leaf path which doesn't have collector. Also, this is not needed since this breakdown is per leaf and there is no
            // concurrency involved.
            assert contexts.size() == 1 : "Unexpected size: "
                + contexts.size()
                + " of leaves breakdown in ConcurrentQueryProfileBreakdown of rewritten query for a leaf.";
            AbstractProfileBreakdown<QueryTimingType> breakdown = contexts.values().iterator().next();
            queryNodeTime = breakdown.toNodeTime() + createWeightTime;
            maxSliceNodeTime = 0L;
            minSliceNodeTime = 0L;
            avgSliceNodeTime = 0L;
            Map<String, Long> queryBreakdownMap = new HashMap<>(breakdown.toBreakdownMap());
            queryBreakdownMap.put(QueryTimingType.CREATE_WEIGHT.toString(), createWeightTime);
            queryBreakdownMap.put(QueryTimingType.CREATE_WEIGHT + TIMING_TYPE_COUNT_SUFFIX, 1L);
            return queryBreakdownMap;
        }

        // first create the slice level breakdowns
        final Map<Collector, Map<String, Long>> sliceLevelBreakdowns = buildSliceLevelBreakdown();
        return buildQueryBreakdownMap(sliceLevelBreakdowns, createWeightTime, createWeightStartTime);
    }

    /**
     * @param createWeightTime time for creating weight
     * @return default breakdown map for concurrent query which includes the create weight time and all other timing type stats in the
     * breakdown has default value of 0. For concurrent search case, the max/min/avg stats for each timing type will also be 0 in this
     * default breakdown map.
     */
    private Map<String, Long> buildDefaultQueryBreakdownMap(long createWeightTime) {
        final Map<String, Long> concurrentQueryBreakdownMap = new HashMap<>();
        for (QueryTimingType timingType : QueryTimingType.values()) {
            final String timingTypeKey = timingType.toString();
            final String timingTypeCountKey = timingTypeKey + TIMING_TYPE_COUNT_SUFFIX;

            if (timingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                concurrentQueryBreakdownMap.put(timingTypeKey, createWeightTime);
                concurrentQueryBreakdownMap.put(timingTypeCountKey, 1L);
                continue;
            }
            final String maxBreakdownTypeTime = MAX_PREFIX + timingTypeKey;
            final String minBreakdownTypeTime = MIN_PREFIX + timingTypeKey;
            final String avgBreakdownTypeTime = AVG_PREFIX + timingTypeKey;
            final String maxBreakdownTypeCount = MAX_PREFIX + timingTypeCountKey;
            final String minBreakdownTypeCount = MIN_PREFIX + timingTypeCountKey;
            final String avgBreakdownTypeCount = AVG_PREFIX + timingTypeCountKey;
            // add time related stats
            concurrentQueryBreakdownMap.put(timingTypeKey, 0L);
            concurrentQueryBreakdownMap.put(maxBreakdownTypeTime, 0L);
            concurrentQueryBreakdownMap.put(minBreakdownTypeTime, 0L);
            concurrentQueryBreakdownMap.put(avgBreakdownTypeTime, 0L);
            // add count related stats
            concurrentQueryBreakdownMap.put(timingTypeCountKey, 0L);
            concurrentQueryBreakdownMap.put(maxBreakdownTypeCount, 0L);
            concurrentQueryBreakdownMap.put(minBreakdownTypeCount, 0L);
            concurrentQueryBreakdownMap.put(avgBreakdownTypeCount, 0L);
        }
        return concurrentQueryBreakdownMap;
    }

    /**
     * Computes the slice level breakdownMap. It uses sliceCollectorsToLeaves to figure out all the leaves or segments part of a slice.
     * Then use the breakdown timing stats for each of these leaves to calculate the breakdown stats at slice level.
     *
     * @return map of collector (or slice) to breakdown map
     */
    Map<Collector, Map<String, Long>> buildSliceLevelBreakdown() {
        final Map<Collector, Map<String, Long>> sliceLevelBreakdowns = new HashMap<>();
        long totalSliceNodeTime = 0L;
        for (Map.Entry<Collector, List<LeafReaderContext>> slice : sliceCollectorsToLeaves.entrySet()) {
            final Collector sliceCollector = slice.getKey();
            // initialize each slice level breakdown
            final Map<String, Long> currentSliceBreakdown = sliceLevelBreakdowns.computeIfAbsent(sliceCollector, k -> new HashMap<>());
            // max slice end time across all timing types
            long sliceMaxEndTime = Long.MIN_VALUE;
            long sliceMinStartTime = Long.MAX_VALUE;
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
                    if (!contexts.containsKey(sliceLeaf)) {
                        // In case like early termination, the sliceCollectorToLeave association will be added for a
                        // leaf, but the leaf level breakdown will not be created in the contexts map.
                        // This is because before updating the contexts map, the query hits earlyTerminationException.
                        // To handle such case, we will ignore the leaf that is not present.
                        //
                        // Other than early termination, it can also happen in other cases. For example: there is a must boolean query
                        // with 2 boolean clauses. While creating scorer for first clause if no docs are found for the field in a leaf
                        // context then it will return null scorer. Then for 2nd clause weight as well no scorer will be created for this
                        // leaf context (as it is a must query). Due to this it will end up missing the leaf context in the contexts map
                        // for second clause weight.
                        continue;
                    }
                    final Map<String, Long> currentSliceLeafBreakdownMap = contexts.get(sliceLeaf).toBreakdownMap();
                    // get the count for current leaf timing type
                    final long sliceLeafTimingTypeCount = currentSliceLeafBreakdownMap.get(timingTypeCountKey);
                    currentSliceBreakdown.compute(
                        timingTypeCountKey,
                        (key, value) -> (value == null) ? sliceLeafTimingTypeCount : value + sliceLeafTimingTypeCount
                    );

                    if (sliceLeafTimingTypeCount == 0L) {
                        // In case where a slice with multiple leaves, it is possible that any one of the leaves has 0 invocations for a
                        // specific breakdown type. We should skip the slice start/end time computation for any leaf with 0 invocations on a
                        // timing type, as 0 does not represent an actual timing.
                        // For example, a slice has 0 invocations for a breakdown type from its leading leaves. Another example, let's
                        // consider a slice with three leaves: leaf A with a score count of 5, leaf B with a score count of 0,
                        // and leaf C with a score count of 4. In this situation, we only compute the timing type slice start/end time based
                        // on leaf A and leaf C. This is because leaf B has a start time of zero.
                        continue;
                    }

                    // compute the sliceStartTime for timingType using min of startTime across slice leaves
                    final long sliceLeafTimingTypeStartTime = currentSliceLeafBreakdownMap.get(timingTypeStartKey);
                    currentSliceBreakdown.compute(
                        timingTypeSliceStartTimeKey,
                        (key, value) -> (value == null) ? sliceLeafTimingTypeStartTime : Math.min(value, sliceLeafTimingTypeStartTime)
                    );

                    // compute the sliceEndTime for timingType using max of endTime across slice leaves
                    final long sliceLeafTimingTypeEndTime = sliceLeafTimingTypeStartTime + currentSliceLeafBreakdownMap.get(
                        timingType.toString()
                    );
                    currentSliceBreakdown.compute(
                        timingTypeSliceEndTimeKey,
                        (key, value) -> (value == null) ? sliceLeafTimingTypeEndTime : Math.max(value, sliceLeafTimingTypeEndTime)
                    );
                }
                // Only when we've checked all leaves in a slice and still find no invocations, then we should set the slice start/end time
                // to the default 0L. This is because buildQueryBreakdownMap expects timingTypeSliceStartTimeKey and
                // timingTypeSliceEndTimeKey in the slice level breakdowns.
                if (currentSliceBreakdown.get(timingTypeCountKey) != null && currentSliceBreakdown.get(timingTypeCountKey) == 0L) {
                    currentSliceBreakdown.put(timingTypeSliceStartTimeKey, 0L);
                    currentSliceBreakdown.put(timingTypeSliceEndTimeKey, 0L);
                }
                // compute sliceMaxEndTime as max of sliceEndTime across all timing types
                sliceMaxEndTime = Math.max(sliceMaxEndTime, currentSliceBreakdown.getOrDefault(timingTypeSliceEndTimeKey, Long.MIN_VALUE));
                long currentSliceStartTime = currentSliceBreakdown.getOrDefault(timingTypeSliceStartTimeKey, Long.MAX_VALUE);
                if (currentSliceStartTime == 0L) {
                    // The timer for the current timing type never starts, so we continue here
                    continue;
                }
                sliceMinStartTime = Math.min(sliceMinStartTime, currentSliceStartTime);
                // compute total time for each timing type at slice level using sliceEndTime and sliceStartTime
                currentSliceBreakdown.put(
                    timingType.toString(),
                    currentSliceBreakdown.getOrDefault(timingTypeSliceEndTimeKey, 0L) - currentSliceBreakdown.getOrDefault(
                        timingTypeSliceStartTimeKey,
                        0L
                    )
                );
            }
            // currentSliceNodeTime does not include the create weight time, as that is computed in non-concurrent part
            long currentSliceNodeTime;
            if (sliceMinStartTime == Long.MAX_VALUE && sliceMaxEndTime == Long.MIN_VALUE) {
                currentSliceNodeTime = 0L;
            } else if (sliceMinStartTime == Long.MAX_VALUE || sliceMaxEndTime == Long.MIN_VALUE) {
                throw new OpenSearchException(
                    "Unexpected value of sliceMinStartTime ["
                        + sliceMinStartTime
                        + "] or sliceMaxEndTime ["
                        + sliceMaxEndTime
                        + "] while computing the slice level timing profile breakdowns"
                );
            } else {
                currentSliceNodeTime = sliceMaxEndTime - sliceMinStartTime;
            }

            // compute max/min slice times
            maxSliceNodeTime = Math.max(maxSliceNodeTime, currentSliceNodeTime);
            minSliceNodeTime = Math.min(minSliceNodeTime, currentSliceNodeTime);
            // total time at query level
            totalSliceNodeTime += currentSliceNodeTime;
        }
        avgSliceNodeTime = totalSliceNodeTime / sliceCollectorsToLeaves.size();
        return sliceLevelBreakdowns;
    }

    /**
     * Computes the query level breakdownMap using the breakdown maps of all the slices. In query level breakdown map, it has the
     * time/count stats for each breakdown type. Total time per breakdown type at query level is computed by subtracting the max of slice
     * end time with min of slice start time for that type. Count for each breakdown type at query level is sum of count of that type
     * across slices. Other than these, there are max/min/avg stats across slices for each breakdown type
     *
     * @param sliceLevelBreakdowns  breakdown map for all the slices
     * @param createWeightTime      time for create weight
     * @param createWeightStartTime start time for create weight
     * @return breakdown map for entire query
     */
    public Map<String, Long> buildQueryBreakdownMap(
        Map<Collector, Map<String, Long>> sliceLevelBreakdowns,
        long createWeightTime,
        long createWeightStartTime
    ) {
        final Map<String, Long> queryBreakdownMap = new HashMap<>();
        long queryEndTime = Long.MIN_VALUE;
        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
            final String timingTypeKey = queryTimingType.toString();
            final String timingTypeCountKey = timingTypeKey + TIMING_TYPE_COUNT_SUFFIX;
            final String sliceEndTimeForTimingType = timingTypeKey + SLICE_END_TIME_SUFFIX;
            final String sliceStartTimeForTimingType = timingTypeKey + SLICE_START_TIME_SUFFIX;

            final String maxBreakdownTypeTime = MAX_PREFIX + timingTypeKey;
            final String minBreakdownTypeTime = MIN_PREFIX + timingTypeKey;
            final String avgBreakdownTypeTime = AVG_PREFIX + timingTypeKey;
            final String maxBreakdownTypeCount = MAX_PREFIX + timingTypeCountKey;
            final String minBreakdownTypeCount = MIN_PREFIX + timingTypeCountKey;
            final String avgBreakdownTypeCount = AVG_PREFIX + timingTypeCountKey;

            long queryTimingTypeEndTime = Long.MIN_VALUE;
            long queryTimingTypeStartTime = Long.MAX_VALUE;
            long queryTimingTypeCount = 0L;

            // the create weight time is computed at the query level and is called only once per query
            if (queryTimingType == QueryTimingType.CREATE_WEIGHT) {
                queryBreakdownMap.put(timingTypeCountKey, 1L);
                queryBreakdownMap.put(timingTypeKey, createWeightTime);
                continue;
            }

            // for all other timing types, we will compute min/max/avg/total across slices
            for (Map.Entry<Collector, Map<String, Long>> sliceBreakdown : sliceLevelBreakdowns.entrySet()) {
                long sliceBreakdownTypeTime = sliceBreakdown.getValue().getOrDefault(timingTypeKey, 0L);
                long sliceBreakdownTypeCount = sliceBreakdown.getValue().getOrDefault(timingTypeCountKey, 0L);
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
                queryTimingTypeEndTime = Math.max(
                    queryTimingTypeEndTime,
                    sliceBreakdown.getValue().getOrDefault(sliceEndTimeForTimingType, Long.MIN_VALUE)
                );
                queryTimingTypeStartTime = Math.min(
                    queryTimingTypeStartTime,
                    sliceBreakdown.getValue().getOrDefault(sliceStartTimeForTimingType, Long.MAX_VALUE)
                );
                queryTimingTypeCount += sliceBreakdownTypeCount;
            }

            if (queryTimingTypeStartTime == Long.MAX_VALUE || queryTimingTypeEndTime == Long.MIN_VALUE) {
                throw new OpenSearchException(
                    "Unexpected timing type ["
                        + timingTypeKey
                        + "] start ["
                        + queryTimingTypeStartTime
                        + "] or end time ["
                        + queryTimingTypeEndTime
                        + "] computed across slices for profile results"
                );
            }
            queryBreakdownMap.put(timingTypeKey, queryTimingTypeEndTime - queryTimingTypeStartTime);
            queryBreakdownMap.put(timingTypeCountKey, queryTimingTypeCount);
            queryBreakdownMap.compute(avgBreakdownTypeTime, (key, value) -> (value == null) ? 0L : value / sliceLevelBreakdowns.size());
            queryBreakdownMap.compute(avgBreakdownTypeCount, (key, value) -> (value == null) ? 0L : value / sliceLevelBreakdowns.size());
            // compute query end time using max of query end time across all timing types
            queryEndTime = Math.max(queryEndTime, queryTimingTypeEndTime);
        }
        if (queryEndTime == Long.MIN_VALUE) {
            throw new OpenSearchException("Unexpected error while computing the query end time across slices in profile result");
        }
        queryNodeTime = queryEndTime - createWeightStartTime;
        return queryBreakdownMap;
    }

    @Override
    public long toNodeTime() {
        return queryNodeTime;
    }

    @Override
    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf) {
        // Each slice (or collector) is executed by single thread. So the list for a key will always be updated by a single thread only
        sliceCollectorsToLeaves.computeIfAbsent(collector, k -> new ArrayList<>()).add(leaf);
    }

    @Override
    public void associateCollectorsToLeaves(Map<Collector, List<LeafReaderContext>> collectorsToLeaves) {
        sliceCollectorsToLeaves.putAll(collectorsToLeaves);
    }

    Map<Collector, List<LeafReaderContext>> getSliceCollectorsToLeaves() {
        return Collections.unmodifiableMap(sliceCollectorsToLeaves);
    }

    // used by tests
    Map<Object, AbstractProfileBreakdown<QueryTimingType>> getContexts() {
        return contexts;
    }

    long getMaxSliceNodeTime() {
        return maxSliceNodeTime;
    }

    long getMinSliceNodeTime() {
        return minSliceNodeTime;
    }

    long getAvgSliceNodeTime() {
        return avgSliceNodeTime;
    }
}
