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
import org.opensearch.search.profile.ProfileMetric;
import org.opensearch.search.profile.SliceProfileResult;
import org.opensearch.search.profile.Timer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.opensearch.search.profile.Timer.TIMING_TYPE_COUNT_SUFFIX;
import static org.opensearch.search.profile.Timer.TIMING_TYPE_START_TIME_SUFFIX;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc). The class supports profiling the concurrent search over segments.
 *
 * @opensearch.internal
 */
public final class ConcurrentQueryProfileBreakdown extends ContextualProfileBreakdown {
    static final String SLICE_END_TIME_SUFFIX = "_slice_end_time";
    static final String SLICE_START_TIME_SUFFIX = "_slice_start_time";
    static final String MAX_PREFIX = "max_";
    static final String MIN_PREFIX = "min_";
    static final String AVG_PREFIX = "avg_";
    private long queryNodeTime = Long.MIN_VALUE;
    private long maxSliceNodeTime = Long.MIN_VALUE;
    private long minSliceNodeTime = Long.MAX_VALUE;
    private long avgSliceNodeTime = 0L;

    // keep track of all breakdown timings per (thread, segment). package-private for testing.
    // Under intra-segment search a single segment (LeafReaderContext) is split into partitions that
    // run on different threads. Keying only by the segment would make those partitions share one
    // breakdown — and therefore one non-thread-safe Timer — so concurrent start()/stop() calls would
    // race and corrupt the timing. Qualifying the key by the searching thread gives each partition of
    // a split segment its own breakdown/Timer. See #contextKey.
    private final Map<Object, AbstractProfileBreakdown> contexts = new ConcurrentHashMap<>();

    // represents slice to leaves mapping as for each slice a unique collector instance is created
    private final Map<Collector, List<LeafReaderContext>> sliceCollectorsToLeaves = new ConcurrentHashMap<>();

    // The id of the thread that searched each slice (collector), captured at associateCollectorToLeaves
    // time (i.e. during that slice's searchLeaf, on the slice's thread). Lets the reduce reconstruct
    // the (threadId, leaf) breakdown key for each slice. One thread per slice, so this is 1:1.
    private final Map<Collector, Long> sliceCollectorThreads = new ConcurrentHashMap<>();

    // Additive per-slice breakdowns captured during the eager reduce. These are the raw per-slice
    // detail from which max_/min_/avg_ are derived; retained (instead of discarded) so consumers can
    // inspect individual slices. Populated in buildSliceLevelBreakdown; does not affect the existing
    // aggregates.
    private final List<SliceProfileResult> sliceProfileResults = new ArrayList<>();

    // Additive side map recording the doc-id range [minDocId, maxDocId) each leaf was searched with,
    // captured at the searchLeaf seam (where the bounds are in scope). Used only to attach doc-ranges
    // to the per-slice partitions; the existing sliceCollectorsToLeaves reduce is unaffected.
    // Keyed by (collector, leaf) so that when a single segment is split across multiple slices
    // (intra-segment search), each slice's partition of that leaf records its OWN doc-id range.
    // Whole-segment partitions record [0, segment maxDoc) (resolved from the NO_MORE_DOCS sentinel at
    // the searchLeaf seam) so the reported doc_range reflects the real segment size.
    private final Map<Collector, Map<LeafReaderContext, int[]>> sliceLeafDocRanges = new ConcurrentHashMap<>();

    private final Collection<Supplier<ProfileMetric>> metricSuppliers;
    private final Set<String> timingMetrics;
    private final Set<String> nonTimingMetrics;

    public ConcurrentQueryProfileBreakdown(Collection<Supplier<ProfileMetric>> metricSuppliers) {
        super(metricSuppliers);
        this.metricSuppliers = metricSuppliers;
        this.timingMetrics = getTimingMetrics();
        this.nonTimingMetrics = getNonTimingMetrics();
    }

    @Override
    public AbstractProfileBreakdown context(Object context) {
        // Qualify the per-leaf breakdown with the thread doing the search, so that intra-segment
        // partitions of the same segment (which run on different threads) get separate breakdowns
        // instead of racing on one shared, non-thread-safe Timer. The thread is the right
        // discriminator here: it is available at scoring time for both parent and child query nodes
        // (child breakdowns learn their collector only later, during tree assembly), and OpenSearch
        // runs one thread per slice, so keying by thread separates exactly the concurrent partitions
        // that would otherwise collide. The collector→thread mapping recorded in
        // associateCollectorToLeaves lets the reduce reconstruct this key.
        final Object key = contextKey(Thread.currentThread().threadId(), context);
        // See please https://bugs.openjdk.java.net/browse/JDK-8161372
        final AbstractProfileBreakdown profile = contexts.get(key);

        if (profile != null) {
            return profile;
        }

        return contexts.computeIfAbsent(key, ctx -> new QueryProfileBreakdown(metricSuppliers));
    }

    /**
     * Builds the key under which a leaf's breakdown is stored: the searching thread's id paired with
     * the segment (leaf). When no thread id is known (e.g. a reduce lookup for a collector whose
     * thread was never recorded), the leaf is used directly, preserving the original single-key
     * behavior. Keying by thread id mirrors how {@link ConcurrentQueryProfiler} already separates
     * concurrent work.
     */
    static Object contextKey(Long threadId, Object leaf) {
        return (threadId == null) ? leaf : new ThreadLeafKey(threadId, leaf);
    }

    /** Composite key pairing the searching thread's id with the segment (leaf) it searched. */
    private record ThreadLeafKey(long threadId, Object leaf) {
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
            AbstractProfileBreakdown breakdown = contexts.values().iterator().next();
            queryNodeTime = breakdown.toNodeTime() + createWeightTime;
            maxSliceNodeTime = 0L;
            minSliceNodeTime = 0L;
            avgSliceNodeTime = 0L;
            Map<String, Long> queryBreakdownMap = new TreeMap<>(breakdown.toBreakdownMap());
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
        final Map<String, Long> concurrentQueryBreakdownMap = new TreeMap<>();
        for (QueryTimingType timingType : QueryTimingType.values()) {
            final String timingTypeKey = timingType.toString();
            final String timingTypeCountKey = timingType + TIMING_TYPE_COUNT_SUFFIX;

            if (timingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                concurrentQueryBreakdownMap.put(timingTypeKey, createWeightTime);
                concurrentQueryBreakdownMap.put(timingTypeCountKey, 1L);
                continue;
            }
            final String maxBreakdownTypeTime = MAX_PREFIX + timingType;
            final String minBreakdownTypeTime = MIN_PREFIX + timingType;
            final String avgBreakdownTypeTime = AVG_PREFIX + timingType;
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
        // Rebuild the per-slice results from scratch; toBreakdownMap() (hence this method) may be
        // invoked more than once for the same breakdown, and we must not accumulate duplicates.
        sliceProfileResults.clear();
        // Collected during the (non-deterministically ordered) iteration below, then sorted so that
        // slice_id can be assigned in a stable order.
        final List<CapturedSlice> capturedSlices = new ArrayList<>(sliceCollectorsToLeaves.size());
        for (Map.Entry<Collector, List<LeafReaderContext>> slice : sliceCollectorsToLeaves.entrySet()) {
            final Collector sliceCollector = slice.getKey();
            // The id of the thread that searched this slice (recorded in associateCollectorToLeaves);
            // used to reconstruct the (threadId, leaf) breakdown key that context() stored under.
            final Long sliceThreadId = sliceCollectorThreads.get(sliceCollector);
            // initialize each slice level breakdown
            final Map<String, Long> currentSliceBreakdown = sliceLevelBreakdowns.computeIfAbsent(sliceCollector, k -> new HashMap<>());
            // max slice end time across all timing types
            long sliceMaxEndTime = Long.MIN_VALUE;
            long sliceMinStartTime = Long.MAX_VALUE;

            for (String timingType : timingMetrics) {
                if (timingType.equals(QueryTimingType.CREATE_WEIGHT.toString())) {
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
                    // Breakdowns are keyed by (thread, leaf); reconstruct the same key using the thread
                    // that searched this slice (recorded in associateCollectorToLeaves).
                    final Object sliceLeafKey = contextKey(sliceThreadId, sliceLeaf);
                    if (!contexts.containsKey(sliceLeafKey)) {
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
                    final Map<String, Long> currentSliceLeafBreakdownMap = contexts.get(sliceLeafKey).toBreakdownMap();
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
                    final long sliceLeafTimingTypeEndTime = sliceLeafTimingTypeStartTime + currentSliceLeafBreakdownMap.get(timingType);
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
                    timingType,
                    currentSliceBreakdown.getOrDefault(timingTypeSliceEndTimeKey, 0L) - currentSliceBreakdown.getOrDefault(
                        timingTypeSliceStartTimeKey,
                        0L
                    )
                );
            }

            for (String metric : nonTimingMetrics) {
                for (LeafReaderContext sliceLeaf : slice.getValue()) {
                    final Object sliceLeafKey = contextKey(sliceThreadId, sliceLeaf);
                    if (!contexts.containsKey(sliceLeafKey)) {
                        continue;
                    }
                    final Map<String, Long> currentSliceLeafBreakdownMap = contexts.get(sliceLeafKey).toBreakdownMap();
                    final long sliceLeafMetricValue = currentSliceLeafBreakdownMap.get(metric);
                    currentSliceBreakdown.compute(
                        metric,
                        (key, value) -> (value == null) ? sliceLeafMetricValue : value + sliceLeafMetricValue
                    );
                }
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

            // Additively capture this slice's raw breakdown (the detail behind max_/min_/avg_) along
            // with the partitions (segment ordinal + doc-id range) it searched, so consumers can
            // inspect individual slices/partitions — mirroring Lucene's per-slice/per-partition shape.
            final Map<LeafReaderContext, int[]> leafRangesForSlice = sliceLeafDocRanges.getOrDefault(
                sliceCollector,
                Collections.emptyMap()
            );
            final List<SliceProfileResult.PartitionInfo> slicePartitions = new ArrayList<>(slice.getValue().size());
            for (LeafReaderContext sliceLeaf : slice.getValue()) {
                // Fall back to the whole segment (0 .. segment maxDoc) when no explicit doc-range was
                // recorded, using the real segment size rather than a sentinel so doc_range is meaningful.
                final int[] docRange = leafRangesForSlice.getOrDefault(sliceLeaf, new int[] { 0, sliceLeaf.reader().maxDoc() });
                slicePartitions.add(new SliceProfileResult.PartitionInfo(sliceLeaf.ord, docRange[0], docRange[1]));
            }
            capturedSlices.add(new CapturedSlice(currentSliceNodeTime, slicePartitions, cleanSliceBreakdown(currentSliceBreakdown)));
        }
        // sliceCollectorsToLeaves is a ConcurrentHashMap, so the iteration order above is not
        // deterministic. Sort the captured slices by their partitions (segment ordinal, then doc-id
        // range) so that slice_id is a stable identifier for a given slice across invocations of this
        // method (toBreakdownMap may call it more than once), rather than depending on map order.
        capturedSlices.sort(CapturedSlice.BY_PARTITIONS);
        for (int sliceId = 0; sliceId < capturedSlices.size(); sliceId++) {
            final CapturedSlice captured = capturedSlices.get(sliceId);
            sliceProfileResults.add(new SliceProfileResult(sliceId, captured.nodeTime, captured.partitions, captured.breakdown));
        }
        // Guard the average against an empty map. In the normal flow toBreakdownMap() returns early
        // when sliceCollectorsToLeaves is empty, so this is defensive for direct callers/tests.
        avgSliceNodeTime = sliceCollectorsToLeaves.isEmpty() ? 0L : totalSliceNodeTime / sliceCollectorsToLeaves.size();
        return sliceLevelBreakdowns;
    }

    /**
     * A slice's captured profiling data, before a stable {@code slice_id} is assigned. Sorting these
     * by their partitions makes the assigned id independent of {@link #sliceCollectorsToLeaves}
     * iteration order.
     */
    private record CapturedSlice(long nodeTime, List<SliceProfileResult.PartitionInfo> partitions, Map<String, Long> breakdown) {
        static final Comparator<CapturedSlice> BY_PARTITIONS = (a, b) -> {
            final int n = Math.min(a.partitions.size(), b.partitions.size());
            for (int i = 0; i < n; i++) {
                final SliceProfileResult.PartitionInfo pa = a.partitions.get(i);
                final SliceProfileResult.PartitionInfo pb = b.partitions.get(i);
                int cmp = Integer.compare(pa.getSegmentOrd(), pb.getSegmentOrd());
                if (cmp != 0) {
                    return cmp;
                }
                cmp = Integer.compare(pa.getMinDocId(), pb.getMinDocId());
                if (cmp != 0) {
                    return cmp;
                }
                cmp = Integer.compare(pa.getMaxDocId(), pb.getMaxDocId());
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(a.partitions.size(), b.partitions.size());
        };
    }

    /**
     * Returns a copy of a slice breakdown containing only user-facing timing and count entries. The
     * {@code *_slice_start_time}/{@code *_slice_end_time} keys are raw {@code System.nanoTime()}
     * timestamps used internally to derive per-timing-type durations; the query-level breakdown
     * strips them before output, so the additive per-slice breakdown does the same to stay consistent
     * and avoid surfacing confusing intermediate timestamps.
     */
    private static Map<String, Long> cleanSliceBreakdown(Map<String, Long> sliceBreakdown) {
        final Map<String, Long> cleaned = new TreeMap<>();
        for (Map.Entry<String, Long> entry : sliceBreakdown.entrySet()) {
            final String key = entry.getKey();
            if (key.endsWith(SLICE_START_TIME_SUFFIX) || key.endsWith(SLICE_END_TIME_SUFFIX)) {
                continue;
            }
            cleaned.put(key, entry.getValue());
        }
        return cleaned;
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
        final Map<String, Long> queryBreakdownMap = new TreeMap<>();
        long queryEndTime = Long.MIN_VALUE;

        // the create weight time is computed at the query level and is called only once per query
        queryBreakdownMap.put(QueryTimingType.CREATE_WEIGHT + TIMING_TYPE_COUNT_SUFFIX, 1L);
        queryBreakdownMap.put(QueryTimingType.CREATE_WEIGHT.toString(), createWeightTime);

        for (String metric : timingMetrics) {

            if (metric.equals(QueryTimingType.CREATE_WEIGHT.toString())) {
                // create weight time is computed at query level and is called only once per query
                continue;
            }

            final String timingTypeCountKey = metric + TIMING_TYPE_COUNT_SUFFIX;
            final String sliceEndTimeForTimingType = metric + SLICE_END_TIME_SUFFIX;
            final String sliceStartTimeForTimingType = metric + SLICE_START_TIME_SUFFIX;

            final String maxBreakdownTypeTime = MAX_PREFIX + metric;
            final String minBreakdownTypeTime = MIN_PREFIX + metric;
            final String avgBreakdownTypeTime = AVG_PREFIX + metric;
            final String maxBreakdownTypeCount = MAX_PREFIX + timingTypeCountKey;
            final String minBreakdownTypeCount = MIN_PREFIX + timingTypeCountKey;
            final String avgBreakdownTypeCount = AVG_PREFIX + timingTypeCountKey;

            long queryTimingTypeEndTime = Long.MIN_VALUE;
            long queryTimingTypeStartTime = Long.MAX_VALUE;
            long queryTimingTypeCount = 0L;

            // for all other timing types, we will compute min/max/avg/total across slices
            for (Map.Entry<Collector, Map<String, Long>> sliceBreakdown : sliceLevelBreakdowns.entrySet()) {
                long sliceBreakdownTypeTime = sliceBreakdown.getValue().getOrDefault(metric, 0L);
                long sliceBreakdownTypeCount = sliceBreakdown.getValue().getOrDefault(timingTypeCountKey, 0L);
                // compute max/min/avg TimingType time across slices
                addStatsToMap(queryBreakdownMap, maxBreakdownTypeTime, minBreakdownTypeTime, avgBreakdownTypeTime, sliceBreakdownTypeTime);
                // compute max/min/avg TimingType count across slices
                addStatsToMap(
                    queryBreakdownMap,
                    maxBreakdownTypeCount,
                    minBreakdownTypeCount,
                    avgBreakdownTypeCount,
                    sliceBreakdownTypeCount
                );

                // only modify the start/end time of the TimingType if the slice used the timer
                if (sliceBreakdownTypeCount > 0L) {
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
            }

            if (queryTimingTypeCount > 0L && (queryTimingTypeStartTime == Long.MAX_VALUE || queryTimingTypeEndTime == Long.MIN_VALUE)) {
                throw new OpenSearchException(
                    "Unexpected timing type ["
                        + metric
                        + "] start ["
                        + queryTimingTypeStartTime
                        + "] or end time ["
                        + queryTimingTypeEndTime
                        + "] computed across slices for profile results"
                );
            }
            queryBreakdownMap.put(metric, (queryTimingTypeCount > 0L) ? queryTimingTypeEndTime - queryTimingTypeStartTime : 0L);
            queryBreakdownMap.put(timingTypeCountKey, queryTimingTypeCount);
            queryBreakdownMap.compute(avgBreakdownTypeTime, (key, value) -> (value == null) ? 0L : value / sliceLevelBreakdowns.size());
            queryBreakdownMap.compute(avgBreakdownTypeCount, (key, value) -> (value == null) ? 0L : value / sliceLevelBreakdowns.size());
            // compute query end time using max of query end time across all timing types
            queryEndTime = Math.max(queryEndTime, queryTimingTypeEndTime);
        }

        for (String metric : nonTimingMetrics) {

            final String maxBreakdownTypeTime = MAX_PREFIX + metric;
            final String minBreakdownTypeTime = MIN_PREFIX + metric;
            final String avgBreakdownTypeTime = AVG_PREFIX + metric;

            long totalBreakdownValue = 0L;

            // for all other timing types, we will compute min/max/avg/total across slices
            for (Map.Entry<Collector, Map<String, Long>> sliceBreakdown : sliceLevelBreakdowns.entrySet()) {
                long sliceBreakdownValue = sliceBreakdown.getValue().getOrDefault(metric, 0L);
                // compute max/min/avg TimingType time across slices
                addStatsToMap(queryBreakdownMap, maxBreakdownTypeTime, minBreakdownTypeTime, avgBreakdownTypeTime, sliceBreakdownValue);
                totalBreakdownValue += sliceBreakdownValue;
            }
            queryBreakdownMap.put(metric, totalBreakdownValue);
            queryBreakdownMap.compute(avgBreakdownTypeTime, (key, value) -> (value == null) ? 0L : value / sliceLevelBreakdowns.size());
        }

        if (queryEndTime == Long.MIN_VALUE) {
            throw new OpenSearchException("Unexpected error while computing the query end time across slices in profile result");
        }
        queryNodeTime = queryEndTime - createWeightStartTime;
        return queryBreakdownMap;
    }

    private void addStatsToMap(Map<String, Long> queryBreakdownMap, String maxKey, String minKey, String avgKey, long sliceValue) {
        queryBreakdownMap.compute(maxKey, (key, value) -> (value == null) ? sliceValue : Math.max(sliceValue, value));
        queryBreakdownMap.compute(minKey, (key, value) -> (value == null) ? sliceValue : Math.min(sliceValue, value));
        queryBreakdownMap.compute(avgKey, (key, value) -> (value == null) ? sliceValue : (value + sliceValue));
    }

    private Set<String> getTimingMetrics() {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, ProfileMetric> entry : metrics.entrySet()) {
            if (entry.getValue() instanceof org.opensearch.search.profile.Timer) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    private Set<String> getNonTimingMetrics() {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, ProfileMetric> entry : metrics.entrySet()) {
            if (!(entry.getValue() instanceof Timer)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    @Override
    public long toNodeTime() {
        return queryNodeTime;
    }

    @Override
    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf) {
        // Each slice (or collector) is executed by single thread. So the list for a key will always be updated by a single thread only
        sliceCollectorsToLeaves.computeIfAbsent(collector, k -> new ArrayList<>()).add(leaf);
        // Record the thread searching this slice so the reduce can reconstruct the (thread, leaf)
        // breakdown key. Called on the slice's own search thread, before that slice scores its leaves.
        sliceCollectorThreads.putIfAbsent(collector, Thread.currentThread().threadId());
    }

    @Override
    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf, int minDocId, int maxDocId) {
        associateCollectorToLeaves(collector, leaf);
        // Additively record the doc-id range this (collector=slice, leaf) was searched with, from the
        // searchLeaf seam where the bounds are in scope. Keyed by (collector, leaf) so that under
        // intra-segment search — where one segment is split across multiple slices — each slice's
        // partition of that leaf keeps its own range. Does not affect the existing reduce.
        sliceLeafDocRanges.computeIfAbsent(collector, k -> new HashMap<>()).put(leaf, new int[] { minDocId, maxDocId });
    }

    @Override
    public void associateCollectorsToLeaves(Map<Collector, List<LeafReaderContext>> collectorsToLeaves) {
        sliceCollectorsToLeaves.putAll(collectorsToLeaves);
    }

    Map<Collector, List<LeafReaderContext>> getSliceCollectorsToLeaves() {
        return Collections.unmodifiableMap(sliceCollectorsToLeaves);
    }

    /**
     * The per-(collector, leaf) doc-id ranges recorded at the searchLeaf seam. Exposed so the tree
     * can propagate them to child breakdowns (whose weights are not exposed by Lucene and therefore
     * never receive the searchLeaf association directly), mirroring how {@link
     * #getSliceCollectorsToLeaves()} is propagated.
     */
    Map<Collector, Map<LeafReaderContext, int[]>> getSliceLeafDocRanges() {
        return Collections.unmodifiableMap(sliceLeafDocRanges);
    }

    /** Copies parent doc-range associations into this (child) breakdown. */
    void associateSliceLeafDocRanges(Map<Collector, Map<LeafReaderContext, int[]>> docRanges) {
        sliceLeafDocRanges.putAll(docRanges);
    }

    /**
     * The collector→thread mapping recorded at associateCollectorToLeaves time. Exposed so the tree
     * can propagate it to child breakdowns (whose weights are not exposed by Lucene and so never
     * receive the searchLeaf association directly); without it a child's reduce cannot reconstruct the
     * (thread, leaf) key its breakdowns were stored under. Mirrors {@link #getSliceCollectorsToLeaves()}.
     */
    Map<Collector, Long> getSliceCollectorThreads() {
        return Collections.unmodifiableMap(sliceCollectorThreads);
    }

    /** Copies parent collector→threadId associations into this (child) breakdown. */
    void associateSliceCollectorThreads(Map<Collector, Long> collectorThreads) {
        sliceCollectorThreads.putAll(collectorThreads);
    }

    // used by tests
    Map<Object, AbstractProfileBreakdown> getContexts() {
        return contexts;
    }

    /**
     * Returns the additive per-slice breakdowns captured during {@link #buildSliceLevelBreakdown()}.
     * Empty until {@link #toBreakdownMap()} has run. These are the raw per-slice detail behind the
     * {@code max_/min_/avg_} aggregates.
     */
    public List<SliceProfileResult> getSliceProfileResults() {
        return Collections.unmodifiableList(sliceProfileResults);
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
