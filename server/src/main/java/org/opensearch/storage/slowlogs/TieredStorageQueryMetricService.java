/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.metrics.CounterMetric;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Singleton service for maintaining per-query metric collectors across threads.
 * Provides thread-safe access to metric collectors during query and fetch phases.
 *
 * @opensearch.experimental
 */
public class TieredStorageQueryMetricService {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TieredStorageQueryMetricService.class);

    private static final Logger logger = LogManager.getLogger(TieredStorageQueryMetricService.class);

    private static final TieredStorageQueryMetricService INSTANCE = new TieredStorageQueryMetricService();

    /**
     * Map of thread ID to active collector. Only one collector is active per thread at a time.
     */
    protected final ConcurrentMap<Long, TieredStoragePerQueryMetric> metricCollectors = new ConcurrentHashMap<>();

    /**
     * Map of task id + shard id to set of collectors for query phase.
     * Multiple threads can work on the same shard concurrently during concurrent segment search.
     */
    protected final ConcurrentMap<String, Set<TieredStoragePerQueryMetric>> taskIdToQueryPhaseCollectorMap = new ConcurrentHashMap<>();

    /**
     * Map of task id + shard id to set of collectors for fetch phase.
     */
    protected final ConcurrentMap<String, Set<TieredStoragePerQueryMetric>> taskIdToFetchPhaseCollectorMap = new ConcurrentHashMap<>();

    private final PrefetchStatsHolder prefetchStats = new PrefetchStatsHolder();

    private static final int MAX_PER_QUERY_COLLECTOR_SIZE = 1000;

    private TieredStorageQueryMetricService() {}

    /**
     * Returns the singleton instance.
     * @return the singleton instance
     */
    public static TieredStorageQueryMetricService getInstance() {
        return INSTANCE;
    }

    /**
     * Returns the per-query metric collector for the given thread.
     * Returns a no-op dummy collector if none exists.
     * @param threadId the thread id
     * @return the metric collector for the thread
     */
    public TieredStoragePerQueryMetric getMetricCollector(final long threadId) {
        return metricCollectors.getOrDefault(threadId, TieredStoragePerQueryMetricDummy.getInstance());
    }

    /**
     * Adds a metric collector for the given thread. Enforces a hard limit on the
     * number of collectors to prevent excessive memory consumption.
     * @param threadId the thread id
     * @param metricCollector the metric collector
     * @param isQueryPhase true if this is for the query phase, false for fetch phase
     */
    public void addMetricCollector(final long threadId, final TieredStoragePerQueryMetric metricCollector, boolean isQueryPhase) {
        // TODO if possible add thread id in collector
        if (metricCollectors.size() >= MAX_PER_QUERY_COLLECTOR_SIZE
            || taskIdToQueryPhaseCollectorMap.values().stream().mapToInt(Set::size).sum() >= MAX_PER_QUERY_COLLECTOR_SIZE
            || taskIdToFetchPhaseCollectorMap.values().stream().mapToInt(Set::size).sum() >= MAX_PER_QUERY_COLLECTOR_SIZE) {
            logger.error(
                "Number of metric collectors already equals maximum size of "
                    + MAX_PER_QUERY_COLLECTOR_SIZE
                    + ". Skipping. Current sizes - metricCollectors: "
                    + metricCollectors.size()
                    + ", queryPhaseCollectors: "
                    + taskIdToQueryPhaseCollectorMap.values().stream().mapToInt(Set::size).sum()
                    + ", fetchPhaseCollectors: "
                    + taskIdToFetchPhaseCollectorMap.values().stream().mapToInt(Set::size).sum()
            );
        } else {
            // The same threadId will not be used concurrently, so below is safe
            metricCollectors.put(threadId, metricCollector);
            // Multiple threads can be working on the same shard at the same time though, so below needs to be atomic
            if (isQueryPhase) {
                taskIdToQueryPhaseCollectorMap.compute(
                    metricCollector.getParentTaskId() + metricCollector.getShardId(),
                    (id, collectors) -> {
                        Set<TieredStoragePerQueryMetric> newCollectors = (collectors == null) ? new HashSet<>() : collectors;
                        newCollectors.add(metricCollector);
                        return newCollectors;
                    }
                );
            } else {
                taskIdToFetchPhaseCollectorMap.compute(
                    metricCollector.getParentTaskId() + metricCollector.getShardId(),
                    (id, collectors) -> {
                        Set<TieredStoragePerQueryMetric> newCollectors = (collectors == null) ? new HashSet<>() : collectors;
                        newCollectors.add(metricCollector);
                        return newCollectors;
                    }
                );
            }
        }
    }

    /**
     * Removes the metric collector for the given thread and records its end time.
     * @param threadId the thread id
     * @return the removed metric collector, or null if none existed
     */
    public TieredStoragePerQueryMetric removeMetricCollector(final long threadId) {
        // Do not update taskIdToCollectorMap here as the query may not be complete
        // For safety, use getOrDefault here
        metricCollectors.getOrDefault(threadId, TieredStoragePerQueryMetricDummy.getInstance()).recordEndTime();
        return metricCollectors.remove(threadId);
    }

    /**
     * Removes all metric collectors for the given task and shard combination.
     * @param parentTaskId the parent task id
     * @param shardId the shard id
     * @param isQueryPhase true for query phase collectors, false for fetch phase
     * @return the set of removed collectors
     */
    public Set<TieredStoragePerQueryMetric> removeMetricCollectors(String parentTaskId, String shardId, boolean isQueryPhase) {
        final Set<TieredStoragePerQueryMetric> collectors;
        if (isQueryPhase) {
            collectors = taskIdToQueryPhaseCollectorMap.remove(parentTaskId + shardId);
        } else {
            collectors = taskIdToFetchPhaseCollectorMap.remove(parentTaskId + shardId);
        }
        if (collectors == null) {
            // Slice Execution hooks will not be triggered in the case of a cache hit, however query phase hooks will always be triggered
            return Collections.emptySet();
        }
        return collectors;
    }

    /**
     * Returns the task-to-collector map for testing.
     * @param isQueryPhase true for query phase map, false for fetch phase
     * @return the task-to-collector map
     */
    Map<String, Set<TieredStoragePerQueryMetric>> getTaskIdToCollectorMap(boolean isQueryPhase) {
        return isQueryPhase ? taskIdToQueryPhaseCollectorMap : taskIdToFetchPhaseCollectorMap;
    }

    /**
     * Returns the metric collectors map for testing.
     * @return the metric collectors map
     */
    Map<Long, TieredStoragePerQueryMetric> getMetricCollectors() {
        return metricCollectors;
    }

    /**
     * Returns estimated memory consumption of the metric service.
     * @return ram bytes usage
     */
    public long ramBytesUsed() {
        long size = BASE_RAM_BYTES_USED;
        // While this is not completely accurate, it serves as good approximation for tracking any memory leaks
        // Each collector in metricCollectors will also be referenced in taskIdToCollectorMap, however the opposite is not true.
        // Therefore, we use taskIdToCollectorMap to estimate ram usage.
        for (Set<TieredStoragePerQueryMetric> collectors : taskIdToQueryPhaseCollectorMap.values()) {
            size += RamUsageEstimator.sizeOf(collectors.toArray(new TieredStoragePerQueryMetric[0]));
        }
        for (Set<TieredStoragePerQueryMetric> collectors : taskIdToFetchPhaseCollectorMap.values()) {
            size += RamUsageEstimator.sizeOf(collectors.toArray(new TieredStoragePerQueryMetric[0]));
        }
        return size;
    }

    /**
     * Records a stored fields prefetch event.
     * @param success true if the prefetch was successful
     */
    public void recordStoredFieldsPrefetch(boolean success) {
        if (success) {
            prefetchStats.storedFieldsPrefetchSuccess.inc();
        } else {
            prefetchStats.storedFieldsPrefetchFailure.inc();
        }
    }

    /**
     * Records a doc values prefetch event.
     * @param success true if the prefetch was successful
     */
    public void recordDocValuesPrefetch(boolean success) {
        if (success) {
            prefetchStats.docValuesPrefetchSuccess.inc();
        } else {
            prefetchStats.docValuesPrefetchFailure.inc();
        }
    }

    /**
     * Returns the current prefetch stats.
     * @return the prefetch stats
     */
    // TODO has to emit as part of node stats
    public PrefetchStats getPrefetchStats() {
        return this.prefetchStats.getStats();
    }

    /**
     * No-op dummy metric collector to avoid null checks throughout the codebase.
     *
     * @opensearch.experimental
     */
    static class TieredStoragePerQueryMetricDummy implements TieredStoragePerQueryMetric {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TieredStoragePerQueryMetricDummy.class);
        private static final TieredStoragePerQueryMetricDummy INSTANCE = new TieredStoragePerQueryMetricDummy();

        /**
         * Returns the singleton dummy instance.
         * @return the dummy instance
         */
        public static TieredStoragePerQueryMetricDummy getInstance() {
            return INSTANCE;
        }

        private TieredStoragePerQueryMetricDummy() {}

        @Override
        public void recordFileAccess(String blockFileName, boolean hit) {
            // Do nothing
        }

        @Override
        public void recordEndTime() {}

        @Override
        public void recordPrefetch(String fileName, int blockId) {}

        @Override
        public void recordReadAhead(String fileName, int blockId) {}

        @Override
        public String getParentTaskId() {
            return "DummyParentTaskId";
        }

        @Override
        public String getShardId() {
            return "DummyShardId";
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED;
        }
    }

    /**
     * Holder for prefetch statistics counters.
     *
     * @opensearch.experimental
     */
    public static final class PrefetchStatsHolder {
        /** Counter for successful stored fields prefetches. */
        final CounterMetric storedFieldsPrefetchSuccess = new CounterMetric();
        /** Counter for failed stored fields prefetches. */
        final CounterMetric storedFieldsPrefetchFailure = new CounterMetric();
        /** Counter for successful doc values prefetches. */
        final CounterMetric docValuesPrefetchSuccess = new CounterMetric();
        /** Counter for failed doc values prefetches. */
        final CounterMetric docValuesPrefetchFailure = new CounterMetric();

        /**
         * Returns the current prefetch stats snapshot.
         * @return the prefetch stats
         */
        PrefetchStats getStats() {
            return new PrefetchStats(
                storedFieldsPrefetchSuccess.count(),
                storedFieldsPrefetchFailure.count(),
                docValuesPrefetchSuccess.count(),
                docValuesPrefetchFailure.count()
            );
        }
    }
}
