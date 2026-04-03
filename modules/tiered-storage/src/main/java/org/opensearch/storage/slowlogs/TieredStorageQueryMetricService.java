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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Query metric service for maintaining the metric collectors in single place. Singleton implementation.
 */
public class TieredStorageQueryMetricService {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TieredStorageQueryMetricService.class);

    /**
     * logger
     */
    private static final Logger logger = LogManager.getLogger(TieredStorageQueryMetricService.class);

    /**
     * Single class instance
     */
    private static final TieredStorageQueryMetricService INSTANCE = new TieredStorageQueryMetricService();

    /**
     * Map of thread ID to active collector, providing a way to retrieve the currently active metric collector on a given thread.
     * The same thread can create multiple collectors over the lifetime of a given query, both for the same query or for other queries.
     * However, only one collector will be active for a given thread at a given time, inactive metric collectors for which the query
     * is still running are tracked in taskIdToFetchPhaseCollectorMap and taskIdToQueryPhaseCollectorMap
     */
    protected final ConcurrentMap<Long, TieredStoragePerQueryMetric> metricCollectors = new ConcurrentHashMap<>();

    /**
     * Map of task id + shard id to set of collectors, providing a way to look up all collectors for a given task/shard. We need both
     * as the same parent task may have multiple shards on the same node. For concurrent segment search there will be multiple collectors
     * per task/shard combination as each slice (thread) creates its own collector. The same thread can process multiple slices for the same
     * or for different queries so it does not come into the picture here.
     */
    protected final ConcurrentMap<String, Set<TieredStoragePerQueryMetric>> taskIdToQueryPhaseCollectorMap = new ConcurrentHashMap<>();
    /** Map of task ID and shard ID to set of fetch phase metric collectors. */
    protected final ConcurrentMap<String, Set<TieredStoragePerQueryMetric>> taskIdToFetchPhaseCollectorMap = new ConcurrentHashMap<>();

    private final PrefetchStatsHolder prefetchStats = new PrefetchStatsHolder();

    /**
     * Maximum number of metric collectors for single warm node
     */
    private static final int MAX_PER_QUERY_COLLECTOR_SIZE = 1000;

    /**
     * Private constructor to keep class Singleton
     */
    private TieredStorageQueryMetricService() {}

    /**
     * Method for returning static instance of class
     * @return singleton instance for class
     */
    public static TieredStorageQueryMetricService getInstance() {
        return INSTANCE;
    }

    /**
     * Returns per query metric collector for this thread. Caller needs to ensure that the collector
     * was initialized and added earlier, otherwise dummy collector is returned which is no op
     * @param threadId threadId for which the collector requested
     * @return instance of TieredStoragePerQueryMetric
     */
    public TieredStoragePerQueryMetric getMetricCollector(final long threadId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Adds metric collector for requested thread Id which can be used later in other components using
     * getMetricCollector function. To prevent too much memory consumption, there is hard limit on the
     * number of metric collectors per OpenSearch node
     * @param threadId threadId for which collector to be added
     * @param metricCollector metric collector object for holding the metrics
     * @param isQueryPhase true if this collector is for the query phase, false for fetch phase
     */
    public void addMetricCollector(final long threadId, final TieredStoragePerQueryMetric metricCollector, boolean isQueryPhase) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Removes the metric collector added earlier using addMetricCollector function
     * @param threadId threadId for which collector needs to be removed
     * @return instance of TieredStoragePerQueryMetric removed from collector map
     */
    public TieredStoragePerQueryMetric removeMetricCollector(final long threadId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Removes all metric collectors for the given parent task and shard.
     * @param parentTaskId the parent task ID
     * @param shardId the shard ID
     * @param isQueryPhase true if removing query phase collectors, false for fetch phase
     * @return set of removed metric collectors
     */
    public Set<TieredStoragePerQueryMetric> removeMetricCollectors(String parentTaskId, String shardId, boolean isQueryPhase) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Package private for testing
     * @return taskIdToCollectorMap
     */
    Map<String, Set<TieredStoragePerQueryMetric>> getTaskIdToCollectorMap(boolean isQueryPhase) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Package private for testing
     * @return metricCollectors
     */
    Map<Long, TieredStoragePerQueryMetric> getMetricCollectors() {
        return metricCollectors;
    }

    /**
     * Returns estimated memory consumption of tiered storage query metric collector
     * This helps with easy monitoring for tracking any memory leaks
     * @return ram bytes usage for this collector instance
     */
    public long ramBytesUsed() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Records a stored fields prefetch operation result.
     * @param success true if the prefetch succeeded, false otherwise
     */
    public void recordStoredFieldsPrefetch(boolean success) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Records a doc values prefetch operation result.
     * @param success true if the prefetch succeeded, false otherwise
     */
    public void recordDocValuesPrefetch(boolean success) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /** Returns the current aggregate prefetch stats for this node. */
    // TODO has to emit as part of node stats
    public PrefetchStats getPrefetchStats() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Keeping this dummy metric collector helps keep code clean by preventing
     * unnecessary null checks
     */
    static class TieredStoragePerQueryMetricDummy implements TieredStoragePerQueryMetric {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TieredStoragePerQueryMetricDummy.class);
        private static final TieredStoragePerQueryMetricDummy INSTANCE = new TieredStoragePerQueryMetricDummy();

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

    /** Holds aggregate counters for prefetch operation statistics. */
    public static final class PrefetchStatsHolder {
        /** Constructs a new PrefetchStatsHolder. */
        PrefetchStatsHolder() {}

        final CounterMetric storedFieldsPrefetchSuccess = new CounterMetric();
        final CounterMetric storedFieldsPrefetchFailure = new CounterMetric();
        final CounterMetric docValuesPrefetchSuccess = new CounterMetric();
        final CounterMetric docValuesPrefetchFailure = new CounterMetric();

        PrefetchStats getStats() {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
