/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.index.Index;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This class holds request-level context for search queries at the coordinator node
 *
 * @opensearch.internal
 */
@InternalApi
public class SearchRequestContext {
    private static final Logger logger = LogManager.getLogger();
    private final SearchRequestOperationsListener searchRequestOperationsListener;
    private long absoluteStartNanos;
    private final Map<String, Long> phaseTookMap;
    private final Map<String, Long> phaseStartOffsetMicrosMap;
    private final Map<String, Long> phaseDurationMicrosMap;
    // Coordinator event name -> [startOffsetMicros, durationMicros]. Written on the coordinator flow including
    // the async rewrite callback, hence ConcurrentHashMap; each key is written once.
    private final Map<String, long[]> coordinatorEventMap;
    // nanoTime when query rewrite began, published across the async rewrite callback. 0 = not started.
    private volatile long rewriteStartNanos;
    private TotalHits totalHits;
    private final EnumMap<ShardStatsFieldNames, Integer> shardStats;
    private Set<Index> successfulSearchShardIndices;

    private final SearchRequest searchRequest;
    private final LinkedBlockingQueue<TaskResourceInfo> phaseResourceUsage;
    private final Supplier<TaskResourceInfo> taskResourceUsageSupplier;
    private boolean streamingRequest;

    SearchRequestContext(
        final SearchRequestOperationsListener searchRequestOperationsListener,
        final SearchRequest searchRequest,
        final Supplier<TaskResourceInfo> taskResourceUsageSupplier
    ) {
        this.searchRequestOperationsListener = searchRequestOperationsListener;
        this.absoluteStartNanos = System.nanoTime();
        this.phaseTookMap = new HashMap<>();
        this.phaseStartOffsetMicrosMap = new HashMap<>();
        this.phaseDurationMicrosMap = new HashMap<>();
        this.coordinatorEventMap = new ConcurrentHashMap<>();
        this.shardStats = new EnumMap<>(ShardStatsFieldNames.class);
        this.searchRequest = searchRequest;
        this.phaseResourceUsage = new LinkedBlockingQueue<>();
        this.taskResourceUsageSupplier = taskResourceUsageSupplier;
    }

    SearchRequestOperationsListener getSearchRequestOperationsListener() {
        return searchRequestOperationsListener;
    }

    void updatePhaseTookMap(String phaseName, Long tookTime) {
        this.phaseTookMap.put(phaseName, tookTime);
    }

    public Map<String, Long> phaseTookMap() {
        return phaseTookMap;
    }

    /** Records a phase start offset in microseconds, relative to {@link #getAbsoluteStartNanos()}. */
    void updatePhaseStartOffsetMap(String phaseName, Long startOffsetMicros) {
        this.phaseStartOffsetMicrosMap.put(phaseName, startOffsetMicros);
    }

    public Map<String, Long> phaseStartOffsetMicrosMap() {
        return phaseStartOffsetMicrosMap;
    }

    /** Records a phase duration in microseconds (separate from the millisecond {@link #phaseTookMap()}). */
    void updatePhaseDurationMicrosMap(String phaseName, Long durationMicros) {
        this.phaseDurationMicrosMap.put(phaseName, durationMicros);
    }

    public Map<String, Long> phaseDurationMicrosMap() {
        return phaseDurationMicrosMap;
    }

    /**
     * Records a coordinator event with absolute {@code System.nanoTime()} bounds, stored as a start offset
     * (relative to {@link #getAbsoluteStartNanos()}) and duration in micros. Duration is clamped as its bounds
     * can be stamped on different threads (e.g. the async rewrite callback), where cross-core nanoTime skew is possible.
     */
    void recordCoordinatorEvent(String eventName, long startNanos, long endNanos) {
        long startOffsetNanos = Math.max(0, startNanos - absoluteStartNanos);
        long durationNanos = Math.max(0, endNanos - startNanos);
        coordinatorEventMap.put(
            eventName,
            new long[] { TimeUnit.NANOSECONDS.toMicros(startOffsetNanos), TimeUnit.NANOSECONDS.toMicros(durationNanos) }
        );
    }

    public Map<String, long[]> coordinatorEventMap() {
        return coordinatorEventMap;
    }

    void setRewriteStartNanos(long rewriteStartNanos) {
        this.rewriteStartNanos = rewriteStartNanos;
    }

    long getRewriteStartNanos() {
        return rewriteStartNanos;
    }

    SearchResponse.PhaseTook getPhaseTook() {
        if (searchRequest != null && searchRequest.isPhaseTook() != null && searchRequest.isPhaseTook()) {
            return new SearchResponse.PhaseTook(phaseTookMap);
        } else {
            return null;
        }
    }

    /**
     * Builds the per-phase latency breakdown for the search response, or {@code null} when phase timing
     * output is disabled. Gated by the same {@code phase_took} flag as {@link #getPhaseTook()} so the
     * breakdown is only surfaced when the caller opted in; the underlying maps are always populated.
     */
    SearchResponse.SearchLatencyBreakdown getLatencyBreakdown() {
        if (searchRequest != null && searchRequest.isPhaseTook() != null && searchRequest.isPhaseTook()) {
            return new SearchResponse.SearchLatencyBreakdown(phaseStartOffsetMicrosMap, phaseDurationMicrosMap, coordinatorEventMap);
        } else {
            return null;
        }
    }

    /**
     * Override absoluteStartNanos set in constructor.
     * For testing only
     */
    void setAbsoluteStartNanos(long absoluteStartNanos) {
        this.absoluteStartNanos = absoluteStartNanos;
    }

    /**
     * Request start time in nanos
     */
    public long getAbsoluteStartNanos() {
        return absoluteStartNanos;
    }

    void setTotalHits(TotalHits totalHits) {
        this.totalHits = totalHits;
    }

    public TotalHits totalHits() {
        return totalHits;
    }

    void setShardStats(int total, int successful, int skipped, int failed) {
        this.shardStats.put(ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_TOTAL, total);
        this.shardStats.put(ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_SUCCESSFUL, successful);
        this.shardStats.put(ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_SKIPPED, skipped);
        this.shardStats.put(ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_FAILED, failed);
    }

    String formattedShardStats() {
        if (shardStats.isEmpty()) {
            return "";
        } else {
            return String.format(
                Locale.ROOT,
                "{%s:%s, %s:%s, %s:%s, %s:%s}",
                ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_TOTAL.toString(),
                shardStats.get(ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_TOTAL),
                ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_SUCCESSFUL.toString(),
                shardStats.get(ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_SUCCESSFUL),
                ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_SKIPPED.toString(),
                shardStats.get(ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_SKIPPED),
                ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_FAILED.toString(),
                shardStats.get(ShardStatsFieldNames.SEARCH_REQUEST_SLOWLOG_SHARD_FAILED)
            );
        }
    }

    public Supplier<TaskResourceInfo> getTaskResourceUsageSupplier() {
        return taskResourceUsageSupplier;
    }

    public void recordPhaseResourceUsage(TaskResourceInfo usage) {
        if (usage != null) {
            this.phaseResourceUsage.add(usage);
        }
    }

    public List<TaskResourceInfo> getPhaseResourceUsage() {
        return new ArrayList<>(phaseResourceUsage);
    }

    public SearchRequest getRequest() {
        return searchRequest;
    }

    void setSuccessfulSearchShardIndices(Set<Index> successfulSearchShardIndices) {
        this.successfulSearchShardIndices = successfulSearchShardIndices;
    }

    /**
     * @return A {@link Set} of {@link Index} representing the names of the indices that were
     * successfully queried at the shard level.
     */
    public Set<Index> getSuccessfulSearchShardIndices() {
        return successfulSearchShardIndices;
    }

    void setStreamingRequest(boolean streamingRequest) {
        this.streamingRequest = streamingRequest;
    }

    public boolean isStreamingRequest() {
        return streamingRequest;
    }
}

enum ShardStatsFieldNames {
    SEARCH_REQUEST_SLOWLOG_SHARD_TOTAL("total"),
    SEARCH_REQUEST_SLOWLOG_SHARD_SUCCESSFUL("successful"),
    SEARCH_REQUEST_SLOWLOG_SHARD_SKIPPED("skipped"),
    SEARCH_REQUEST_SLOWLOG_SHARD_FAILED("failed");

    private final String name;

    ShardStatsFieldNames(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
