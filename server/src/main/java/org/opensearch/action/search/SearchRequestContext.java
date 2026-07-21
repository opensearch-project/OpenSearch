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
    private final long absoluteStartMillis;
    private final Map<String, Long> phaseTookMap;
    private TotalHits totalHits;
    private final EnumMap<ShardStatsFieldNames, Integer> shardStats;
    private Set<Index> successfulSearchShardIndices;

    private final SearchRequest searchRequest;
    private final LinkedBlockingQueue<TaskResourceInfo> phaseResourceUsage;
    private final Supplier<TaskResourceInfo> taskResourceUsageSupplier;
    private boolean streamingRequest;

    // Latency breakdown tracking for the unaccounted gap
    private final SearchLatencyBreakdown latencyBreakdown;

    // Tracks when pipeline response transform processing started (set by listener wrapper)
    private volatile Long pipelineResponseTransformStartNanos;

    SearchRequestContext(
        final SearchRequestOperationsListener searchRequestOperationsListener,
        final SearchRequest searchRequest,
        final Supplier<TaskResourceInfo> taskResourceUsageSupplier
    ) {
        this.searchRequestOperationsListener = searchRequestOperationsListener;
        this.absoluteStartNanos = System.nanoTime();
        this.absoluteStartMillis = System.currentTimeMillis();
        this.phaseTookMap = new HashMap<>();
        this.shardStats = new EnumMap<>(ShardStatsFieldNames.class);
        this.searchRequest = searchRequest;
        this.phaseResourceUsage = new LinkedBlockingQueue<>();
        this.taskResourceUsageSupplier = taskResourceUsageSupplier;
        this.latencyBreakdown = new SearchLatencyBreakdown();
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

    SearchResponse.PhaseTook getPhaseTook() {
        if (searchRequest != null && searchRequest.isPhaseTook() != null && searchRequest.isPhaseTook()) {
            return new SearchResponse.PhaseTook(phaseTookMap);
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

    /**
     * Request start time in wall clock millis (System.currentTimeMillis).
     * Used for cross-node absolute offset computation via NTP-synced clocks.
     */
    public long getAbsoluteStartMillis() {
        return absoluteStartMillis;
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

    /**
     * Returns the latency breakdown tracker for this request.
     */
    public SearchLatencyBreakdown getLatencyBreakdown() {
        return latencyBreakdown;
    }

    /**
     * Sets the start nanos for pipeline response transform processing.
     */
    public void setPipelineResponseTransformStartNanos(long nanos) {
        this.pipelineResponseTransformStartNanos = nanos;
    }

    /**
     * Gets the start nanos for pipeline response transform processing, or null if not set.
     */
    public Long getPipelineResponseTransformStartNanos() {
        return pipelineResponseTransformStartNanos;
    }

    /**
     * Record that a phase has started. Computes inter-phase gap if a previous phase completed.
     */
    void onPhaseStartForBreakdown(String phaseName, long phaseStartNanos) {
        latencyBreakdown.markFirstPhaseStart(phaseStartNanos);
        String lastCompleted = latencyBreakdown.getLastCompletedPhaseName();
        if (lastCompleted != null) {
            long gapNanos = phaseStartNanos - latencyBreakdown.getLastPhaseEndNanos();
            if (gapNanos > 0) {
                latencyBreakdown.recordInterPhaseGap(lastCompleted, phaseName, gapNanos);
            }
        }
    }

    /**
     * Record that a phase has ended.
     */
    void onPhaseEndForBreakdown(String phaseName, long phaseEndNanos) {
        latencyBreakdown.markPhaseEnd(phaseName, phaseEndNanos);
    }

    /**
     * Returns the complete latency breakdown as a unified map of name to millis.
     * Includes phase timings (from phaseTookMap) merged with gap/overhead timings.
     * Only non-zero values are included to keep the map concise.
     */
    public Map<String, Object> getLatencyBreakdownMap() {
        long requestEndNanos = System.nanoTime();

        // Populate phase timings into the breakdown object (convert millis back to nanos for consistency)
        Map<String, Long> phaseTook = phaseTookMap();
        if (phaseTook != null) {
            Long canMatch = phaseTook.get("can_match");
            if (canMatch != null) latencyBreakdown.recordCanMatchPhase(TimeUnit.MILLISECONDS.toNanos(canMatch));
            Long dfs = phaseTook.get("dfs_pre_query");
            if (dfs != null) latencyBreakdown.recordDfsPhase(TimeUnit.MILLISECONDS.toNanos(dfs));
            Long query = phaseTook.get("query");
            if (query != null) latencyBreakdown.recordQueryPhase(TimeUnit.MILLISECONDS.toNanos(query));
            Long fetch = phaseTook.get("fetch");
            if (fetch != null) latencyBreakdown.recordFetchPhase(TimeUnit.MILLISECONDS.toNanos(fetch));
            Long expand = phaseTook.get("expand");
            if (expand != null) latencyBreakdown.recordExpandPhase(TimeUnit.MILLISECONDS.toNanos(expand));
        }

        return latencyBreakdown.toUnifiedBreakdownMap(absoluteStartNanos, requestEndNanos);
    }

    /**
     * Returns the timed breakdown map with start_offset and duration for each entry.
     * This is what the Gantt chart visualization needs for bar positioning.
     * Inner map keys: "start_offset_micros", "duration_micros".
     */
    public Map<String, Map<String, Long>> getTimedLatencyBreakdownMap() {
        return latencyBreakdown.toTimedBreakdownMap(absoluteStartNanos);
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
