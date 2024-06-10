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
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
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
    private TotalHits totalHits;
    private final EnumMap<ShardStatsFieldNames, Integer> shardStats;

    private final SearchRequest searchRequest;
    private final LinkedBlockingQueue<TaskResourceInfo> phaseResourceUsage;
    private final Supplier<TaskResourceInfo> taskResourceUsageSupplier;

    SearchRequestContext(
        final SearchRequestOperationsListener searchRequestOperationsListener,
        final SearchRequest searchRequest,
        final Supplier<TaskResourceInfo> taskResourceUsageSupplier
    ) {
        this.searchRequestOperationsListener = searchRequestOperationsListener;
        this.absoluteStartNanos = System.nanoTime();
        this.phaseTookMap = new HashMap<>();
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
