/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.noop.NoopSpan;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * This class holds request-level context for search queries at the coordinator node
 *
 * @opensearch.internal
 */
@InternalApi
class SearchRequestContext {
    private final SearchRequest searchRequest;
    private SearchTask searchTask;
    private final SearchRequestOperationsListener searchRequestOperationsListener;
    private long absoluteStartNanos;
    private final Map<String, Long> phaseTookMap;
    private TotalHits totalHits;
    private final EnumMap<ShardStatsFieldNames, Integer> shardStats;
    private final Tracer tracer;
    private Span requestSpan;
    private Span phaseSpan;

    /**
     * This constructor is for testing only
     */
    SearchRequestContext(SearchRequestOperationsListener searchRequestOperationsListener, SearchRequest searchRequest) {
        this(searchRequestOperationsListener, searchRequest, NoopTracer.INSTANCE);
    }

    SearchRequestContext(SearchRequestOperationsListener searchRequestOperationsListener, SearchRequest searchRequest, Tracer tracer) {
        this.searchRequestOperationsListener = searchRequestOperationsListener;
        this.searchRequest = searchRequest;
        this.tracer = tracer;
        this.absoluteStartNanos = System.nanoTime();
        this.phaseTookMap = new HashMap<>();
        this.shardStats = new EnumMap<>(ShardStatsFieldNames.class);
        this.requestSpan = NoopSpan.INSTANCE;
        this.phaseSpan = NoopSpan.INSTANCE;
    }

    SearchRequest getSearchRequest() {
        return searchRequest;
    }

    void setSearchTask(SearchTask searchTask) {
        this.searchTask = searchTask;
    }

    SearchTask getSearchTask() {
        return searchTask;
    }

    SearchRequestOperationsListener getSearchRequestOperationsListener() {
        return searchRequestOperationsListener;
    }

    void updatePhaseTookMap(String phaseName, Long tookTime) {
        this.phaseTookMap.put(phaseName, tookTime);
    }

    Map<String, Long> phaseTookMap() {
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
    long getAbsoluteStartNanos() {
        return absoluteStartNanos;
    }

    void setTotalHits(TotalHits totalHits) {
        this.totalHits = totalHits;
    }

    TotalHits totalHits() {
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

    Tracer getTracer() {
        return tracer;
    }

    void setRequestSpan(Span requestSpan) {
        this.requestSpan = requestSpan;
    }

    Span getRequestSpan() {
        return requestSpan;
    }

    void setPhaseSpan(Span phaseSpan) {
        this.phaseSpan = phaseSpan;
    }

    Span getPhaseSpan() {
        return phaseSpan;
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
