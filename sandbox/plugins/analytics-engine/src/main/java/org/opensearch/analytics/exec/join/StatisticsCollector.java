/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.transport.client.Client;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-query index statistics collector.
 *
 * <p>Shard counts come from {@link ClusterState} metadata. Row counts come from the
 * {@code IndicesStats} admin API ({@code primaries.docs.count}) — primaries-only so we don't
 * double-count replicas. The IndicesStats fetch is synchronous (we're already on the SEARCH
 * executor when the advisor is consulted), and is best-effort: any failure leaves {@code rowCount}
 * at 0, which {@link JoinStrategySelector} treats as "unknown" and refuses broadcast fail-safe.
 *
 * @opensearch.internal
 */
public final class StatisticsCollector {

    private static final Logger LOGGER = LogManager.getLogger(StatisticsCollector.class);

    /**
     * Builds a {@code Map<indexName, TableStatistics>} from cluster metadata. Row counts are
     * left at zero — callers that need rows should use {@link #collect(ClusterState, Client, Collection)}.
     */
    public static Map<String, TableStatistics> collect(ClusterState clusterState, Collection<String> indexNames) {
        return collect(clusterState, null, indexNames);
    }

    /**
     * Builds a {@code Map<indexName, TableStatistics>} populating shard counts from cluster
     * metadata and primary-shard doc counts from {@code IndicesStats} when {@code client} is
     * non-null. IndicesStats failures are logged but never fail the caller — the resulting
     * {@code rowCount=0} is the fail-safe signal for {@link JoinStrategySelector}.
     */
    public static Map<String, TableStatistics> collect(ClusterState clusterState, Client client, Collection<String> indexNames) {
        Map<String, TableStatistics> result = new HashMap<>();

        // Pre-populate shard counts from cluster metadata. This is always available and never
        // blocks. Row counts default to 0 here and may be upgraded by the IndicesStats call below.
        Map<String, Integer> shardCounts = new HashMap<>();
        for (String indexName : indexNames) {
            IndexMetadata metadata = clusterState.metadata().index(indexName);
            if (metadata == null) {
                LOGGER.warn("Index [{}] not found in cluster metadata — skipping statistics", indexName);
                continue;
            }
            shardCounts.put(indexName, metadata.getNumberOfShards());
        }

        Map<String, Long> rowCounts = client == null ? Map.of() : fetchPrimaryDocCounts(client, shardCounts.keySet());

        for (Map.Entry<String, Integer> entry : shardCounts.entrySet()) {
            String indexName = entry.getKey();
            int shardCount = entry.getValue();
            long rowCount = rowCounts.getOrDefault(indexName, 0L);
            result.put(indexName, new TableStatistics(indexName, rowCount, shardCount));
        }
        return result;
    }

    /**
     * Issues a single {@code IndicesStats} request scoped to {@code indexNames} and {@code docs}
     * stats only. Returns a map of {@code index → primaries.docs.count}; any index missing from
     * the response (or any failure to call IndicesStats) leaves the entry absent so the caller
     * keeps the fail-safe zero.
     */
    private static Map<String, Long> fetchPrimaryDocCounts(Client client, Collection<String> indexNames) {
        if (indexNames.isEmpty()) {
            return Map.of();
        }
        try {
            IndicesStatsRequest request = new IndicesStatsRequest();
            request.indices(indexNames.toArray(new String[0]));
            request.clear();
            request.docs(true);
            IndicesStatsResponse response = client.admin().indices().stats(request).actionGet();
            Map<String, Long> rowCounts = new HashMap<>();
            for (Map.Entry<String, IndexStats> entry : response.getIndices().entrySet()) {
                CommonStats primaries = entry.getValue().getPrimaries();
                DocsStats docs = primaries == null ? null : primaries.getDocs();
                if (docs != null) {
                    rowCounts.put(entry.getKey(), docs.getCount());
                }
            }
            return rowCounts;
        } catch (Exception e) {
            LOGGER.warn("IndicesStats fetch failed for {}; row counts will be unknown — broadcast will fall back fail-safe", indexNames, e);
            return Map.of();
        }
    }
}
