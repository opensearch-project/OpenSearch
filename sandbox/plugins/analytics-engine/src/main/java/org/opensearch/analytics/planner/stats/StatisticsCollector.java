/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.stats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-query entry point for index statistics consumed by the cost model. Today a thin reader of
 * shard counts from {@link ClusterState} metadata ({@code rowCount=0}); a future
 * {@code IndicesStats}-backed cache slots in behind this signature to return real row counts.
 *
 * @opensearch.internal
 */
public final class StatisticsCollector {

    private static final Logger LOGGER = LogManager.getLogger(StatisticsCollector.class);

    private StatisticsCollector() {}

    /**
     * Returns one {@link TableStatistics} per requested index. Indices missing from cluster
     * metadata are silently dropped (logged at WARN). Returns an empty map for an empty input.
     */
    public static Map<String, TableStatistics> collect(ClusterState clusterState, Collection<String> indexNames) {
        if (indexNames.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, TableStatistics> result = new HashMap<>(indexNames.size());
        for (String indexName : indexNames) {
            IndexMetadata metadata = clusterState.metadata().index(indexName);
            if (metadata == null) {
                LOGGER.warn("Index [{}] not found in cluster metadata — skipping statistics", indexName);
                continue;
            }
            // rowCount=0: callers fall back to shards × defaultRowsPerShard via
            // TableStatistics.rowCountOrEstimate. The cache wiring (future work) replaces this
            // with a real value.
            result.put(indexName, new TableStatistics(indexName, 0L, metadata.getNumberOfShards()));
        }
        return result;
    }
}
