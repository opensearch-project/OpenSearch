/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration test for stats API behavior with replica shards. Uses the DFA replication
 * pattern (remote store + segment replication) since DFA does not support plain peer
 * recovery to non-remote-store replicas.
 *
 * @opensearch.experimental
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@ThreadLeakFilters(filters = AbstractCompositeEngineIT.ParquetNativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class StatsReplicaIT extends DataFormatAwareReplicationBaseIT {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Add Netty4ModulePlugin to the parent's list so getRestClient() works for /_plugins/* endpoints.
        return Stream.concat(super.nodePlugins().stream(), Stream.of(Netty4ModulePlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            // Pin processors to a fixed value (matches AbstractCompositeEngineIT). Required to
            // avoid Netty NettyRuntime.availableProcessors static collision across nodes in a
            // multi-node IT cluster running in a single JVM.
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 1)
            .build();
    }

    /**
     * Verifies that replica shards are excluded from stats aggregation — only primary
     * shard stats are counted. Uses createDfaIndex(1) which gives 1 primary + 1 replica
     * via remote-store-backed segment replication.
     *
     * <p>The index name is {@link DataFormatAwareReplicationBaseIT#INDEX_NAME} = "dfa-replication-base-idx".
     */
    public void testReplicasExcludedFromAggregation() throws Exception {
        // Sets up cluster manager + 2 data nodes + 1 shard with 1 replica via remote store.
        createDfaIndex(1);

        // Index 100 docs (refresh policy NONE — inherited from base class indexDocs())
        indexDocs(100);

        // Refresh so the docs land in a refreshable segment and primary's tracker counts them
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Hit the parquet stats endpoint via REST
        Map<String, Object> r = StatsITHelpers.parquetIndexStats(getRestClient(), INDEX_NAME);

        // Replica copy exists but only the primary's tracker contributes to the count.
        StatsITHelpers.assertCounter(
            "primary-only count for index with 1 replica must equal 100 (NOT 200 — replicas excluded)",
            r,
            "indices." + INDEX_NAME + ".indexing.docs_indexed_total",
            100L
        );
        // _shards header reports primaries only — 1 shard, 1 successful, 0 failed.
        StatsITHelpers.assertCounter("_shards.total counts primaries only", r, "_shards.total", 1L);
        StatsITHelpers.assertCounter("_shards.successful", r, "_shards.successful", 1L);
        StatsITHelpers.assertCounter("_shards.failed", r, "_shards.failed", 0L);
    }
}
