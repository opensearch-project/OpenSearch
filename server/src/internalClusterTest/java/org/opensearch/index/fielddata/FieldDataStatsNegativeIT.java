/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Verifies fielddata byte counters do not go negative when a shard is relocated
 * off and back to the same node while a sibling shard keeps the IndexService
 * alive. Without the PR #21667 fix, stale CacheCleaner removals are routed by
 * shardId to the current shard, decrementing a counter on bytes it never cached.
 *
 * Two timing levers:
 * 1. Restore the production default cleanup interval (1m, vs the 1s set by
 *    OpenSearchIntegTestCase) so the periodic cleaner cannot fire during the
 *    relocation round-trips and prematurely drain stale entries.
 * 2. After the round-trips are complete, drive cleanup synchronously via
 *    IndicesFieldDataCache.clear() instead of waiting on the periodic timer.
 *    Avoids Thread.sleep and runs deterministically.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class FieldDataStatsNegativeIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "test_fd_stats";
    private static final String FIELD = "name";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(IndicesService.INDICES_CACHE_CLEAN_INTERVAL_SETTING.getKey(), "1m")
            .build();
    }

    public void testReproduceNegativeFieldDataStats() throws Exception {
        final String node1 = internalCluster().startNode();
        final String node2 = internalCluster().startNode();
        ensureStableCluster(2);

        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.include._name", node1)
            ).setMapping(FIELD, "type=text,fielddata=true")
        );
        ensureGreen(INDEX);

        // Multi-segment data: single-segment loadGlobal bypasses the cache.
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 5; i++) {
                client().prepareIndex(INDEX).setSource(FIELD, "b" + batch + "_d" + i).get();
            }
            client().admin().indices().prepareRefresh(INDEX).get();
            client().admin().indices().prepareFlush(INDEX).get();
        }
        populateCache();

        assertThat("expected fielddata to be cached", getFieldDataBytes(), greaterThan(0L));

        // 3 round-trips of shard 1. Shard 0 anchors the IndexService on node1.
        for (int trip = 0; trip < 3; trip++) {
            updateAllocationFilter(node1 + "," + node2);
            ensureGreen(INDEX);
            updateAllocationFilter(node1);
            ensureGreen(INDEX);
            populateCache();
        }

        // Drive cleanup synchronously instead of waiting for the periodic CacheCleaner.
        // Equivalent to one cleaner tick: refresh() iterates the cache, fires onRemoval for
        // entries whose readers have been closed — which is where the misrouted-decrement bug
        // surfaces.
        internalCluster().getInstance(IndicesService.class, node1).getIndicesFieldDataCache().getCache().refresh();

        // Assert non-negative bytes at every layer.
        // 1) Per-shard counter (ShardFieldData.totalMetric).
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats(INDEX).clear().setFieldData(true).get();
        for (ShardStats shardStats : indicesStats.getShards()) {
            long bytes = shardStats.getStats().getFieldData().getMemorySizeInBytes();
            assertThat("shard " + shardStats.getShardRouting().shardId() + " bytes: " + bytes, bytes, greaterThanOrEqualTo(0L));
        }

        // 2) Node-level stats: serialization throws on negative bytes, surfacing
        // as a per-node failure even when one shard's counter happens to net positive.
        NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().clear().setIndices(true).get();
        assertThat("node stats had failures", nodesStats.failures().size(), org.hamcrest.Matchers.equalTo(0));
        for (var node : nodesStats.getNodes()) {
            long bytes = node.getIndices().getFieldData().getMemorySizeInBytes();
            assertThat("node " + node.getNode().getName() + " bytes: " + bytes, bytes, greaterThanOrEqualTo(0L));
        }
    }

    private void populateCache() {
        client().prepareSearch(INDEX).setQuery(new MatchAllQueryBuilder()).addSort(FIELD, SortOrder.ASC).setSize(20).get();
    }

    private void updateAllocationFilter(String includeNames) {
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX)
            .setSettings(Settings.builder().put("index.routing.allocation.include._name", includeNames))
            .get();
    }

    private long getFieldDataBytes() {
        IndicesStatsResponse stats = client().admin().indices().prepareStats(INDEX).clear().setFieldData(true).get();
        return stats.getTotal().getFieldData().getMemorySizeInBytes();
    }
}
