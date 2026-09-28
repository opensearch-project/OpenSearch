/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.index.warmer.WarmerStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests that verify index warming (e.g., eager global ordinals loading) works correctly
 * on NRT replica shards during segment replication. This validates the end-to-end flow where
 * index warmer gets triggered when new segments arrive on replicas via segment replication.
 *
 * <p>Uses a keyword field with {@code eager_global_ordinals: true}
 * to exercise the warming path during segment replication.</p>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationReplicaIndexWarmerIT extends SegmentReplicationBaseIT {
    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    /**
     * Verifies that eager global ordinals are loaded on both primary and replica shards
     * after segment replication, by checking warmer invocation metrics from the index stats API.
     *
     * <p>This test ensures that the {@code WarmerRefreshListener} in {@code NRTReplicationEngine}
     * correctly invokes the {@code IndexWarmer} chain on replica shards, which in turn loads
     * global ordinals for the keyword field with {@code eager_global_ordinals: true}.</p>
     */
    public void testEagerGlobalOrdinalsLoadedOnReplicaAfterSegmentReplication() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping("category", "type=keyword,eager_global_ordinals=true")
        );
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final int docCount = randomIntBetween(10, 20);
        indexTestDocuments(docCount, -1);
        final Map<String, Long> warmerTotalBeforeRefresh = getWarmerTotalPerShardType(INDEX_NAME);
        refresh(INDEX_NAME);
        waitForSearchableDocs(docCount, primaryNode, replicaNode);

        // Assert warmer invoked metrics from index stats API for both primary and replica shards
        assertBusy(() -> {
            Map<String, Long> warmerTotalsAfterRefresh = getWarmerTotalPerShardType(INDEX_NAME);
            compareWarmerTotals(warmerTotalBeforeRefresh, warmerTotalsAfterRefresh);
        });
    }

    /**
     * Verifies that warmer invocations continue on replica shards after a force merge on the primary,
     * followed by segment replication. This ensures warming is triggered on subsequent segment updates.
     */
    public void testWarmerInvokedOnReplicaAfterForceMerge() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping("category", "type=keyword,eager_global_ordinals=true")
        );
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        // Index documents in batches to create multiple segments
        final int docCount = randomIntBetween(10, 20);
        indexTestDocuments(docCount, 3);
        waitForSearchableDocs(docCount, primaryNode, replicaNode);

        // Capture warmer stats before force merge
        final Map<String, Long> warmerTotalBeforeForceMerge = getWarmerTotalPerShardType(INDEX_NAME);

        // Force merge and wait for segment replication to complete
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();

        // Verify warmer was invoked again on replica after force merge replication
        assertBusy(() -> {
            final Map<String, Long> warmerTotalAfterForceMerge = getWarmerTotalPerShardType(INDEX_NAME);
            compareWarmerTotals(warmerTotalBeforeForceMerge, warmerTotalAfterForceMerge, true);
        });
    }

    private Map<String, Long> getWarmerTotalPerShardType(String indexName) {
        IndicesStatsResponse shardStatsArray = client().admin().indices().prepareStats(indexName).clear().setWarmer(true).get();
        Map<String, Long> warmerTotals = new HashMap<>();
        warmerTotals.put("primary", 0L);
        warmerTotals.put("replica", 0L);
        for (ShardStats shardStats : shardStatsArray.getShards()) {
            WarmerStats warmerStats = shardStats.getStats().getWarmer();
            if (warmerStats != null) {
                String key = shardStats.getShardRouting().primary() ? "primary" : "replica";
                warmerTotals.merge(key, warmerStats.total(), Long::sum);
            }
        }
        return warmerTotals;
    }

    private void indexTestDocuments(int docCount, int refreshAfterDocs) {
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("category", "value-" + i).get();
            if (refreshAfterDocs != -1 && i % refreshAfterDocs == 0) {
                refresh(INDEX_NAME);
            }
        }
    }

    private void compareWarmerTotals(Map<String, Long> before, Map<String, Long> after) {
        compareWarmerTotals(before, after, false);
    }

    private void compareWarmerTotals(Map<String, Long> before, Map<String, Long> after, boolean onReplicaOnly) {
        for (String shardType : before.keySet()) {
            if (onReplicaOnly && shardType.equals("primary")) {
                continue; // Skip primary if we're only checking replica warmer totals
            }
            long beforeTotal = before.get(shardType);
            long afterTotal = after.get(shardType);
            assertThat(
                "Warmer total for " + shardType + " should increase after segment replication",
                afterTotal,
                greaterThan(beforeTotal)
            );
        }
    }
}
