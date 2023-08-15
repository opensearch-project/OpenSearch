/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ShardMovementStrategyTests extends OpenSearchIntegTestCase {

    protected String startDataOnlyNode(final String zone) {
        final Settings settings = Settings.builder().put("node.attr.zone", zone).build();
        return internalCluster().startDataOnlyNode(settings);
    }

    protected void createAndIndex(String index, int replicaCount, int shardCount) {
        assertAcked(
            prepareCreate(
                index,
                -1,
                Settings.builder()
                    .put("number_of_shards", shardCount)
                    .put("number_of_replicas", replicaCount)
                    .put("max_result_window", 20000)
            )
        );
        int startDocCountId = 0;
        for (int i = 0; i < 10; i++) {
            index(index, "_doc", Integer.toString(startDocCountId), "foo", "bar" + startDocCountId);
            ++startDocCountId;
        }
        flushAndRefresh(index);
    }

    private static Settings.Builder getSettings(ShardMovementStrategy shardMovementStrategy, boolean movePrimaryFirst) {
        return Settings.builder()
            .put("cluster.routing.allocation.shard_movement_strategy", shardMovementStrategy)
            .put("cluster.routing.allocation.move.primary_first", movePrimaryFirst);
    }

    public void testClusterGreenAfterPartialRelocationPrimaryFirstShardMovementMovePrimarySettingEnabled() throws InterruptedException {
        testClusterGreenAfterPartialRelocation(ShardMovementStrategy.PRIMARY_FIRST, true);
    }

    public void testClusterGreenAfterPartialRelocationPrimaryFirstShardMovementMovePrimarySettingDisabled() throws InterruptedException {
        testClusterGreenAfterPartialRelocation(ShardMovementStrategy.PRIMARY_FIRST, false);
    }

    public void testClusterGreenAfterPartialRelocationReplicaFirstShardMovementPrimaryFirstEnabled() throws InterruptedException {
        testClusterGreenAfterPartialRelocation(ShardMovementStrategy.REPLICA_FIRST, true);
    }

    public void testClusterGreenAfterPartialRelocationReplicaFirstShardMovementPrimaryFirstDisabled() throws InterruptedException {
        testClusterGreenAfterPartialRelocation(ShardMovementStrategy.REPLICA_FIRST, false);
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9178")
    public void testClusterGreenAfterPartialRelocationNoPreferenceShardMovementPrimaryFirstEnabled() throws InterruptedException {
        testClusterGreenAfterPartialRelocation(ShardMovementStrategy.NO_PREFERENCE, true);
    }

    private boolean shouldMovePrimaryShardsFirst(ShardMovementStrategy shardMovementStrategy, boolean movePrimaryFirst) {
        if (shardMovementStrategy == ShardMovementStrategy.NO_PREFERENCE && movePrimaryFirst) {
            return true;
        }
        return shardMovementStrategy == ShardMovementStrategy.PRIMARY_FIRST;
    }

    /**
     * Creates two nodes each in two zones and shuts down nodes in zone1 after
     * relocating half the number of shards. Shards per node constraint ensures
     * that exactly 50% of shards relocate to nodes in zone2 giving time to shut down
     * nodes in zone1. Depending on the shard movement strategy, we check whether the
     * primary or replica shards are moved first, and zone2 nodes have all the shards
     */
    private void testClusterGreenAfterPartialRelocation(ShardMovementStrategy shardMovementStrategy, boolean movePrimaryFirst)
        throws InterruptedException {
        internalCluster().startClusterManagerOnlyNodes(1);
        final String z1 = "zone-1", z2 = "zone-2";
        final int primaryShardCount = 6;
        assertTrue("Primary shard count must be even for equal distribution across two nodes", primaryShardCount % 2 == 0);
        final String z1n1 = startDataOnlyNode(z1);
        ensureGreen();
        createAndIndex("foo", 1, primaryShardCount);
        ensureYellow();
        // Start second node in same zone only after yellow cluster to ensure
        // that one gets all primaries and other all secondaries
        final String z1n2 = startDataOnlyNode(z1);
        ensureGreen();

        // Enable cluster level setting for moving primaries first and keep new
        // zone nodes excluded to prevent any shard relocation
        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        settingsRequest.persistentSettings(
            getSettings(shardMovementStrategy, movePrimaryFirst).put("cluster.routing.allocation.exclude.zone", z2)
        );
        client().admin().cluster().updateSettings(settingsRequest).actionGet();

        final String z2n1 = startDataOnlyNode(z2);
        final String z2n2 = startDataOnlyNode(z2);

        // Create cluster state listener to compute number of shards on new zone
        // nodes before counting down the latch
        final CountDownLatch shardMoveLatch = new CountDownLatch(1);
        final ClusterStateListener listener = event -> {
            if (event.routingTableChanged()) {
                final RoutingNodes routingNodes = event.state().getRoutingNodes();
                int startedCount = 0;
                for (Iterator<RoutingNode> it = routingNodes.iterator(); it.hasNext();) {
                    RoutingNode routingNode = it.next();
                    final String nodeName = routingNode.node().getName();
                    if (nodeName.equals(z2n1) || nodeName.equals(z2n2)) {
                        int count = 0;
                        for (ShardRouting shardEntry : routingNode) {
                            // If shard movement strategy is primary first, asserting that primary shards are moved first; else assert
                            // shards are replicas
                            if ((shardEntry.primary() == shouldMovePrimaryShardsFirst(shardMovementStrategy, movePrimaryFirst))
                                && shardEntry.state() == ShardRoutingState.STARTED) {
                                count++;
                            }
                        }
                        startedCount += count;
                    }
                }

                // Count down the latch once all the shards have initialized on nodes in zone-2
                if (startedCount == primaryShardCount) {
                    shardMoveLatch.countDown();
                }
            }
        };
        internalCluster().clusterService().addListener(listener);

        // Exclude zone1 nodes for allocation and await latch count down
        settingsRequest = new ClusterUpdateSettingsRequest();
        settingsRequest.persistentSettings(
            Settings.builder()
                .put("cluster.routing.allocation.exclude.zone", z1)
                // Total shards per node constraint is added to pause the relocation after shards
                // have relocated to allow time for node shutdown and validate yellow cluster
                .put("cluster.routing.allocation.total_shards_per_node", primaryShardCount / 2)
        );
        client().admin().cluster().updateSettings(settingsRequest);
        shardMoveLatch.await();

        // Shutdown both nodes in zone 1 and ensure cluster does not become red
        try {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(z1n1));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(z1n2));
        } catch (Exception e) {}
        // Due to shards per node constraint cluster cannot be green
        // Since yellow suffices for this test, not removing shards constraint
        ensureYellow();
    }
}
