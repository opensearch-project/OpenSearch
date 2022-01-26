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
public class MovePrimaryFirstTests extends OpenSearchIntegTestCase {

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

    /**
     * Creates two nodes each in two zones and shuts down nodes in one zone
     * after relocating half the number of shards. Since, primaries are relocated
     * first, cluster should stay green as primary should have relocated
     */
    public void testClusterGreenAfterPartialRelocation() throws InterruptedException {
        internalCluster().startMasterOnlyNodes(1);
        final String z1 = "zone-1", z2 = "zone-2";
        final int primaryShardCount = 100;
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
            Settings.builder().put("cluster.routing.allocation.move.primary_first", true).put("cluster.routing.allocation.exclude.zone", z2)
        );
        client().admin().cluster().updateSettings(settingsRequest).actionGet();

        final String z2n1 = startDataOnlyNode(z2);
        final String z2n2 = startDataOnlyNode(z2);

        // Create cluster state listener to compute number of shards on new zone
        // nodes before counting down the latch
        final CountDownLatch primaryMoveLatch = new CountDownLatch(1);
        final ClusterStateListener listener = event -> {
            if (event.routingTableChanged()) {
                final RoutingNodes routingNodes = event.state().getRoutingNodes();
                int startedz2n1 = 0;
                int startedz2n2 = 0;
                for (Iterator<RoutingNode> it = routingNodes.iterator(); it.hasNext();) {
                    RoutingNode routingNode = it.next();
                    final String nodeName = routingNode.node().getName();
                    if (nodeName.equals(z2n1)) {
                        startedz2n1 = routingNode.numberOfShardsWithState(ShardRoutingState.STARTED);
                    } else if (nodeName.equals(z2n2)) {
                        startedz2n2 = routingNode.numberOfShardsWithState(ShardRoutingState.STARTED);
                    }
                }
                if (startedz2n1 >= primaryShardCount / 2 && startedz2n2 >= primaryShardCount / 2) {
                    primaryMoveLatch.countDown();
                }
            }
        };
        internalCluster().clusterService().addListener(listener);

        // Exclude zone1 nodes for allocation and await latch count down
        settingsRequest = new ClusterUpdateSettingsRequest();
        settingsRequest.persistentSettings(Settings.builder().put("cluster.routing.allocation.exclude.zone", z1));
        client().admin().cluster().updateSettings(settingsRequest);
        primaryMoveLatch.await();

        // Shutdown both nodes in zone and ensure cluster stays green
        try {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(z1n1));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(z1n2));
        } catch (Exception e) {}
        ensureGreen();
    }
}
