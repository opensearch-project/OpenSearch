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

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class MovePrimaryFirstTestCase extends OpenSearchIntegTestCase {

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
        indexMoreDocs(index, 0, 10);
    }

    protected void indexMoreDocs(String index, int startDocCountId, int docCountToIndex) {
        for (int i = 0; i < docCountToIndex; i++) {
            index(index, "_doc", Integer.toString(startDocCountId), "foo", "bar" + startDocCountId);
            ++startDocCountId;
        }
        flushAndRefresh(index);
    }

    public void testClusterGreenAfterPartialRelocation() throws InterruptedException {
        internalCluster().startMasterOnlyNodes(1);
        final String z1 = "zone-1", z2 = "zone-2";
        final int primaryShardCount = 100;
        final String z1n1 = startDataOnlyNode(z1);
        ensureGreen();
        createAndIndex("foo", 1, primaryShardCount);
        ensureYellow();
        final String z1n2 = startDataOnlyNode(z1);
        ensureGreen();

        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest();
        settingsRequest.persistentSettings(
            Settings.builder().put("cluster.routing.allocation.move.primary_first", true).put("cluster.routing.allocation.exclude.zone", z2)
        );
        client().admin().cluster().updateSettings(settingsRequest).actionGet();

        final String z2n1 = startDataOnlyNode(z2);
        final String z2n2 = startDataOnlyNode(z2);

        final CountDownLatch primaryMoveLatch = new CountDownLatch(1);
        final ClusterStateListener listener = event -> {
            if (event.routingTableChanged()) {
                final RoutingNodes routingNodes = event.state().getRoutingNodes();
                int relocatedShards = 0;
                for (Iterator<RoutingNode> it = routingNodes.iterator(); it.hasNext();) {
                    RoutingNode routingNode = it.next();
                    final String nodeName = routingNode.node().getName();
                    if (nodeName.equals(z2n1) || nodeName.equals(z2n2)) {
                        relocatedShards += routingNode.numberOfShardsWithState(ShardRoutingState.STARTED);
                    }
                }
                if (relocatedShards >= primaryShardCount) {
                    primaryMoveLatch.countDown();
                }
            }
        };
        internalCluster().clusterService().addListener(listener);

        settingsRequest = new ClusterUpdateSettingsRequest();
        settingsRequest.persistentSettings(Settings.builder().put("cluster.routing.allocation.exclude.zone", z1));
        client().admin().cluster().updateSettings(settingsRequest);
        primaryMoveLatch.await();
        try {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(z1n1));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(z1n2));
        } catch (Exception e) {}
        ensureGreen();
    }
}
