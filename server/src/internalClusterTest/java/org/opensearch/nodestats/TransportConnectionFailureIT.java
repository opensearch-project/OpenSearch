/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nodestats;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportConnectionFailureStats;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 3)
public class TransportConnectionFailureIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    private Map<String, List<String>> setupCluster(int nodeCountPerAZ, Settings commonSettings) {

        logger.info("--> starting a dedicated cluster manager node");
        internalCluster().startClusterManagerOnlyNode(Settings.builder().put(commonSettings).build());

        logger.info("--> starting 1 nodes on zones 'a' & 'b' & 'c'");
        Map<String, List<String>> nodeMap = new HashMap<>();
        List<String> nodes_in_zone_a = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        nodeMap.put("a", nodes_in_zone_a);
        List<String> nodes_in_zone_b = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        nodeMap.put("b", nodes_in_zone_b);

        List<String> nodes_in_zone_c = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );
        nodeMap.put("c", nodes_in_zone_c);

        logger.info("--> waiting for nodes to form a cluster");
        int totalNodes = nodeCountPerAZ * 3 + 1;
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForNodes(Integer.toString(totalNodes))
            .execute()
            .actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();
        return nodeMap;

    }

    private void setUpIndexing(int numShards, int numReplicas) {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", numReplicas)
            )
        );
        ensureGreen();

        logger.info("--> creating indices for test");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId("" + i).setSource("field_" + i, "value_" + i).get();
        }
        refresh("test");
    }

    public void testTransportConnectionFailureStats() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(nodeMap.get("b").get(0)).collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        DiscoveryNodes dataNodes = internalCluster().clusterService().state().nodes();

        Map<String, String> nodeIDMap = new HashMap<>();
        for (DiscoveryNode node : dataNodes) {
            nodeIDMap.put(node.getName(), node.getId());
        }

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Set<String> hitNodes = new HashSet<>();
        logger.info("--> making search requests");

        Future<SearchResponse>[] responses = new Future[50];
        logger.info("--> making search requests");
        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().client(nodeMap.get("a").get(0))
                .prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        Map<String, Integer> failures = TransportConnectionFailureStats.getInstance().getConnectionFailure();

        NodesStatsResponse nodeStats = client().admin()
            .cluster()
            .prepareNodesStats()
            .addMetric("transport_connection_failure")
            .execute()
            .actionGet();
        Map<String, NodeStats> stats = nodeStats.getNodesMap();

        NodeStats nodeStatsC = stats.get(nodeIDMap.get(nodeMap.get("c").get(0)));
        Assert.assertTrue(nodeStatsC.getTransportConnectionFailureStats().getConnectionFailure().get(nodeMap.get("b").get(0)) > 0);

    }
}
