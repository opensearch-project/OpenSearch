/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;

import java.util.Set;
import java.util.stream.StreamSupport;

import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;

/**
 * Tests allocation convergence in a tight-capacity scenario: 3 nodes, cluster limit of 6 shards/node
 * (18 total slots), 3 indices totaling 20 shards where one index has an index-level limit of 1 shard/node.
 * Expected result: 17 assigned shards, 3 unassigned (the constrained index's replicas).
 * <p>
 * This requires the balancer to redistribute shards optimally. LocalShardsBalancer handles this via
 * weight-based rebalancing. RemoteShardsBalancer's balance() only considers primary counts, so it
 * cannot always converge.
 */
public class ShardsBalancerTightCapacityTests extends OpenSearchAllocationTestCase {
    /**
     * LocalShardsBalancer converges to 17 assigned shards via weight-based total-shard rebalancing.
     */
    public void testTightCapacityConvergenceWithLocalShards() {
        int assignedShards = runTightCapacityScenario(false);
        assertEquals("LocalShardsBalancer should converge to 17 assigned shards", 17, assignedShards);
    }

    /**
     * RemoteShardsBalancer may settle at 16 assigned shards because its balance() only rebalances
     * by primary count, not total shard count. When this is fixed, remove the {@code @AwaitsFix}.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/19726#issuecomment-4049069484")
    public void testTightCapacityConvergenceWithRemoteShards() {
        int assignedShards = runTightCapacityScenario(true);
        assertEquals("RemoteShardsBalancer should converge to 17 assigned shards", 17, assignedShards);
    }

    /**
     * @param remote true for remote-capable nodes/indices (RemoteShardsBalancer), false for local (LocalShardsBalancer)
     * @return number of assigned shards after allocation converges
     */
    private int runTightCapacityScenario(boolean remote) {
        final String clusterLimitKey = remote
            ? ShardsLimitAllocationDecider.CLUSTER_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING.getKey()
            : ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey();
        final Settings settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
            .put(clusterLimitKey, 6)
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AllocationService strategy = createAllocationService(settings, clusterSettings, random());

        final String indexLimitKey = remote
            ? ShardsLimitAllocationDecider.INDEX_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING.getKey()
            : ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey();

        // test1: 3p+3r with index limit 1/node, test2: 4p+4r, test3: 3p+3r = 20 total, 17 assignable
        final Metadata.Builder mb = Metadata.builder();
        mb.put(IndexMetadata.builder("test1").settings(indexSettings(remote).put(indexLimitKey, 1)).numberOfShards(3).numberOfReplicas(1));
        mb.put(IndexMetadata.builder("test2").settings(indexSettings(remote)).numberOfShards(4).numberOfReplicas(1));
        mb.put(IndexMetadata.builder("test3").settings(indexSettings(remote)).numberOfShards(3).numberOfReplicas(1));
        Metadata metadata = mb.build();

        final RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .addAsNew(metadata.index("test3"))
            .build();

        final Set<DiscoveryNodeRole> roles = remote
            ? Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.WARM_ROLE)
            : Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        final DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 0; i < 3; i++) {
            String id = "node-" + i;
            nb.add(newNode(id, id, roles));
        }

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(nb)
            .build();

        clusterState = allocateToConvergence(strategy, clusterState);

        return StreamSupport.stream(clusterState.getRoutingNodes().spliterator(), false).mapToInt(RoutingNode::numberOfOwningShards).sum();
    }

    private Settings.Builder indexSettings(boolean remote) {
        final Settings.Builder sb = settings(Version.CURRENT);
        if (remote) {
            sb.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey());
        }
        return sb;
    }

    private ClusterState allocateToConvergence(AllocationService service, ClusterState clusterState) {
        clusterState = service.reroute(clusterState, "reroute");
        int iterations = 0;
        while (clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false && iterations < 10) {
            clusterState = startInitializingShardsAndReroute(service, clusterState);
            iterations++;
        }
        assertTrue("Expected no shards to be INITIALIZING", clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty());
        return clusterState;
    }
}
