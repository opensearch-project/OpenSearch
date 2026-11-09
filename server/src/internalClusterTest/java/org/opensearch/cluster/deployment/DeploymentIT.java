/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.deployment;

import org.opensearch.action.admin.cluster.deployment.GetDeploymentAction;
import org.opensearch.action.admin.cluster.deployment.GetDeploymentRequest;
import org.opensearch.action.admin.cluster.deployment.GetDeploymentResponse;
import org.opensearch.action.admin.cluster.deployment.TransitionDeploymentAction;
import org.opensearch.action.admin.cluster.deployment.TransitionDeploymentRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 6)
public class DeploymentIT extends OpenSearchIntegTestCase {
    private static final int NUM_ZONES = 3;
    private static final String INDEX_NAME = "deployment_test";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.attr.zone", "zone-" + (nodeOrdinal % NUM_ZONES + 1))
            .build();
    }

    public void testDrainAndFinish() throws Exception {
        String deploymentId = UUID.randomUUID().toString();

        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            )
        );
        ensureGreen(INDEX_NAME);

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest().clear().routingTable(true).indices(INDEX_NAME).nodes(true);
        ClusterState state = client().admin().cluster().state(clusterStateRequest).get().getState();
        RoutingTable routingTable = state.routingTable();
        DiscoveryNodes nodes = state.nodes();
        List<ShardRouting> primaryShards = routingTable.index(INDEX_NAME).shardsMatchingPredicate(ShardRouting::primary);
        String nodeWithPrimary = primaryShards.getFirst().currentNodeId();
        String zoneWithPrimary = nodes.get(nodeWithPrimary).getAttributes().get("zone");
        List<String> nodeIdsInDrainedZone = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            if (zoneWithPrimary.equals(node.getAttributes().get("zone"))) {
                nodeIdsInDrainedZone.add(node.getId());
            }
        }

        TransitionDeploymentRequest drainRequest = new TransitionDeploymentRequest().deploymentId(deploymentId)
            .targetState(DeploymentState.DRAIN)
            .attributes(Map.of("zone", zoneWithPrimary));
        assertAcked(client().execute(TransitionDeploymentAction.INSTANCE, drainRequest).get());

        // Verify the deployment is visible via GET
        GetDeploymentResponse getResponse = client().execute(GetDeploymentAction.INSTANCE, new GetDeploymentRequest(deploymentId)).get();
        assertEquals(1, getResponse.getDeployments().size());
        assertTrue(getResponse.getDeployments().containsKey(deploymentId));
        assertEquals(DeploymentState.DRAIN, getResponse.getDeployments().get(deploymentId).getState());

        assertBusy(() -> {
            ClusterState newState = client().admin().cluster().state(clusterStateRequest).get().getState();
            RoutingTable newRoutingTable = newState.routingTable();
            assertEquals(
                0,
                newRoutingTable.shardsMatchingPredicateCount(s -> s.primary() && nodeIdsInDrainedZone.contains(s.currentNodeId()))
            );
        }, 30, TimeUnit.SECONDS);

        ClusterSearchShardsRequest shardsRequest = new ClusterSearchShardsRequest().indices(INDEX_NAME);
        assertBusy(() -> {
            ClusterSearchShardsResponse drainedShardsResponse = client().admin().cluster().searchShards(shardsRequest).get();
            for (DiscoveryNode node : drainedShardsResponse.getNodes()) {
                assertNotEquals(zoneWithPrimary, node.getAttributes().get("zone"));
            }
            for (ClusterSearchShardsGroup group : drainedShardsResponse.getGroups()) {
                for (ShardRouting shard : group.getShards()) {
                    assertFalse(nodeIdsInDrainedZone.contains(shard.currentNodeId()));
                }
            }
        }, 30, TimeUnit.SECONDS);

        TransitionDeploymentRequest finishRequest = new TransitionDeploymentRequest().deploymentId(deploymentId)
            .targetState(DeploymentState.FINISH);
        assertAcked(client().execute(TransitionDeploymentAction.INSTANCE, finishRequest).get());

        assertBusy(() -> {
            ClusterSearchShardsResponse finishedShardsResponse = client().admin().cluster().searchShards(shardsRequest).get();
            Set<String> nodeZones = new HashSet<>();
            for (DiscoveryNode node : finishedShardsResponse.getNodes()) {
                nodeZones.add(node.getAttributes().get("zone"));
            }
            assertTrue(nodeZones.contains(zoneWithPrimary));
            boolean foundShardOnDrainedZone = false;
            for (ClusterSearchShardsGroup group : finishedShardsResponse.getGroups()) {
                for (ShardRouting shard : group.getShards()) {
                    if (nodeIdsInDrainedZone.contains(shard.currentNodeId())) {
                        foundShardOnDrainedZone = true;
                        break;
                    }
                }
            }
            assertTrue(foundShardOnDrainedZone);
        }, 30, TimeUnit.SECONDS);

        // Verify the deployment is no longer visible
        GetDeploymentResponse getResponseAfterFinish = client().execute(
            GetDeploymentAction.INSTANCE,
            new GetDeploymentRequest(deploymentId)
        ).get();
        assertTrue(getResponseAfterFinish.getDeployments().isEmpty());
    }
}
