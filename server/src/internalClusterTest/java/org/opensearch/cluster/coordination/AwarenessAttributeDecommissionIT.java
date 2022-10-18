/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionAction;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.opensearch.test.NodeRoles.onlyRole;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AwarenessAttributeDecommissionIT extends OpenSearchIntegTestCase {
    private final Logger logger = LogManager.getLogger(AwarenessAttributeDecommissionIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    @After
    public void cleanup() throws Exception {
        assertNoTimeout(client().admin().cluster().prepareHealth().get());
    }

    public void testDecommissionStatusUpdatePublishedToAllNodes() throws ExecutionException, InterruptedException {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'a' & 'b' & 'c'");
        List<String> clusterManagerNodes = internalCluster().startNodes(
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "a")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "b")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "c")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build()
        );

        logger.info("--> start 3 data nodes on zones 'a' & 'b' & 'c'");
        List<String> dataNodes = internalCluster().startNodes(
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "a")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.DATA_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "b")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.DATA_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "c")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.DATA_ROLE))
                .build()
        );

        ensureStableCluster(6);

        logger.info("--> starting decommissioning nodes in zone {}", 'c');
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "c");
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        DecommissionResponse decommissionResponse = client().execute(DecommissionAction.INSTANCE, decommissionRequest).get();
        assertTrue(decommissionResponse.isAcknowledged());

        // Will wait for all events to complete
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        // assert that decommission status is successful
        GetDecommissionStateResponse response = client().execute(
            GetDecommissionStateAction.INSTANCE,
            new GetDecommissionStateRequest(decommissionAttribute.attributeName())
        ).get();
        assertEquals(response.getAttributeValue(), decommissionAttribute.attributeValue());
        assertEquals(response.getDecommissionStatus(), DecommissionStatus.SUCCESSFUL);

        ClusterState clusterState = client(clusterManagerNodes.get(0)).admin().cluster().prepareState().execute().actionGet().getState();
        assertEquals(4, clusterState.nodes().getSize());

        // assert status on nodes that are part of cluster currently
        Iterator<DiscoveryNode> discoveryNodeIterator = clusterState.nodes().getNodes().valuesIt();
        while (discoveryNodeIterator.hasNext()) {
            // assert no node has decommissioned attribute
            DiscoveryNode node = discoveryNodeIterator.next();
            assertNotEquals(node.getAttributes().get("zone"), "c");

            // assert all the nodes has status as SUCCESSFUL
            ClusterService localNodeClusterService = internalCluster().getInstance(ClusterService.class, node.getName());
            assertEquals(
                localNodeClusterService.state().metadata().decommissionAttributeMetadata().status(),
                DecommissionStatus.SUCCESSFUL
            );
        }

        // assert status on decommissioned node
        // Here we will verify that until it got kicked out, it received appropriate status updates
        // decommissioned nodes hence will have status as IN_PROGRESS as it will be kicked out later after this
        // and won't receive status update to SUCCESSFUL
        String randomDecommissionedNode = randomFrom(clusterManagerNodes.get(2), dataNodes.get(2));
        ClusterService decommissionedNodeClusterService = internalCluster().getInstance(ClusterService.class, randomDecommissionedNode);
        assertEquals(
            decommissionedNodeClusterService.state().metadata().decommissionAttributeMetadata().status(),
            DecommissionStatus.IN_PROGRESS
        );

        // Will wait for all events to complete
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        // Recommissioning the zone back to gracefully succeed the test once above tests succeeds
        DeleteDecommissionStateResponse deleteDecommissionStateResponse = client(clusterManagerNodes.get(0)).execute(
            DeleteDecommissionStateAction.INSTANCE,
            new DeleteDecommissionStateRequest()
        ).get();
        assertTrue(deleteDecommissionStateResponse.isAcknowledged());

        // will wait for cluster to stabilise with a timeout of 2 min (findPeerInterval for decommissioned nodes)
        // as by then all nodes should have joined the cluster
        ensureStableCluster(6, TimeValue.timeValueMinutes(2));
    }
}
