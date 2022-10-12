/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission.awareness;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionAction;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.NodeRoles.onlyRole;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 2)
public class AwarenessAttributeRecommissionIT extends OpenSearchIntegTestCase {

    private final Logger logger = LogManager.getLogger(AwarenessAttributeRecommissionIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    @After
    public void cleanup() throws Exception {
        assertNoTimeout(client().admin().cluster().prepareHealth().get());
    }

    /*
     * Test Plan -
     * 1. Do a decommission.
     * 2. Verify the decommission.
     * 2. Initiate a recommission. With timeout of 30secs.
     * 3. Verify the cluster.
     */
    public void testNodesRemovedAfterZoneDecommission_ClusterManagerNotInToBeDecommissionedZone() throws Exception {
        int dataNodeCountPerAZ = 2;
        List<String> zones = new ArrayList<>(Arrays.asList("a", "b", "c"));
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
        Map<String, String> clusterManagerNameToZone = new HashMap<>();
        clusterManagerNameToZone.put(clusterManagerNodes.get(0), "a");
        clusterManagerNameToZone.put(clusterManagerNodes.get(1), "b");
        clusterManagerNameToZone.put(clusterManagerNodes.get(2), "c");

        logger.info("--> starting 4 nodes each on zones 'a' & 'b' & 'c'");
        List<String> nodes_in_zone_a = internalCluster().startDataOnlyNodes(
            dataNodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        List<String> nodes_in_zone_b = internalCluster().startDataOnlyNodes(
            dataNodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        List<String> nodes_in_zone_c = internalCluster().startDataOnlyNodes(
            dataNodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );
        ensureStableCluster(9);

        String originalClusterManager = internalCluster().getClusterManagerName();
        String originalClusterManagerZone = clusterManagerNameToZone.get(originalClusterManager);
        logger.info("--> original cluster manager - name {}, zone {}", originalClusterManager, originalClusterManagerZone);

        List<String> tempZones = new ArrayList<>(zones);
        tempZones.remove(originalClusterManagerZone);
        String zoneToDecommission = randomFrom(tempZones);

        logger.info("--> starting decommissioning nodes in zone {}", zoneToDecommission);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", zoneToDecommission);
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        DecommissionResponse decommissionResponse = client().execute(DecommissionAction.INSTANCE, decommissionRequest).get();
        assertTrue(decommissionResponse.isAcknowledged());

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        // assert that number of nodes should be 10 ( 2 cluster manager nodes + 8 data nodes )
        assertEquals(clusterState.nodes().getNodes().size(), 6);
        assertEquals(clusterState.nodes().getDataNodes().size(), 4);
        assertEquals(clusterState.nodes().getClusterManagerNodes().size(), 2);

        // recommissioning the zone, to safely succeed the test. Specific tests for recommissioning will be written separately
        DeleteDecommissionStateResponse deleteDecommissionResponse = client().execute(
            DeleteDecommissionStateAction.INSTANCE,
            new DeleteDecommissionStateRequest()
        ).get();
        assertTrue(deleteDecommissionResponse.isAcknowledged());

        ensureStableCluster(9, TimeValue.timeValueSeconds(30L)); // time should be set to findPeerInterval setting
    }
}
