/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionAction;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.coordination.JoinHelper;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.cluster.decommission.NodeDecommissionedException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.discovery.PeerFinder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

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
    /*
    Test Plan -
    1. Basic decommissioning
    2. Decommissioning when master in decommissioned zone
    3. Basic assert testing (zone, force zone not present; second request after successful etc)
    4. Failure scenarios
    5. Concurrency handling
     */

    public void testNodesRemovedAfterZoneDecommission_ClusterManagerNotInToBeDecommissionedZone() throws Exception {
        int dataNodeCountPerAZ = 4;
        List<String> zones = new ArrayList<>(Arrays.asList("a", "b", "c"));
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'a' & 'b' & 'c'");
        List<String> clusterManagerNodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)).build()
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
        ensureStableCluster(15);
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(15))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

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
        assertEquals(clusterState.nodes().getNodes().size(), 10);
        assertEquals(clusterState.nodes().getDataNodes().size(), 8);
        assertEquals(clusterState.nodes().getClusterManagerNodes().size(), 2);

        // assert that no node has decommissioned attribute
        Iterator<DiscoveryNode> discoveryNodeIterator = clusterState.nodes().getNodes().valuesIt();
        while(discoveryNodeIterator.hasNext()) {
            DiscoveryNode node = discoveryNodeIterator.next();
            assertNotEquals(node.getAttributes().get("zone"), zoneToDecommission);
        }

        // assert that decommission status is successful
        GetDecommissionStateResponse response = client().execute(GetDecommissionStateAction.INSTANCE, new GetDecommissionStateRequest()).get();
        assertEquals(response.getDecommissionedAttribute(), decommissionAttribute);
        assertEquals(response.getDecommissionStatus(), DecommissionStatus.SUCCESSFUL);

        // assert that no node present in Voting Config Exclusion
        assertEquals(clusterState.metadata().coordinationMetadata().getVotingConfigExclusions().size(),0);

        // assert that cluster manager didn't switch during test
        assertEquals(originalClusterManager,internalCluster().getClusterManagerName());

        // ------------------------------------------------------------------------------
        // tests for removed nodes
        // ------------------------------------------------------------------------------
        List<String> removedNodes;
        switch (zoneToDecommission) {
            case "a":
                removedNodes = new ArrayList<>(nodes_in_zone_a);
                break;
            case "b":
                removedNodes = new ArrayList<>(nodes_in_zone_b);
                break;
            case "c":
                removedNodes = new ArrayList<>(nodes_in_zone_c);
                break;
            default:
                throw new IllegalStateException("unrecognized zone to be decommissioned");
        }
        String randomRemovedNode = randomFrom(removedNodes);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, randomRemovedNode);
        DecommissionAttributeMetadata metadata = clusterService.state().metadata().custom(DecommissionAttributeMetadata.TYPE);

        // The decommissioned node would be having status as IN_PROGRESS as it was kicked out later
        // and not receiving any further state updates
        assertEquals(metadata.decommissionAttribute(), decommissionAttribute);
        assertEquals(metadata.status(), DecommissionStatus.IN_PROGRESS);

        // assert the node has decommissioned attribute
        assertEquals(clusterService.localNode().getAttributes().get("zone"), zoneToDecommission);

        // assert exception on decommissioned node
        Logger clusterLogger = LogManager.getLogger(JoinHelper.class);
        MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(clusterLogger);
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test",
                JoinHelper.class.getCanonicalName(),
                Level.INFO,
                "local node is decommissioned. Will not be able to join the cluster"
            )
        );
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test",
                JoinHelper.class.getCanonicalName(),
                Level.INFO,
                "failed to join"
            ) {
                @Override
                public boolean innerMatch(LogEvent event) {
                    return event.getThrown() != null
                        && event.getThrown().getClass() == RemoteTransportException.class
                        && event.getThrown().getCause() != null
                        && event.getThrown().getCause().getClass() == NodeDecommissionedException.class;
                }
            }
        );
        TransportService clusterManagerTransportService = internalCluster().getInstance(
            TransportService.class,
            originalClusterManager
        );
        MockTransportService decommissionedNodeTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            randomRemovedNode
        );
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        decommissionedNodeTransportService.addSendBehavior(
            clusterManagerTransportService,
            (connection, requestId, action, request, options) -> {
                if (action.equals(JoinHelper.JOIN_ACTION_NAME)) {
                    countDownLatch.countDown();
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        decommissionedNodeTransportService.addConnectBehavior(clusterManagerTransportService, Transport::openConnection);
        countDownLatch.await();
        mockLogAppender.assertAllExpectationsMatched();

        // test for PeerFinder
        PeerFinder peerFinder = (PeerFinder) internalCluster().getInstance(
            PeerFinder.class,
            randomRemovedNode
        );

//        // recommissioning the zone, to safely succeed the test. Specific tests for recommissioning will be written separately
//        DeleteDecommissionResponse deleteDecommissionResponse = client().execute(DeleteDecommissionAction.INSTANCE, new DeleteDecommissionRequest()).get();
//        assertTrue(deleteDecommissionResponse.isAcknowledged());
//
//        ensureStableCluster(15, TimeValue.timeValueSeconds(30L)); // time should be set to findPeerInterval setting
    }

    public void testNodesRemovedAfterZoneDecommission_ClusterManagerInToBeDecommissionedZone() throws Exception {
        int dataNodeCountPerAZ = 4;
        List<String> zones = new ArrayList<String>(Arrays.asList("a", "b", "c"));
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'a' & 'b' & 'c'");
        List<String> clusterManagerNodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)).build()
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
        ensureStableCluster(15);
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(15))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        String originalClusterManager = internalCluster().getClusterManagerName();
        String originalClusterManagerZone = clusterManagerNameToZone.get(originalClusterManager);
        logger.info("--> original cluster manager - name {}, zone {}", originalClusterManager, originalClusterManagerZone);

        logger.info("--> starting decommissioning nodes in zone {}", originalClusterManagerZone);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", originalClusterManagerZone);
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        DecommissionResponse decommissionResponse = client().execute(DecommissionAction.INSTANCE, decommissionRequest).get();
        assertTrue(decommissionResponse.isAcknowledged());

        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        // assert that number of nodes should be 10 ( 2 cluster manager nodes + 8 data nodes )
        assertEquals(clusterState.nodes().getNodes().size(), 10);
        assertEquals(clusterState.nodes().getDataNodes().size(), 8);
        assertEquals(clusterState.nodes().getClusterManagerNodes().size(), 2);

        // assert that no node has decommissioned attribute
        Iterator<DiscoveryNode> discoveryNodeIterator = clusterState.nodes().getNodes().valuesIt();
        while(discoveryNodeIterator.hasNext()) {
            DiscoveryNode node = discoveryNodeIterator.next();
            assertNotEquals(node.getAttributes().get("zone"), originalClusterManagerZone);
        }

        // assert that cluster manager is changed
        assertNotEquals(originalClusterManager, internalCluster().getClusterManagerName());

        // assert that decommission status is successful
        GetDecommissionStateResponse response = client().execute(GetDecommissionStateAction.INSTANCE, new GetDecommissionStateRequest()).get();
        assertEquals(response.getDecommissionedAttribute(), decommissionAttribute);
        assertEquals(response.getDecommissionStatus(), DecommissionStatus.SUCCESSFUL);

        // assert that no node present in Voting Config Exclusion
        assertEquals(clusterState.metadata().coordinationMetadata().getVotingConfigExclusions().size(),0);

        // ------------------------------------------------------------------------------
        // tests for removed nodes
        // ------------------------------------------------------------------------------
        List<String> removedNodes;
        switch (originalClusterManagerZone) {
            case "a":
                removedNodes = new ArrayList<>(nodes_in_zone_a);
                break;
            case "b":
                removedNodes = new ArrayList<>(nodes_in_zone_b);
                break;
            case "c":
                removedNodes = new ArrayList<>(nodes_in_zone_c);
                break;
            default:
                throw new IllegalStateException("unrecognized zone to be decommissioned");
        }
        String randomRemovedNode = randomFrom(removedNodes);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, randomRemovedNode);
        DecommissionAttributeMetadata metadata = clusterService.state().metadata().custom(DecommissionAttributeMetadata.TYPE);

        // The decommissioned node would be having status as IN_PROGRESS
        assertEquals(metadata.decommissionAttribute(), decommissionAttribute);
        assertEquals(metadata.status(), DecommissionStatus.IN_PROGRESS);

        // assert the node has decommissioned attribute
        assertEquals(clusterService.localNode().getAttributes().get("zone"), originalClusterManagerZone);

        // assert exception on decommissioned node
        Logger clusterLogger = LogManager.getLogger(JoinHelper.class);
        MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(clusterLogger);
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test",
                JoinHelper.class.getCanonicalName(),
                Level.INFO,
                "local node is decommissioned. Will not be able to join the cluster"
            )
        );
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test",
                JoinHelper.class.getCanonicalName(),
                Level.INFO,
                "failed to join"
            ) {
                @Override
                public boolean innerMatch(LogEvent event) {
                    return event.getThrown() != null
                        && event.getThrown().getClass() == RemoteTransportException.class
                        && event.getThrown().getCause() != null
                        && event.getThrown().getCause().getClass() == NodeDecommissionedException.class;
                }
            }
        );
        TransportService clusterManagerTransportService = internalCluster().getInstance(
            TransportService.class,
            internalCluster().getClusterManagerName()
        );
        MockTransportService decommissionedNodeTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            randomRemovedNode
        );
        // test for PeerFinder
        PeerFinder peerFinder = internalCluster().getInstance(
            PeerFinder.class,
            randomRemovedNode
        );
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        // TODO can check join request timings using counters
        decommissionedNodeTransportService.addSendBehavior(
            clusterManagerTransportService,
            (connection, requestId, action, request, options) -> {
                if (action.equals(JoinHelper.JOIN_ACTION_NAME)) {
                    countDownLatch.countDown();
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        decommissionedNodeTransportService.addConnectBehavior(clusterManagerTransportService, Transport::openConnection);
        countDownLatch.await();
        mockLogAppender.assertAllExpectationsMatched();

//        // recommissioning the zone, to safely succeed the test. Specific tests for recommissioning will be written separately
//        DeleteDecommissionResponse deleteDecommissionResponse = client().execute(DeleteDecommissionAction.INSTANCE, new DeleteDecommissionRequest()).get();
//        assertTrue(deleteDecommissionResponse.isAcknowledged());
//
//        ensureStableCluster(15, TimeValue.timeValueSeconds(30L)); // time should be set to findPeerInterval setting
    }
}
