/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateAction;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionAction;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.cluster.decommission.DecommissioningFailedException;
import org.opensearch.cluster.decommission.NodeDecommissionedException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.discovery.Discovery;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

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

    public void testDecommissionFailedWhenNotZoneAware() throws Exception {
        Settings commonSettings = Settings.builder().build();
        // Start 3 cluster manager eligible nodes
        internalCluster().startClusterManagerOnlyNodes(3, Settings.builder().put(commonSettings).build());
        // start 3 data nodes
        internalCluster().startDataOnlyNodes(3, Settings.builder().put(commonSettings).build());
        ensureStableCluster(6);
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(6))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-1");
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        assertBusy(() -> {
            DecommissioningFailedException ex = expectThrows(
                DecommissioningFailedException.class,
                () -> client().execute(DecommissionAction.INSTANCE, decommissionRequest).actionGet()
            );
            assertTrue(ex.getMessage().contains("invalid awareness attribute requested for decommissioning"));
        });
    }

    public void testDecommissionFailedWhenNotForceZoneAware() throws Exception {
        Settings commonSettings = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone").build();
        // Start 3 cluster manager eligible nodes
        logger.info("--> start 3 cluster manager nodes on zones 'a' & 'b' & 'c'");
        internalCluster().startNodes(
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
        logger.info("--> starting data node each on zones 'a' & 'b' & 'c'");
        internalCluster().startDataOnlyNode(Settings.builder().put(commonSettings).put("node.attr.zone", "a").build());
        internalCluster().startDataOnlyNode(Settings.builder().put(commonSettings).put("node.attr.zone", "b").build());
        internalCluster().startDataOnlyNode(Settings.builder().put(commonSettings).put("node.attr.zone", "c").build());
        ensureStableCluster(6);
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(6))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "a");
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        assertBusy(() -> {
            DecommissioningFailedException ex = expectThrows(
                DecommissioningFailedException.class,
                () -> client().execute(DecommissionAction.INSTANCE, decommissionRequest).actionGet()
            );
            assertTrue(ex.getMessage().contains("doesn't have the decommissioning attribute"));
        });
    }

    public void testNodesRemovedAfterZoneDecommission_ClusterManagerNotInToBeDecommissionedZone() throws Exception {
        assertNodesRemovedAfterZoneDecommission(false);
    }

    public void testNodesRemovedAfterZoneDecommission_ClusterManagerInToBeDecommissionedZone() throws Exception {
        assertNodesRemovedAfterZoneDecommission(true);
    }

    public void testInvariantsAndLogsOnDecommissionedNodes() throws Exception {
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
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(6))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 0.0, "b", 1.0, "c", 1.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        logger.info("--> starting decommissioning nodes in zone {}", 'a');
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "a");
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        decommissionRequest.setNoDelay(true);
        DecommissionResponse decommissionResponse = client().execute(DecommissionAction.INSTANCE, decommissionRequest).get();
        assertTrue(decommissionResponse.isAcknowledged());

        // Will wait for all events to complete
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        String decommissionedNode = randomFrom(clusterManagerNodes.get(0), dataNodes.get(0));
        String activeNode = dataNodes.get(1);

        ClusterService decommissionedNodeClusterService = internalCluster().getInstance(ClusterService.class, decommissionedNode);
        DecommissionAttributeMetadata metadata = decommissionedNodeClusterService.state()
            .metadata()
            .custom(DecommissionAttributeMetadata.TYPE);
        // The decommissioned node would not be having status as SUCCESS as it was kicked out later
        // and not receiving any further state updates
        // This also helps to test metadata status updates was received by this node until it got kicked by the leader
        assertEquals(metadata.decommissionAttribute(), decommissionAttribute);
        assertNotNull(metadata.status());
        assertEquals(metadata.status(), DecommissionStatus.IN_PROGRESS);

        // assert the node has decommissioned attribute
        assertEquals(decommissionedNodeClusterService.localNode().getAttributes().get("zone"), "a");

        // assert exception on decommissioned node
        Logger clusterLogger = LogManager.getLogger(JoinHelper.class);
        MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(clusterLogger);
        mockLogAppender.addExpectation(
            new MockLogAppender.PatternSeenEventExpectation(
                "test",
                JoinHelper.class.getCanonicalName(),
                Level.INFO,
                "local node is decommissioned \\[.*]\\. Will not be able to join the cluster"
            )
        );
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation("test", JoinHelper.class.getCanonicalName(), Level.INFO, "failed to join") {
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
            internalCluster().getClusterManagerName(activeNode)
        );
        MockTransportService decommissionedNodeTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            decommissionedNode
        );
        final CountDownLatch countDownLatch = new CountDownLatch(2);
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

        // decommissioned node should have Coordinator#localNodeCommissioned = false
        Coordinator coordinator = (Coordinator) internalCluster().getInstance(Discovery.class, decommissionedNode);
        assertFalse(coordinator.localNodeCommissioned());

        // Check cluster health API for decommissioned and active node
        ClusterHealthResponse activeNodeLocalHealth = client(activeNode).admin()
            .cluster()
            .prepareHealth()
            .setLocal(true)
            .setEnsureNodeWeighedIn(true)
            .execute()
            .actionGet();
        assertFalse(activeNodeLocalHealth.isTimedOut());

        ClusterHealthResponse decommissionedNodeLocalHealth = client(decommissionedNode).admin()
            .cluster()
            .prepareHealth()
            .setLocal(true)
            .execute()
            .actionGet();
        assertFalse(decommissionedNodeLocalHealth.isTimedOut());

        NodeDecommissionedException ex = expectThrows(
            NodeDecommissionedException.class,
            () -> client(decommissionedNode).admin()
                .cluster()
                .prepareHealth()
                .setLocal(true)
                .setEnsureNodeWeighedIn(true)
                .execute()
                .actionGet()
        );
        assertTrue(ex.getMessage().contains("local node is decommissioned"));

        // Recommissioning the zone back to gracefully succeed the test once above tests succeeds
        DeleteDecommissionStateResponse deleteDecommissionStateResponse = client(activeNode).execute(
            DeleteDecommissionStateAction.INSTANCE,
            new DeleteDecommissionStateRequest()
        ).get();
        assertTrue(deleteDecommissionStateResponse.isAcknowledged());

        ClusterService activeNodeClusterService = internalCluster().getInstance(ClusterService.class, activeNode);
        ClusterStateObserver clusterStateObserver = new ClusterStateObserver(
            activeNodeClusterService,
            null,
            logger,
            client(activeNode).threadPool().getThreadContext()
        );
        CountDownLatch expectedStateLatch = new CountDownLatch(1);
        Predicate<ClusterState> expectedClusterStatePredicate = clusterState -> {
            if (clusterState.metadata().decommissionAttributeMetadata() != null) return false;
            if (clusterState.metadata().coordinationMetadata().getVotingConfigExclusions().isEmpty() == false) return false;
            if (clusterState.nodes().getNodes().size() != 6) return false;
            return clusterState.metadata().coordinationMetadata().getLastCommittedConfiguration().getNodeIds().size() == 3;
        };

        ClusterState currentState = activeNodeClusterService.state();
        if (expectedClusterStatePredicate.test(currentState)) {
            logger.info("cluster restored");
            expectedStateLatch.countDown();
        } else {
            clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    logger.info("cluster restored");
                    expectedStateLatch.countDown();
                }

                @Override
                public void onClusterServiceClose() {
                    throw new AssertionError("unexpected close");
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    throw new AssertionError("unexpected timeout");
                }
            }, expectedClusterStatePredicate);
        }
        // if the below condition is passed, then we are sure that config size is restored
        assertTrue(expectedStateLatch.await(180, TimeUnit.SECONDS));
        // will wait for cluster to stabilise with a timeout of 2 min as by then all nodes should have joined the cluster
        ensureStableCluster(6);
    }

    private void assertNodesRemovedAfterZoneDecommission(boolean originalClusterManagerDecommission) throws Exception {
        int dataNodeCountPerAZ = 4;
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

        logger.info("--> starting 4 data nodes each on zones 'a' & 'b' & 'c'");
        Map<String, List<String>> zoneToNodesMap = new HashMap<>();
        zoneToNodesMap.put(
            "a",
            internalCluster().startDataOnlyNodes(
                dataNodeCountPerAZ,
                Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
            )
        );
        zoneToNodesMap.put(
            "b",
            internalCluster().startDataOnlyNodes(
                dataNodeCountPerAZ,
                Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
            )
        );
        zoneToNodesMap.put(
            "c",
            internalCluster().startDataOnlyNodes(
                dataNodeCountPerAZ,
                Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
            )
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

        String zoneToDecommission = originalClusterManagerZone;

        if (originalClusterManagerDecommission == false) {
            // decommission one zone where active cluster manager is not present
            List<String> tempZones = new ArrayList<>(zones);
            tempZones.remove(originalClusterManagerZone);
            zoneToDecommission = randomFrom(tempZones);
        }
        String activeNode;
        switch (zoneToDecommission) {
            case "a":
                activeNode = randomFrom(randomFrom(zoneToNodesMap.get("b")), randomFrom(zoneToNodesMap.get("c")));
                break;
            case "b":
                activeNode = randomFrom(randomFrom(zoneToNodesMap.get("a")), randomFrom(zoneToNodesMap.get("c")));
                break;
            case "c":
                activeNode = randomFrom(randomFrom(zoneToNodesMap.get("a")), randomFrom(zoneToNodesMap.get("b")));
                break;
            default:
                throw new IllegalStateException("unexpected zone decommissioned");
        }

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = new HashMap<>(Map.of("a", 1.0, "b", 1.0, "c", 1.0));
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        weights.put(zoneToDecommission, 0.0);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        logger.info("--> starting decommissioning nodes in zone {}", zoneToDecommission);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", zoneToDecommission);
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        decommissionRequest.setNoDelay(true);
        DecommissionResponse decommissionResponse = client().execute(DecommissionAction.INSTANCE, decommissionRequest).get();
        assertTrue(decommissionResponse.isAcknowledged());

        client(activeNode).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        ClusterState clusterState = client(activeNode).admin().cluster().prepareState().execute().actionGet().getState();

        // assert that number of nodes should be 10 ( 2 cluster manager nodes + 8 data nodes )
        assertEquals(clusterState.nodes().getNodes().size(), 10);
        assertEquals(clusterState.nodes().getDataNodes().size(), 8);
        assertEquals(clusterState.nodes().getClusterManagerNodes().size(), 2);

        Iterator<DiscoveryNode> discoveryNodeIterator = clusterState.nodes().getNodes().values().iterator();
        while (discoveryNodeIterator.hasNext()) {
            // assert no node has decommissioned attribute
            DiscoveryNode node = discoveryNodeIterator.next();
            assertNotEquals(node.getAttributes().get("zone"), zoneToDecommission);

            // assert no node is decommissioned from Coordinator#localNodeCommissioned
            Coordinator coordinator = (Coordinator) internalCluster().getInstance(Discovery.class, node.getName());
            assertTrue(coordinator.localNodeCommissioned());
        }

        // assert that decommission status is successful
        GetDecommissionStateResponse response = client(activeNode).execute(
            GetDecommissionStateAction.INSTANCE,
            new GetDecommissionStateRequest(decommissionAttribute.attributeName())
        ).get();
        assertEquals(response.getAttributeValue(), decommissionAttribute.attributeValue());
        assertEquals(response.getDecommissionStatus(), DecommissionStatus.SUCCESSFUL);

        // assert that no node present in Voting Config Exclusion
        assertEquals(clusterState.metadata().coordinationMetadata().getVotingConfigExclusions().size(), 0);

        String currentClusterManager = internalCluster().getClusterManagerName(activeNode);
        assertNotNull(currentClusterManager);
        if (originalClusterManagerDecommission) {
            // assert that cluster manager switched during the test
            assertNotEquals(originalClusterManager, currentClusterManager);
        } else {
            // assert that cluster manager didn't switch during test
            assertEquals(originalClusterManager, currentClusterManager);
        }

        // Will wait for all events to complete
        client(activeNode).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        // Recommissioning the zone back to gracefully succeed the test once above tests succeeds
        DeleteDecommissionStateResponse deleteDecommissionStateResponse = client(currentClusterManager).execute(
            DeleteDecommissionStateAction.INSTANCE,
            new DeleteDecommissionStateRequest()
        ).get();
        assertTrue(deleteDecommissionStateResponse.isAcknowledged());

        // will wait for cluster to stabilise with a timeout of 2 min as by then all nodes should have joined the cluster
        ensureStableCluster(15, TimeValue.timeValueMinutes(2));
    }

    public void testDecommissionFailedWhenDifferentAttributeAlreadyDecommissioned() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'a' & 'b' & 'c'");
        internalCluster().startNodes(
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
        logger.info("--> starting 1 nodes each on zones 'a' & 'b' & 'c'");
        internalCluster().startDataOnlyNode(Settings.builder().put(commonSettings).put("node.attr.zone", "a").build());
        internalCluster().startDataOnlyNode(Settings.builder().put(commonSettings).put("node.attr.zone", "b").build());
        String node_in_c = internalCluster().startDataOnlyNode(Settings.builder().put(commonSettings).put("node.attr.zone", "c").build());
        ensureStableCluster(6);
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(6))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 0.0, "b", 1.0, "c", 1.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        logger.info("--> starting decommissioning nodes in zone {}", 'a');
        DecommissionRequest decommissionRequest = new DecommissionRequest(new DecommissionAttribute("zone", "a"));
        DecommissionResponse decommissionResponse = client().execute(DecommissionAction.INSTANCE, decommissionRequest).get();
        assertTrue(decommissionResponse.isAcknowledged());

        DecommissionRequest newDecommissionRequest = new DecommissionRequest(new DecommissionAttribute("zone", "b"));
        assertBusy(
            () -> expectThrows(
                DecommissioningFailedException.class,
                () -> client(node_in_c).execute(DecommissionAction.INSTANCE, newDecommissionRequest).actionGet()
            )
        );

        // Will wait for all events to complete
        client(node_in_c).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        // Recommissioning the zone back to gracefully succeed the test once above tests succeeds
        DeleteDecommissionStateResponse deleteDecommissionStateResponse = client(node_in_c).execute(
            DeleteDecommissionStateAction.INSTANCE,
            new DeleteDecommissionStateRequest()
        ).get();
        assertTrue(deleteDecommissionStateResponse.isAcknowledged());

        // will wait for cluster to stabilise with a timeout of 2 min as by then all nodes should have joined the cluster
        ensureStableCluster(6, TimeValue.timeValueMinutes(2));
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

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        logger.info("--> starting decommissioning nodes in zone {}", 'c');
        String activeNode = randomFrom(dataNodes.get(0), dataNodes.get(1));
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "c");
        // Set the timeout to 0 to do immediate Decommission
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        decommissionRequest.setNoDelay(true);
        DecommissionResponse decommissionResponse = client(activeNode).execute(DecommissionAction.INSTANCE, decommissionRequest).get();
        assertTrue(decommissionResponse.isAcknowledged());

        // Will wait for all events to complete
        client(activeNode).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        logger.info("--> Received LANGUID event");

        // assert that decommission status is successful
        GetDecommissionStateResponse response = client(activeNode).execute(
            GetDecommissionStateAction.INSTANCE,
            new GetDecommissionStateRequest(decommissionAttribute.attributeName())
        ).get();
        assertEquals(response.getAttributeValue(), decommissionAttribute.attributeValue());
        assertEquals(DecommissionStatus.SUCCESSFUL, response.getDecommissionStatus());

        logger.info("--> Decommission status is successful");
        ClusterState clusterState = client(activeNode).admin().cluster().prepareState().execute().actionGet().getState();
        assertEquals(4, clusterState.nodes().getSize());

        logger.info("--> Got cluster state with 4 nodes.");
        // assert status on nodes that are part of cluster currently
        Iterator<DiscoveryNode> discoveryNodeIterator = clusterState.nodes().getNodes().values().iterator();
        DiscoveryNode clusterManagerNodeAfterDecommission = null;
        while (discoveryNodeIterator.hasNext()) {
            // assert no node has decommissioned attribute
            DiscoveryNode node = discoveryNodeIterator.next();
            assertNotEquals(node.getAttributes().get("zone"), "c");
            if (node.isClusterManagerNode()) {
                clusterManagerNodeAfterDecommission = node;
            }
            // assert all the nodes has status as SUCCESSFUL
            ClusterService localNodeClusterService = internalCluster().getInstance(ClusterService.class, node.getName());
            assertEquals(
                localNodeClusterService.state().metadata().decommissionAttributeMetadata().status(),
                DecommissionStatus.SUCCESSFUL
            );
        }
        assertNotNull("Cluster Manager not found after decommission", clusterManagerNodeAfterDecommission);
        logger.info("--> Cluster Manager node found after decommission");

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
        logger.info("--> Verified the decommissioned node has in_progress state.");

        // Will wait for all events to complete
        client(activeNode).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();
        logger.info("--> Got LANGUID event");
        // Recommissioning the zone back to gracefully succeed the test once above tests succeeds
        DeleteDecommissionStateResponse deleteDecommissionStateResponse = client(activeNode).execute(
            DeleteDecommissionStateAction.INSTANCE,
            new DeleteDecommissionStateRequest()
        ).get();
        assertTrue(deleteDecommissionStateResponse.isAcknowledged());
        logger.info("--> Deleting decommission done.");

        // will wait for cluster to stabilise with a timeout of 2 min (findPeerInterval for decommissioned nodes)
        // as by then all nodes should have joined the cluster
        ensureStableCluster(6, TimeValue.timeValueSeconds(121));
    }

    public void testDecommissionFailedWhenAttributeNotWeighedAway() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();
        // Start 3 cluster manager eligible nodes
        internalCluster().startClusterManagerOnlyNodes(3, Settings.builder().put(commonSettings).build());
        // start 3 data nodes
        internalCluster().startDataOnlyNodes(3, Settings.builder().put(commonSettings).build());
        ensureStableCluster(6);
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(6))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "c");
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        decommissionRequest.setNoDelay(true);
        assertBusy(() -> {
            DecommissioningFailedException ex = expectThrows(
                DecommissioningFailedException.class,
                () -> client().execute(DecommissionAction.INSTANCE, decommissionRequest).actionGet()
            );
            assertTrue(
                ex.getMessage()
                    .contains("no weights are set to the attribute. Please set appropriate weights before triggering decommission action")
            );
        });

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 1.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        assertBusy(() -> {
            DecommissioningFailedException ex = expectThrows(
                DecommissioningFailedException.class,
                () -> client().execute(DecommissionAction.INSTANCE, decommissionRequest).actionGet()
            );
            assertTrue(ex.getMessage().contains("weight for decommissioned attribute is expected to be [0.0] but found [1.0]"));
        });
    }

    public void testDecommissionFailedWithOnlyOneAttributeValueForLeader() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "b") // force zone values is only set for zones of routing nodes
            .build();
        // Start 3 cluster manager eligible nodes in zone a
        internalCluster().startClusterManagerOnlyNodes(3, Settings.builder().put(commonSettings).put("node.attr.zone", "a").build());
        // Start 3 data nodes in zone b
        internalCluster().startDataOnlyNodes(3, Settings.builder().put(commonSettings).put("node.attr.zone", "b").build());
        ensureStableCluster(6);
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(6))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        logger.info("--> setting shard routing weights");
        Map<String, Double> weights = Map.of("b", 1.0); // weights are expected to be set only for routing nodes
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        // prepare request to attempt to decommission zone 'a'
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "a");
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        decommissionRequest.setNoDelay(true);

        // since there is just one zone present in the cluster, and on initiating decommission for that zone,
        // although all the nodes would be added to voting config exclusion list, but those nodes won't be able to
        // abdicate themselves as we wouldn't have any other leader eligible node which would be declare itself cluster manager
        // and hence due to which the leader won't get abdicated and decommission request should eventually fail.
        // And in this case, to ensure decommission request doesn't leave mutating change in the cluster, we ensure
        // that no exclusion is set to the cluster and state for decommission is marked as FAILED
        OpenSearchTimeoutException ex = expectThrows(
            OpenSearchTimeoutException.class,
            () -> client().execute(DecommissionAction.INSTANCE, decommissionRequest).actionGet()
        );
        assertTrue(ex.getMessage().contains("while removing to-be-decommissioned cluster manager eligible nodes"));

        ClusterService leaderClusterService = internalCluster().getInstance(
            ClusterService.class,
            internalCluster().getClusterManagerName()
        );
        ClusterStateObserver clusterStateObserver = new ClusterStateObserver(
            leaderClusterService,
            null,
            logger,
            client(internalCluster().getClusterManagerName()).threadPool().getThreadContext()
        );
        CountDownLatch expectedStateLatch = new CountDownLatch(1);

        ClusterState currentState = internalCluster().clusterService().state();
        if (currentState.getVotingConfigExclusions().isEmpty()) {
            logger.info("exclusion already cleared");
            expectedStateLatch.countDown();
        } else {
            clusterStateObserver.waitForNextChange(new WaitForClearVotingConfigExclusion(expectedStateLatch));
        }
        // if the below condition is passed, then we are sure exclusion is cleared
        assertTrue(expectedStateLatch.await(30, TimeUnit.SECONDS));

        expectedStateLatch = new CountDownLatch(1);
        currentState = internalCluster().clusterService().state();
        DecommissionAttributeMetadata decommissionAttributeMetadata = currentState.metadata().decommissionAttributeMetadata();
        if (decommissionAttributeMetadata != null && decommissionAttributeMetadata.status().equals(DecommissionStatus.FAILED)) {
            logger.info("decommission status has already turned false");
            expectedStateLatch.countDown();
        } else {
            clusterStateObserver.waitForNextChange(new WaitForFailedDecommissionState(expectedStateLatch));
        }

        // if the below condition is passed, then we are sure current decommission status is marked FAILED
        assertTrue(expectedStateLatch.await(30, TimeUnit.SECONDS));

        // ensure all nodes are part of cluster
        ensureStableCluster(6, TimeValue.timeValueMinutes(2));
    }

    public void testDecommissionAcknowledgedIfWeightsNotSetForNonRoutingNode() throws ExecutionException, InterruptedException {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'd' & 'e' & 'f'");
        List<String> clusterManagerNodes = internalCluster().startNodes(
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "d")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "e")
                .put(onlyRole(commonSettings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                .build(),
            Settings.builder()
                .put(commonSettings)
                .put("node.attr.zone", "f")
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

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        logger.info("--> starting decommissioning nodes in zone {}", 'd');
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "d");
        // Set the timeout to 0 to do immediate Decommission
        DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
        decommissionRequest.setNoDelay(true);
        DecommissionResponse decommissionResponse = client(dataNodes.get(0)).execute(DecommissionAction.INSTANCE, decommissionRequest)
            .get();
        assertTrue(decommissionResponse.isAcknowledged());

        client(dataNodes.get(0)).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();

        ClusterState clusterState = client(dataNodes.get(0)).admin().cluster().prepareState().execute().actionGet().getState();

        // assert that number of nodes should be 5 ( 2 cluster manager nodes + 3 data nodes )
        assertEquals(clusterState.nodes().getNodes().size(), 5);
        assertEquals(clusterState.nodes().getDataNodes().size(), 3);
        assertEquals(clusterState.nodes().getClusterManagerNodes().size(), 2);

        // Recommissioning the zone back to gracefully succeed the test once above tests succeeds
        DeleteDecommissionStateResponse deleteDecommissionStateResponse = client(dataNodes.get(0)).execute(
            DeleteDecommissionStateAction.INSTANCE,
            new DeleteDecommissionStateRequest()
        ).get();
        assertTrue(deleteDecommissionStateResponse.isAcknowledged());

        // will wait for cluster to stabilise with a timeout of 2 min as by then all nodes should have joined the cluster
        ensureStableCluster(6, TimeValue.timeValueMinutes(2));
    }

    public void testConcurrentDecommissionAction() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> start 3 cluster manager nodes on zones 'a' & 'b' & 'c'");
        internalCluster().startNodes(
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
        internalCluster().startNodes(
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
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(Integer.toString(6))
            .execute()
            .actionGet();
        assertFalse(health.isTimedOut());

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 0.0, "b", 1.0, "c", 1.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(weightedRoutingResponse.isAcknowledged());

        AtomicInteger numRequestAcknowledged = new AtomicInteger();
        AtomicInteger numRequestUnAcknowledged = new AtomicInteger();
        AtomicInteger numRequestFailed = new AtomicInteger();
        int concurrentRuns = randomIntBetween(5, 10);
        TestThreadPool testThreadPool = null;
        logger.info("--> starting {} concurrent decommission action in zone {}", concurrentRuns, 'a');
        try {
            testThreadPool = new TestThreadPool(AwarenessAttributeDecommissionIT.class.getName());
            List<Runnable> operationThreads = new ArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(concurrentRuns);
            for (int i = 0; i < concurrentRuns; i++) {
                Runnable thread = () -> {
                    logger.info("Triggering decommission action");
                    DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "a");
                    DecommissionRequest decommissionRequest = new DecommissionRequest(decommissionAttribute);
                    decommissionRequest.setNoDelay(true);
                    try {
                        DecommissionResponse decommissionResponse = client().execute(DecommissionAction.INSTANCE, decommissionRequest)
                            .get();
                        if (decommissionResponse.isAcknowledged()) {
                            numRequestAcknowledged.incrementAndGet();
                        } else {
                            numRequestUnAcknowledged.incrementAndGet();
                        }
                    } catch (Exception e) {
                        numRequestFailed.incrementAndGet();
                    }
                    countDownLatch.countDown();
                };
                operationThreads.add(thread);
            }
            TestThreadPool finalTestThreadPool = testThreadPool;
            operationThreads.forEach(runnable -> finalTestThreadPool.executor("generic").execute(runnable));
            countDownLatch.await();
        } finally {
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
        assertEquals(concurrentRuns, numRequestAcknowledged.get() + numRequestUnAcknowledged.get() + numRequestFailed.get());
        assertEquals(concurrentRuns - 1, numRequestFailed.get());
        assertEquals(1, numRequestAcknowledged.get() + numRequestUnAcknowledged.get());
    }

    private static class WaitForFailedDecommissionState implements ClusterStateObserver.Listener {

        final CountDownLatch doneLatch;

        WaitForFailedDecommissionState(CountDownLatch latch) {
            this.doneLatch = latch;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().decommissionAttributeMetadata();
            if (decommissionAttributeMetadata != null && decommissionAttributeMetadata.status().equals(DecommissionStatus.FAILED)) {
                doneLatch.countDown();
            }
        }

        @Override
        public void onClusterServiceClose() {
            throw new AssertionError("unexpected close");
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            throw new AssertionError("unexpected timeout");
        }
    }

    private static class WaitForClearVotingConfigExclusion implements ClusterStateObserver.Listener {

        final CountDownLatch doneLatch;

        WaitForClearVotingConfigExclusion(CountDownLatch latch) {
            this.doneLatch = latch;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            if (state.getVotingConfigExclusions().isEmpty()) {
                doneLatch.countDown();
            }
        }

        @Override
        public void onClusterServiceClose() {
            throw new AssertionError("unexpected close");
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            throw new AssertionError("unexpected timeout");
        }
    }
}
