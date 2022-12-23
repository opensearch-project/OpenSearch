/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.awarenesshealth;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class ClusterAwarenessAttributeHealthTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;

    protected static Set<DiscoveryNodeRole> NODE_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool("ClusterAwarenessAttributeHealthTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        CapturingTransport transport = new CapturingTransport();
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void terminateThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testClusterAwarenessAttributeHealth() {

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .put(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values",
                "zone_1, zone_2, zone_3"
            )
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(
                DiscoveryNodes.builder(clusterService.state().nodes())
                    .add(
                        new DiscoveryNode(
                            "node1",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_1"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node2",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_2"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node3",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_3"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
            )
            .build();

        Map<String, NodeShardInfo> shardMapPerNode = new HashMap<>();
        shardMapPerNode.put("node1", new NodeShardInfo("node1", 2, 0, 0));
        shardMapPerNode.put("node2", new NodeShardInfo("node2", 2, 0, 0));
        shardMapPerNode.put("node3", new NodeShardInfo("node3", 2, 0, 0));

        ClusterAwarenessAttributeHealth clusterAwarenessAttributeHealth = new ClusterAwarenessAttributeHealth(
            "zone",
            null,
            true,
            shardMapPerNode,
            clusterState.nodes().getDataNodes(),
            12
        );

        assertEquals("zone", clusterAwarenessAttributeHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributeHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(3, attributeValueMap.size());
        assertClusterAwarenessAttributeValueHealth(attributeValueMap, true);
    }

    public void testClusterAwarenessAttributeHealthNodeAttributeDoesNotExists() {

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .put(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values",
                "zone_1, zone_2, zone_3"
            )
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(
                DiscoveryNodes.builder(clusterService.state().nodes())
                    .add(
                        new DiscoveryNode(
                            "node1",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_1"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node2",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_2"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node3",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_3"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
            )
            .build();

        Map<String, NodeShardInfo> shardMapPerNode = new HashMap<>();
        shardMapPerNode.put("node1", new NodeShardInfo("node1", 2, 0, 0));
        shardMapPerNode.put("node2", new NodeShardInfo("node2", 2, 0, 0));
        shardMapPerNode.put("node3", new NodeShardInfo("node3", 2, 0, 0));

        ClusterAwarenessAttributeHealth clusterAwarenessAttributeHealth = new ClusterAwarenessAttributeHealth(
            "rack",
            null,
            true,
            shardMapPerNode,
            clusterState.nodes().getDataNodes(),
            12
        );

        assertEquals("rack", clusterAwarenessAttributeHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributeHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(0, attributeValueMap.size());
    }

    public void testClusterAwarenessAttributeHealthNoReplicaEnforcement() {

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .put(
                AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values",
                "zone_1, zone_2, zone_3"
            )
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(
                DiscoveryNodes.builder(clusterService.state().nodes())
                    .add(
                        new DiscoveryNode(
                            "node1",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_1"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node2",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_2"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node3",
                            buildNewFakeTransportAddress(),
                            singletonMap("zone", "zone_3"),
                            NODE_ROLE,
                            Version.CURRENT
                        )
                    )
            )
            .build();

        Map<String, NodeShardInfo> shardMapPerNode = new HashMap<>();
        shardMapPerNode.put("node1", new NodeShardInfo("node1", 2, 0, 0));
        shardMapPerNode.put("node2", new NodeShardInfo("node2", 2, 0, 0));
        shardMapPerNode.put("node3", new NodeShardInfo("node3", 2, 0, 0));

        ClusterAwarenessAttributeHealth clusterAwarenessAttributeHealth = new ClusterAwarenessAttributeHealth(
            "zone",
            null,
            false,
            shardMapPerNode,
            clusterState.nodes().getDataNodes(),
            12
        );

        assertEquals("zone", clusterAwarenessAttributeHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributeHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(3, attributeValueMap.size());
        assertClusterAwarenessAttributeValueHealth(attributeValueMap, false);
    }

    private void assertClusterAwarenessAttributeValueHealth(
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap,
        boolean replicaEnforcement
    ) {
        for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : attributeValueMap.values()) {
            assertEquals("1.0", String.valueOf(clusterAwarenessAttributeValueHealth.getWeight()));
            assertEquals(2, clusterAwarenessAttributeValueHealth.getActiveShards());
            if (replicaEnforcement) {
                assertEquals(2, clusterAwarenessAttributeValueHealth.getUnassignedShards());
            } else {
                assertEquals(-1, clusterAwarenessAttributeValueHealth.getUnassignedShards());
            }
            assertEquals(1, clusterAwarenessAttributeValueHealth.getNodes());
        }
    }

}
