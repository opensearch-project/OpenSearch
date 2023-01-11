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
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.RoutingTableGenerator;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class ClusterAwarenessHealthTests extends OpenSearchTestCase {

    private final IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(
        new ThreadContext(Settings.EMPTY)
    );

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;

    protected static Set<DiscoveryNodeRole> NODE_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool("ClusterAwarenessHealthTests");
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

    public void testClusterHealthWithNoAwarenessAttribute() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
            .nodes(clusterService.state().nodes())
            .build();

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(0, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
        }
    }

    ClusterAwarenessHealth serializeResponse(ClusterAwarenessHealth clusterAwarenessHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterAwarenessHealth.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            clusterAwarenessHealth = new ClusterAwarenessHealth(in);
        }
        return clusterAwarenessHealth;
    }

    public void testClusterHealthWithAwarenessAttributeAndNoNodeAttribute() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
            .nodes(clusterService.state().nodes())
            .build();

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(0, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
        }
    }

    public void testClusterHealthWithAwarenessAttributeAndNodeAttribute() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
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

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(3, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
            for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : attributeHealthMap.get(attributesName)
                .getAwarenessAttributeHealthMap()
                .values()) {
                assertEquals(-1, clusterAwarenessAttributeValueHealth.getUnassignedShards());
            }
        }
    }

    public void testClusterHealthWithAwarenessAttributeAndReplicaEnforcementEnabled() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = 1;
            int numberOfReplicas = 5;
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone" + ".values", "a,b,c")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
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

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(3, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
            for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : attributeHealthMap.get(attributesName)
                .getAwarenessAttributeHealthMap()
                .values()) {
                assertEquals(1.0, clusterAwarenessAttributeValueHealth.getWeight(), 0.0);
            }
        }
    }

    public void testClusterHealthWithAwarenessAttributeAndReplicaEnforcementNotEnabled() throws IOException {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 4; i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();
            IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);
            metadata.put(indexMetadata, true);
            routingTable.add(indexRoutingTable);
        }

        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable.build())
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

        indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), (String[]) null);

        ClusterAwarenessHealth clusterAwarenessHealth = new ClusterAwarenessHealth(clusterState, clusterSettings, "zone");
        clusterAwarenessHealth = serializeResponse(clusterAwarenessHealth);
        Map<String, ClusterAwarenessAttributesHealth> attributeHealthMap = clusterAwarenessHealth.getClusterAwarenessAttributesHealthMap();
        for (String attributesName : attributeHealthMap.keySet()) {
            assertEquals("zone", attributesName);
            assertEquals(3, attributeHealthMap.get(attributesName).getAwarenessAttributeHealthMap().size());
            for (ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth : attributeHealthMap.get(attributesName)
                .getAwarenessAttributeHealthMap()
                .values()) {
                assertEquals(-1, clusterAwarenessAttributeValueHealth.getUnassignedShards());
                assertEquals(1.0, clusterAwarenessAttributeValueHealth.getWeight(), 0.0);
            }
        }
    }
}
