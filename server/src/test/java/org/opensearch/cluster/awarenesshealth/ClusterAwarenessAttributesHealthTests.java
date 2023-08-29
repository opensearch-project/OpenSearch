/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.awarenesshealth;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class ClusterAwarenessAttributesHealthTests extends OpenSearchTestCase {

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

        ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(
            "zone",
            true,
            clusterState
        );

        assertEquals("zone", clusterAwarenessAttributesHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributesHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(3, attributeValueMap.size());
    }

    public void testClusterAwarenessAttributeHealthNodeAttributeDoesNotExists() {
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

        ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(
            "rack",
            true,
            clusterState
        );

        assertEquals("rack", clusterAwarenessAttributesHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributesHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(0, attributeValueMap.size());
    }

    public void testClusterAwarenessAttributeHealthNoReplicaEnforcement() {
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

        ClusterAwarenessAttributesHealth clusterAwarenessAttributesHealth = new ClusterAwarenessAttributesHealth(
            "zone",
            false,
            clusterState
        );

        assertEquals("zone", clusterAwarenessAttributesHealth.getAwarenessAttributeName());
        Map<String, ClusterAwarenessAttributeValueHealth> attributeValueMap = clusterAwarenessAttributesHealth
            .getAwarenessAttributeHealthMap();
        assertEquals(3, attributeValueMap.size());
    }
}
