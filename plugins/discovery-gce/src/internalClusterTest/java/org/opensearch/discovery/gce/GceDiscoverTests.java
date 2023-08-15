/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.discovery.gce;

import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.NetworkInterface;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cloud.gce.GceInstancesService;
import org.opensearch.cloud.gce.util.Access;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.discovery.gce.GceDiscoveryPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.singletonList;
import static org.opensearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

@OpenSearchIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numDataNodes = 0, numClientNodes = 0)
public class GceDiscoverTests extends OpenSearchIntegTestCase {

    /** Holds a list of the current discovery nodes started in tests **/
    private static final Map<String, DiscoveryNode> nodes = new ConcurrentHashMap<>();

    @After
    public void clearGceNodes() {
        nodes.clear();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "gce")
            .put("cloud.gce.project_id", "test")
            .put("cloud.gce.zone", "test")
            .build();
    }

    public void testJoin() {
        // start cluster-manager node
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        registerGceNode(clusterManagerNode);

        ClusterStateResponse clusterStateResponse = client(clusterManagerNode).admin()
            .cluster()
            .prepareState()
            .setClusterManagerNodeTimeout("1s")
            .clear()
            .setNodes(true)
            .get();
        assertNotNull(clusterStateResponse.getState().nodes().getClusterManagerNodeId());

        // start another node
        final String secondNode = internalCluster().startNode();
        registerGceNode(secondNode);
        clusterStateResponse = client(secondNode).admin()
            .cluster()
            .prepareState()
            .setClusterManagerNodeTimeout("1s")
            .clear()
            .setNodes(true)
            .setLocal(true)
            .get();
        assertNotNull(clusterStateResponse.getState().nodes().getClusterManagerNodeId());

        // wait for the cluster to form
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(2)).get());
        assertNumberOfNodes(2);

        // add one more node and wait for it to join
        final String thirdNode = internalCluster().startDataOnlyNode();
        registerGceNode(thirdNode);
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(3)).get());
        assertNumberOfNodes(3);
    }

    /**
     * Register an existing node as a GCE node
     *
     * @param nodeName the name of the node
     */
    private static void registerGceNode(final String nodeName) {
        final TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
        assertNotNull(transportService);
        final DiscoveryNode discoveryNode = transportService.getLocalNode();
        assertNotNull(discoveryNode);
        if (nodes.put(discoveryNode.getName(), discoveryNode) != null) {
            throw new IllegalArgumentException("Node [" + discoveryNode.getName() + "] cannot be registered twice");
        }
    }

    /**
     * Asserts that the cluster nodes info contains an expected number of node
     *
     * @param expected the expected number of nodes
     */
    private static void assertNumberOfNodes(final int expected) {
        assertEquals(expected, client().admin().cluster().prepareNodesInfo().clear().get().getNodes().size());
    }

    /**
     * Test plugin that exposes internal test cluster nodes as if they were real GCE nodes.
     * Use {@link #registerGceNode(String)} method to expose nodes in the tests.
     */
    public static class TestPlugin extends GceDiscoveryPlugin {

        public TestPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected GceInstancesService createGceInstancesService() {
            return new GceInstancesService() {
                @Override
                public Collection<Instance> instances() {
                    return Access.doPrivileged(() -> {
                        final List<Instance> instances = new ArrayList<>();

                        for (DiscoveryNode discoveryNode : nodes.values()) {
                            Instance instance = new Instance();
                            instance.setName(discoveryNode.getName());
                            instance.setStatus("STARTED");

                            NetworkInterface networkInterface = new NetworkInterface();
                            networkInterface.setNetworkIP(discoveryNode.getAddress().toString());
                            instance.setNetworkInterfaces(singletonList(networkInterface));

                            instances.add(instance);
                        }

                        return instances;
                    });
                }

                @Override
                public String projectId() {
                    return PROJECT_SETTING.get(settings);
                }

                @Override
                public List<String> zones() {
                    return ZONE_SETTING.get(settings);
                }

                @Override
                public void close() throws IOException {}
            };
        }
    }
}
