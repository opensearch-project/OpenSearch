/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;
import org.opensearch.transport.nio.MockNioTransportPlugin;
import org.opensearch.transport.nio.NioTransportPlugin;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

/**
 * Abstract Rest Test Case for IdentityPlugin that installs and enables IdentityPlugin and removes mock
 * http transport to enable REST requests against a test cluster
 *
 * @opensearch.experimental
 */
// TODO not sure why ThreadLeakScope.NONE is required
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class HttpSmokeTestCaseWithIdentity extends OpenSearchIntegTestCase {

    private static String nodeTransportTypeKey;
    private static String nodeHttpTypeKey;
    private static String clientTypeKey;

    @SuppressWarnings("unchecked")
    @BeforeClass
    public static void setUpTransport() {
        nodeTransportTypeKey = getTypeKey(randomFrom(getTestTransportPlugin(), Netty4ModulePlugin.class, NioTransportPlugin.class));
        nodeHttpTypeKey = getHttpTypeKey(randomFrom(Netty4ModulePlugin.class, NioTransportPlugin.class));
        clientTypeKey = getTypeKey(randomFrom(getTestTransportPlugin(), Netty4ModulePlugin.class, NioTransportPlugin.class));
    }

    private static String getTypeKey(Class<? extends Plugin> clazz) {
        if (clazz.equals(MockNioTransportPlugin.class)) {
            return MockNioTransportPlugin.MOCK_NIO_TRANSPORT_NAME;
        } else if (clazz.equals(NioTransportPlugin.class)) {
            return NioTransportPlugin.NIO_TRANSPORT_NAME;
        } else {
            assert clazz.equals(Netty4ModulePlugin.class);
            return Netty4ModulePlugin.NETTY_TRANSPORT_NAME;
        }
    }

    private static String getHttpTypeKey(Class<? extends Plugin> clazz) {
        if (clazz.equals(NioTransportPlugin.class)) {
            return NioTransportPlugin.NIO_HTTP_TRANSPORT_NAME;
        } else {
            assert clazz.equals(Netty4ModulePlugin.class);
            return Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME;
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.TRANSPORT_TYPE_KEY, nodeTransportTypeKey)
            .put(NetworkModule.HTTP_TYPE_KEY, nodeHttpTypeKey)
            .put(ConfigConstants.IDENTITY_ENABLED, true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(getTestTransportPlugin(), Netty4ModulePlugin.class, NioTransportPlugin.class, IdentityPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    protected void startNodes() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();

        ClusterStateResponse clusterStateResponse = client(clusterManagerNode).admin()
            .cluster()
            .prepareState()
            .setClusterManagerNodeTimeout("1s")
            .clear()
            .setNodes(true)
            .get();
        assertNotNull(clusterStateResponse.getState().nodes().getClusterManagerNodeId());

        // start another node
        final String dataNode = internalCluster().startDataOnlyNode();
        clusterStateResponse = client(dataNode).admin()
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
        List<NodeInfo> nodeInfos = client().admin().cluster().prepareNodesInfo().get().getNodes();
        assertEquals(2, nodeInfos.size());

        Thread.sleep(1000);
    }

    protected void ensureIdentityIndexIsGreen() {
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setClusterManagerNodeTimeout("1s").get();

        assertTrue(
            ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX + " index exists",
            clusterHealthResponse.getIndices().containsKey(ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX)
        );

        ClusterIndexHealth identityIndexHealth = clusterHealthResponse.getIndices().get(ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX);
        assertEquals(ClusterHealthStatus.GREEN, identityIndexHealth.getStatus());
    }

}
