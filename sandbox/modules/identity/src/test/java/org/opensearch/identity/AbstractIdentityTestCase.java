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
import org.opensearch.common.settings.Settings;
import org.opensearch.http.CorsHandler;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Collection;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

/**
 * Base test case for integration tests against the identity plugin.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class AbstractIdentityTestCase extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(IdentityPlugin.class, Netty4ModulePlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(nodeSettings()).build();
    }

    final Settings nodeSettings() {
        return Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), CorsHandler.ANY_ORIGIN)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .put(ConfigConstants.IDENTITY_ENABLED, true)
            .build();
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
