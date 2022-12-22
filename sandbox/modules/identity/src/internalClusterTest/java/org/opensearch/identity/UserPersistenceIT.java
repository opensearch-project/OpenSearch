/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UserPersistenceIT extends HttpSmokeTestCaseWithIdentity {

    public static final String TEST_RESOURCE_RELATIVE_PATH = "../../resources/test/";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = super.nodePlugins().stream().collect(Collectors.toList());
        return plugins;
    }

    public void testUserPersistence() throws Exception {
        // TODO see if possible to do this without relative paths
        final String defaultInitDirectory = Paths.get("../../resources/internalClusterTest/persistence").toAbsolutePath().toString();
        System.setProperty("identity.default_init.dir", defaultInitDirectory);

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

        Thread.sleep(3000);

        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setClusterManagerNodeTimeout("1s").get();

        assertTrue(
            ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX + " index exists",
            clusterHealthResponse.getIndices().containsKey(ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX)
        );

        ClusterIndexHealth identityIndexHealth = clusterHealthResponse.getIndices().get(ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX);
        assertEquals(ClusterHealthStatus.GREEN, identityIndexHealth.getStatus());
    }
}
