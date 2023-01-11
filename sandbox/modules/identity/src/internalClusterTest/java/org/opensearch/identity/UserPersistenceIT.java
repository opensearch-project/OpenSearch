/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.junit.rules.TemporaryFolder;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UserPersistenceIT extends HttpSmokeTestCaseWithIdentity {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = super.nodePlugins().stream().collect(Collectors.toList());
        return plugins;
    }

    public void startNodes() throws Exception {
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
    }

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testUserPersistence() throws Exception {
        try {
            TemporaryFolder folder = new TemporaryFolder();
            folder.create();
            File internalUsersYml = folder.newFile("internal_users.yml");
            FileWriter fw1 = new FileWriter(internalUsersYml);
            BufferedWriter bw1 = new BufferedWriter(fw1);
            bw1.write(
                "new-user:\n"
                    + "  hash: \"$2y$12$88IFVl6IfIwCFh5aQYfOmuXVL9j2hz/GusQb35o.4sdTDAEMTOD.K\"\n"
                    + "  attributes:\n"
                    + "    attribute1: \"value1\"\n"
                    + "\n"
                    + "## Demo users\n"
                    + "\n"
                    + "admin:\n"
                    + "  hash: \"$2a$12$VcCDgh2NDk07JGN0rjGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG\"\n"
                    + "\n"
                    + "kibanaserver:\n"
                    + "  hash: \"$2a$12$4AcgAt3xwOWadA5s5blL6ev39OXDNhmOesEoo33eZtrq2N0YrU3H.\"\n"
                    + "\n"
                    + "kibanaro:\n"
                    + "  hash: \"$2a$12$JJSXNfTowz7Uu5ttXfeYpeYE0arACvcwlPBStB1F.MI7f0U9Z4DGC\"\n"
                    + "  attributes:\n"
                    + "    attribute1: \"value1\"\n"
                    + "    attribute2: \"value2\"\n"
                    + "    attribute3: \"value3\"\n"
                    + "\n"
                    + "logstash:\n"
                    + "  hash: \"$2a$12$u1ShR4l4uBS3Uv59Pa2y5.1uQuZBrZtmNfqB3iM/.jL0XoV9sghS2\"\n"
                    + "\n"
                    + "readall:\n"
                    + "  hash: \"$2a$12$ae4ycwzwvLtZxwZ82RmiEunBbIPiAmGZduBAjKN0TXdwQFtCwARz2\"\n"
                    + "\n"
                    + "snapshotrestore:\n"
                    + "  hash: \"$2y$12$DpwmetHKwgYnorbgdvORCenv4NAK8cPUg8AI6pxLCuWf/ALc0.v7W\"\n"
                    + "\n"
            );
            bw1.close();
            // TODO see if possible to do this without relative paths
            final String defaultInitDirectory = folder.getRoot().getAbsolutePath();
            System.setProperty("identity.default_init.dir", defaultInitDirectory);

            startNodes();

            ClusterHealthResponse clusterHealthResponse = client().admin()
                .cluster()
                .prepareHealth()
                .setClusterManagerNodeTimeout("1s")
                .get();

            assertTrue(
                ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX + " index exists",
                clusterHealthResponse.getIndices().containsKey(ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX)
            );

            ClusterIndexHealth identityIndexHealth = clusterHealthResponse.getIndices().get(ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX);
            assertEquals(ClusterHealthStatus.GREEN, identityIndexHealth.getStatus());
        } catch (IOException ioe) {
            fail("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }

    /**
     * This test verifies that identity module can initialize with invalid yml supplied, for this test a user without a
     * hash is supplied in the internal_users.yml file
     *
     * The node should start up with invalid config.
     *
     * TODO Should this prevent node startup, log with warnings, or what should be intended behavior?
     *
     * @throws Exception - This test should not throw an exception
     */
    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testUserPersistenceInvalidYml() throws Exception {
        try {
            TemporaryFolder folder = new TemporaryFolder();
            folder.create();
            File internalUsersYml = folder.newFile("internal_users.yml");
            FileWriter fw1 = new FileWriter(internalUsersYml);
            BufferedWriter bw1 = new BufferedWriter(fw1);
            bw1.write(
                "# Invalid internal_users.yml, hash is required\n"
                    + "new-user:\n"
                    + "  attributes:\n"
                    + "    attribute1: \"value1\"\n"
                    + "\n"
            );
            bw1.close();
            // TODO see if possible to do this without relative paths
            final String defaultInitDirectory = folder.getRoot().getAbsolutePath();
            System.setProperty("identity.default_init.dir", defaultInitDirectory);

            startNodes();

            ClusterHealthResponse clusterHealthResponse = client().admin()
                .cluster()
                .prepareHealth()
                .setClusterManagerNodeTimeout("1s")
                .get();

            assertTrue(
                ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX + " index exists",
                clusterHealthResponse.getIndices().containsKey(ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX)
            );

            ClusterIndexHealth identityIndexHealth = clusterHealthResponse.getIndices().get(ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX);
            assertThat(identityIndexHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        } catch (IOException ioe) {
            fail("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }
}
