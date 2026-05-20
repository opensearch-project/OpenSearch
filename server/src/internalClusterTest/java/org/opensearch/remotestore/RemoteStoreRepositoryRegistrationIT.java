/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster.RestartCallback;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.repositories.blobstore.BlobStoreRepository.SYSTEM_REPOSITORY_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreRepositoryRegistrationIT extends RemoteStoreBaseIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    public void testSingleNodeClusterRepositoryRegistration() throws Exception {
        internalCluster().startNode();
    }

    public void testMultiNodeClusterRepositoryRegistration() throws Exception {
        internalCluster().startNodes(3);
    }

    public void testMultiNodeClusterRepositoryRegistrationWithMultipleClusterManager() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startNodes(3);
    }

    public void testMultiNodeClusterActiveClusterManagerShutDown() throws Exception {
        internalCluster().startNodes(3);
        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(2);
    }

    public void testMultiNodeClusterActiveMClusterManagerRestart() throws Exception {
        internalCluster().startNodes(3);
        String clusterManagerNodeName = internalCluster().getClusterManagerName();
        internalCluster().restartNode(clusterManagerNodeName);
        ensureStableCluster(3);
    }

    public void testMultiNodeClusterRandomNodeRestart() throws Exception {
        internalCluster().startNodes(3);
        internalCluster().restartRandomDataNode();
        ensureStableCluster(3);
    }

    public void testMultiNodeClusterActiveClusterManagerRecoverNetworkIsolation() {
        internalCluster().startClusterManagerOnlyNodes(3);
        String dataNode = internalCluster().startNode();

        NetworkDisruption partition = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(partition);

        partition.startDisrupting();
        ensureStableCluster(3, dataNode);
        partition.stopDisrupting();

        ensureStableCluster(4);

        internalCluster().clearDisruptionScheme();
    }

    public void testMultiNodeClusterRandomNodeRecoverNetworkIsolation() {
        Set<String> nodesInOneSide = internalCluster().startNodes(3).stream().collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInAnotherSide = internalCluster().startNodes(3).stream().collect(Collectors.toCollection(HashSet::new));
        ensureStableCluster(6);

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInAnotherSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        networkDisruption.startDisrupting();
        ensureStableCluster(3, nodesInOneSide.stream().findAny().get());
        networkDisruption.stopDisrupting();

        ensureStableCluster(6);

        internalCluster().clearDisruptionScheme();
    }

    public void testMultiNodeClusterRandomNodeRecoverNetworkIsolationPostNonRestrictedSettingsUpdate() {
        Set<String> nodesInOneSide = internalCluster().startNodes(3).stream().collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInAnotherSide = internalCluster().startNodes(3).stream().collect(Collectors.toCollection(HashSet::new));
        ensureStableCluster(6);

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInAnotherSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        networkDisruption.startDisrupting();

        final Client client = client(nodesInOneSide.iterator().next());
        RepositoryMetadata repositoryMetadata = client.admin()
            .cluster()
            .prepareGetRepositories(REPOSITORY_NAME)
            .get()
            .repositories()
            .get(0);
        Settings.Builder updatedSettings = Settings.builder().put(repositoryMetadata.settings()).put("chunk_size", new ByteSizeValue(20));
        updatedSettings.remove("system_repository");
        OpenSearchIntegTestCase.putRepositoryRequestBuilder(
            client.admin().cluster(),
            repositoryMetadata.name(),
            repositoryMetadata.type(),
            true,
            updatedSettings,
            null,
            false
        ).get();

        ensureStableCluster(3, nodesInOneSide.stream().findAny().get());
        networkDisruption.stopDisrupting();

        ensureStableCluster(6);

        internalCluster().clearDisruptionScheme();
    }

    public void testNodeRestartPostNonRestrictedSettingsUpdate() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startNodes(3);

        final Client client = client();
        RepositoryMetadata repositoryMetadata = client.admin()
            .cluster()
            .prepareGetRepositories(REPOSITORY_NAME)
            .get()
            .repositories()
            .get(0);
        Settings.Builder updatedSettings = Settings.builder().put(repositoryMetadata.settings()).put("chunk_size", new ByteSizeValue(20));
        updatedSettings.remove("system_repository");

        createRepository(repositoryMetadata.name(), repositoryMetadata.type(), updatedSettings);

        internalCluster().restartRandomDataNode();

        ensureStableCluster(4);
    }

    public void testSystemRepositorySettingIsHiddenForGetRepositoriesRequest() throws IOException {
        GetRepositoriesRequest request = new GetRepositoriesRequest(new String[] { REPOSITORY_NAME });
        GetRepositoriesResponse repositoriesResponse = client().execute(GetRepositoriesAction.INSTANCE, request).actionGet();
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(randomFrom(XContentType.JSON));
        XContentBuilder xContentBuilder = repositoriesResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        repositoriesResponse = GetRepositoriesResponse.fromXContent(createParser(xContentBuilder));
        assertEquals(false, SYSTEM_REPOSITORY_SETTING.get(repositoriesResponse.repositories().get(0).settings()));
    }

    /**
     * Test node join failure when trying to join a cluster with different remote store repository attributes.
     * This negative test case verifies that nodes with incompatible remote store configurations are rejected.
     */
    public void testNodeJoinFailureWithDifferentRemoteStoreRepositoryAttributes() throws Exception {
        // Start initial cluster with specific remote store repository configuration
        internalCluster().startNode();
        ensureStableCluster(1);

        // Attempt to start a second node with different remote store attributes
        // This should fail because the remote store repository attributes don't match
        expectThrows(IllegalStateException.class, () -> {
            internalCluster().startNode(
                Settings.builder()
                    .put("node.attr.remote_store.segment.repository", "different-repo")
                    .put("node.attr.remote_store.translog.repository", "different-translog-repo")
                    .build()
            );
            ensureStableCluster(2);
        });

        ensureStableCluster(1);
    }

    /**
     * Test node rejoin failure when node attributes are changed after initial join.
     * This test verifies that a node cannot rejoin the cluster with different remote store attributes.
     */
    public void testNodeRejoinFailureWithChangedRemoteStoreAttributes() throws Exception {
        // Start cluster with 2 nodes
        internalCluster().startNodes(2);
        ensureStableCluster(2);

        String nodeToRestart = internalCluster().getNodeNames()[1];

        // Attempt to restart node with different remote store attributes should fail
        // The validation happens during node startup and throws IllegalStateException
        expectThrows(IllegalStateException.class, () -> {
            internalCluster().restartNode(nodeToRestart, new RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) {
                    // Return different remote store attributes when restarting
                    // This will fail because it's missing the required repository type attributes
                    return Settings.builder()
                        .put("node.attr.remote_store.segment.repository", "changed-segment-repo")
                        .put("node.attr.remote_store.translog.repository", "changed-translog-repo")
                        .build();
                }
            });
        });

        ensureStableCluster(1);
    }

    /**
     * Test node join failure when missing required remote store attributes.
     * This test verifies that nodes without proper remote store configuration are rejected.
     */
    public void testNodeJoinFailureWithMissingRemoteStoreAttributes() throws Exception {
        internalCluster().startNode();
        ensureStableCluster(1);

        // Attempt to add a node without remote store attributes
        // This should fail because remote store attributes are required
        expectThrows(IllegalStateException.class, () -> {
            internalCluster().startNode(
                Settings.builder()
                    .putNull("node.attr.remote_store.segment.repository")
                    .putNull("node.attr.remote_store.translog.repository")
                    .build()
            );
        });

        ensureStableCluster(1);
    }

    /**
     * Test repository verification failure during node join.
     * This test verifies that nodes fail to join when remote store repositories cannot be verified
     * due to invalid repository settings or missing repository type information.
     */
    public void testRepositoryVerificationFailureDuringNodeJoin() throws Exception {
        internalCluster().startNode();
        ensureStableCluster(1);

        // Attempt to start a node with invalid repository type - this should fail during repository validation
        // We use an invalid repository type that doesn't exist to trigger repository verification failure
        expectThrows(Exception.class, () -> {
            internalCluster().startNode(
                Settings.builder()
                    .put("node.attr.remote_store.segment.repository", REPOSITORY_NAME)
                    .put("node.attr.remote_store.translog.repository", REPOSITORY_NAME)
                    .put("node.attr.remote_store.repository." + REPOSITORY_NAME + ".type", "invalid_repo_type")
                    .build()
            );
        });

        ensureStableCluster(1);
    }

}
