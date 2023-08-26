/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.action.admin.cluster.remotestore.RemoteStoreNode.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.action.admin.cluster.remotestore.RemoteStoreNode.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoteStoreRepositoryRegistrationIT extends RemoteStoreBaseIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    private RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        Map<String, String> nodeAttributes = node.getAttributes();
        String type = nodeAttributes.get(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));

        String settingsAttributeKeyPrefix = String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, name);
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> node.getAttributes().get(key)));

        Settings.Builder settings = Settings.builder();
        settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));

        return new RepositoryMetadata(name, type, settings.build());
    }

    private void assertRemoteStoreRepositoryOnAllNodes() {
        RepositoriesMetadata repositories = internalCluster().getInstance(ClusterService.class, internalCluster().getNodeNames()[0])
            .state()
            .metadata()
            .custom(RepositoriesMetadata.TYPE);
        RepositoryMetadata actualSegmentRepository = repositories.repository(REPOSITORY_NAME);
        RepositoryMetadata actualTranslogRepository = repositories.repository(REPOSITORY_2_NAME);

        for (String nodeName : internalCluster().getNodeNames()) {
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
            DiscoveryNode node = clusterService.localNode();
            RepositoryMetadata expectedSegmentRepository = buildRepositoryMetadata(node, REPOSITORY_NAME);
            RepositoryMetadata expectedTranslogRepository = buildRepositoryMetadata(node, REPOSITORY_2_NAME);
            assertTrue(actualSegmentRepository.equalsIgnoreGenerations(expectedSegmentRepository));
            assertTrue(actualTranslogRepository.equalsIgnoreGenerations(expectedTranslogRepository));
        }
    }

    public void testSingleNodeClusterRepositoryRegistration() {
        internalCluster().startClusterManagerOnlyNode(remoteStoreNodeAttributes(REPOSITORY_NAME, REPOSITORY_2_NAME));
        ensureStableCluster(1);

        assertRemoteStoreRepositoryOnAllNodes();
    }

    public void testMultiNodeClusterRepositoryRegistration() {
        Settings clusterSettings = remoteStoreNodeAttributes(REPOSITORY_NAME, REPOSITORY_2_NAME);
        internalCluster().startClusterManagerOnlyNode(clusterSettings);
        internalCluster().startNodes(3, clusterSettings);
        ensureStableCluster(4);

        assertRemoteStoreRepositoryOnAllNodes();
    }

    public void testMultiNodeClusterOnlyDataRepositoryRegistration() {
        Settings clusterSettings = remoteStoreNodeAttributes(REPOSITORY_NAME, REPOSITORY_2_NAME);
        internalCluster().startNodes(3, clusterSettings);
        ensureStableCluster(3);

        assertRemoteStoreRepositoryOnAllNodes();
    }

    public void testMultiNodeClusterRepositoryRegistrationWithMultipleMasters() {
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startNodes(3);
        ensureStableCluster(6);

        assertRemoteStoreRepositoryOnAllNodes();
    }
}
