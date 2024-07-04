/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.node.Node;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class WarmIndexRemoteStoreSegmentReplicationIT extends SegmentReplicationIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;
    protected boolean clusterSettingsSuppliedByTest = false;

    @Before
    private void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL.name())
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (clusterSettingsSuppliedByTest) {
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
        } else {
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, translogRepoPath))
                //.put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), -1)
                .build();
        }
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.TIERED_REMOTE_INDEX, true);

        return featureSettings.build();
    }

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    protected boolean warmIndexSegmentReplicationEnabled() {
        return true;
    }

    @After
    public void teardown() {
        clusterSettingsSuppliedByTest = false;
        for (String nodeName : internalCluster().getNodeNames()) {
            logger.info("file cache node name is {}", nodeName);
            FileCache fileCache = internalCluster().getInstance(Node.class, nodeName).fileCache();
            fileCache.clear();
        }
        assertRemoteStoreRepositoryOnAllNodes(REPOSITORY_NAME);
        assertRemoteStoreRepositoryOnAllNodes(REPOSITORY_2_NAME);
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
        clusterAdmin().prepareCleanupRepository(REPOSITORY_2_NAME).get();
    }

    public RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
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
        settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

        return new RepositoryMetadata(name, type, settings.build());
    }

    public void assertRemoteStoreRepositoryOnAllNodes(String repositoryName) {
        RepositoriesMetadata repositories = internalCluster().getInstance(ClusterService.class, internalCluster().getNodeNames()[0])
            .state()
            .metadata()
            .custom(RepositoriesMetadata.TYPE);
        RepositoryMetadata actualRepository = repositories.repository(repositoryName);

        final RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);

        for (String nodeName : internalCluster().getNodeNames()) {
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
            DiscoveryNode node = clusterService.localNode();
            RepositoryMetadata expectedRepository = buildRepositoryMetadata(node, repositoryName);

            // Validated that all the restricted settings are entact on all the nodes.
            repository.getRestrictedSystemRepositorySettings()
                .stream()
                .forEach(
                    setting -> assertEquals(
                        String.format(Locale.ROOT, "Restricted Settings mismatch [%s]", setting.getKey()),
                        setting.get(actualRepository.settings()),
                        setting.get(expectedRepository.settings())
                    )
                );
        }
    }

}
