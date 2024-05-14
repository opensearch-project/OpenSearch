/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.translogmetadata;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreIT;
import org.opensearch.remotestore.translogmetadata.mocks.MockFsMetadataSupportedRepositoryPlugin;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreTranslogMetadataIT extends RemoteStoreIT {

    protected final String INDEX_NAME = "remote-store-test-idx-1";
    Path repositoryLocation;
    boolean compress;
    boolean overrideBuildRepositoryMetadata;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockFsMetadataSupportedRepositoryPlugin.class);
    }

    @Before
    public void setup() {
        clusterSettingsSuppliedByTest = true;
        overrideBuildRepositoryMetadata = false;
        repositoryLocation = randomRepoPath();
        compress = randomBoolean();
    }

    @Override
    public RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        if (overrideBuildRepositoryMetadata) {
            Map<String, String> nodeAttributes = node.getAttributes();
            String type = nodeAttributes.get(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));

            String settingsAttributeKeyPrefix = String.format(
                Locale.getDefault(),
                REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
                name
            );
            Map<String, String> settingsMap = node.getAttributes()
                .keySet()
                .stream()
                .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
                .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> node.getAttributes().get(key)));

            Settings.Builder settings = Settings.builder();
            settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));
            settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

            if (name.equals(REPOSITORY_NAME)) {
                settings.put("location", repositoryLocation)
                    .put("compress", compress)
                    .put("max_remote_upload_bytes_per_sec", "1kb")
                    .put("chunk_size", 100, ByteSizeUnit.BYTES);
                return new RepositoryMetadata(name, MockFsMetadataSupportedRepositoryPlugin.TYPE_MD, settings.build());
            }
            return new RepositoryMetadata(name, type, settings.build());
        } else {
            return super.buildRepositoryMetadata(node, name);
        }

    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                remoteStoreClusterSettings(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD
                )
            )
            .build();
    }

    // Test local only translog files which are not uploaded to remote store (no metadata present in remote)
    // Without the cleanup change in RemoteFsTranslog.createEmptyTranslog, this test fails with NPE.
    public void testLocalOnlyTranslogCleanupOnNodeRestart() throws Exception {
        clusterSettingsSuppliedByTest = true;

        // Overriding settings to use AsyncMultiStreamBlobContainer
        Settings settings = Settings.builder()
            .put(super.nodeSettings(1))
            .put(
                remoteStoreClusterSettings(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    MockFsMetadataSupportedRepositoryPlugin.TYPE_MD
                )
            )
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        String dataNode = internalCluster().startDataOnlyNode(settings);

        // 1. Create index with 0 replica
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 10000L, -1));
        ensureGreen(INDEX_NAME);

        // 2. Index docs
        int searchableDocs = 0;
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            indexBulk(INDEX_NAME, 15);
            refresh(INDEX_NAME);
            searchableDocs += 15;
        }
        indexBulk(INDEX_NAME, 15);

        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs);

        // 3. Delete metadata from remote translog
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);

        String shardPath = getShardLevelBlobPath(client(), INDEX_NAME, BlobPath.cleanPath(), "0", TRANSLOG, METADATA).buildAsString();
        Path translogMetaDataPath = Path.of(translogRepoPath + "/" + shardPath);

        try (Stream<Path> files = Files.list(translogMetaDataPath)) {
            files.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    // Ignore
                }
            });
        }

        internalCluster().restartNode(dataNode);

        ensureGreen(INDEX_NAME);

        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs);
        indexBulk(INDEX_NAME, 15);
        refresh(INDEX_NAME);
        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs + 15);
    }

}
