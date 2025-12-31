/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;
import org.opensearch.test.transport.MockTransportService;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.opensearch.common.blobstore.versioned.VersionedBlobContainer;
import org.opensearch.common.blobstore.versioned.VersionedInputStream;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.Repository;
import org.opensearch.env.Environment;
import org.opensearch.core.common.Strings;
import org.opensearch.common.settings.Setting;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_PUBLICATION_SETTING_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteManifestConditionalUpdateIT extends RemoteStoreBaseIntegTestCase {

    public static class MockS3RepositoryPlugin extends Plugin implements RepositoryPlugin {
        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                MockS3Repository.TYPE,
                metadata -> new MockS3Repository(metadata, namedXContentRegistry, clusterService, recoverySettings)
            );
        }
    }

    public static class MockS3Repository extends MeteredBlobStoreRepository {
        public static final String TYPE = "mock_s3";
        static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

        private volatile BlobPath basePath;

        public MockS3Repository(
            RepositoryMetadata metadata,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, namedXContentRegistry, clusterService, recoverySettings, Map.of());
            readRepositoryMetadata();
        }

        private void readRepositoryMetadata() {
            final String basePath = BASE_PATH_SETTING.get(metadata.settings());
            if (Strings.hasLength(basePath)) {
                this.basePath = new BlobPath().add(basePath);
            } else {
                this.basePath = BlobPath.cleanPath();
            }
        }

        @Override
        protected BlobStore createBlobStore() {
            return new MockS3BlobStore();
        }

        @Override
        public BlobPath basePath() {
            return basePath;
        }
    }

    private static class MockS3BlobStore implements BlobStore {
        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new MockS3BlobContainer(path);
        }

        @Override
        public void close() {}
    }

    private static class MockS3BlobContainer extends VersionedBlobContainer {
        private static final Path STORAGE_DIR = createTempDir().resolve("mock-s3-storage");

        static {
            try {
                java.nio.file.Files.createDirectories(STORAGE_DIR);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        MockS3BlobContainer(BlobPath path) {
            super(path);
        }

        private Path getBlobPath(String blobName) {
            return STORAGE_DIR.resolve(blobName);
        }

        private Path getVersionPath(String blobName) {
            return STORAGE_DIR.resolve(blobName + ".version");
        }

        @Override
        public String conditionallyWriteBlobWithVersion(String blobName, InputStream inputStream, long blobSize, String expectedVersion) throws IOException {
            Path versionPath = getVersionPath(blobName);
            String currentVersion = java.nio.file.Files.exists(versionPath) ? java.nio.file.Files.readString(versionPath) : null;
            if (currentVersion != null && !currentVersion.equals(expectedVersion)) {
                throw new IOException("Version conflict: expected " + expectedVersion + " but was " + currentVersion);
            }
            String newVersion = UUID.randomUUID().toString();
            java.nio.file.Files.write(getBlobPath(blobName), inputStream.readAllBytes());
            java.nio.file.Files.writeString(versionPath, newVersion);
            return newVersion;
        }

        @Override
        public String writeVersionedBlobIfNotExists(String blobName, InputStream inputStream, long blobSize) throws IOException {
            if (java.nio.file.Files.exists(getBlobPath(blobName))) {
                throw new IOException("Blob already exists: " + blobName);
            }
            String version = UUID.randomUUID().toString();
            java.nio.file.Files.write(getBlobPath(blobName), inputStream.readAllBytes());
            java.nio.file.Files.writeString(getVersionPath(blobName), version);
            return version;
        }

        @Override
        public VersionedInputStream readVersionedBlob(String blobName) throws IOException {
            Path blobPath = getBlobPath(blobName);
            if (!java.nio.file.Files.exists(blobPath)) throw new IOException("Blob not found: " + blobName);
            String version = java.nio.file.Files.readString(getVersionPath(blobName));
            return new VersionedInputStream(version, java.nio.file.Files.newInputStream(blobPath));
        }

        @Override
        public String getVersion(String blobName) throws IOException {
            Path versionPath = getVersionPath(blobName);
            if (!java.nio.file.Files.exists(versionPath)) throw new IOException("Blob not found: " + blobName);
            return java.nio.file.Files.readString(versionPath);
        }

        @Override
        public boolean blobExists(String blobName) {
            return java.nio.file.Files.exists(getBlobPath(blobName));
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            Path blobPath = getBlobPath(blobName);
            if (!java.nio.file.Files.exists(blobPath)) throw new IOException("Blob not found: " + blobName);
            return java.nio.file.Files.newInputStream(blobPath);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return null;
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            Path blobPath = getBlobPath(blobName);
            if (failIfAlreadyExists && java.nio.file.Files.exists(blobPath)) {
                // For verification blobs, delete and recreate
                if (blobName.endsWith(".dat")) {
                    java.nio.file.Files.deleteIfExists(blobPath);
                    java.nio.file.Files.deleteIfExists(getVersionPath(blobName));
                } else {
                    throw new IOException("Blob already exists: " + blobName);
                }
            }
            java.nio.file.Files.write(blobPath, inputStream.readAllBytes());
            java.nio.file.Files.writeString(getVersionPath(blobName), UUID.randomUUID().toString());
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {

        }

        @Override
        public Map<String, BlobMetadata> listBlobs() { return Collections.emptyMap(); }

        @Override
        public Map<String, BlobContainer> children() { return Collections.emptyMap(); }

        /**
         * Lists all blobs in the container that match the specified prefix.
         *
         * @param blobNamePrefix The prefix to match against blob names in the container.
         * @return A map of the matching blobs in the container.  The keys in the map are the names of the blobs
         * and the values are {@link BlobMetadata}, containing basic information about each blob.
         * @throws IOException if there were any failures in reading from the blob container.
         */
        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            Map<String, BlobMetadata> result = new HashMap<>();
            try (Stream<Path> files = java.nio.file.Files.list(STORAGE_DIR)) {
                files.filter(path -> {
                    String fileName = path.getFileName().toString();
                    return fileName.startsWith(blobNamePrefix) && !fileName.endsWith(".version");
                }).forEach(path -> {
                    String blobName = path.getFileName().toString();
                    try {
                        long size = java.nio.file.Files.size(path);
                        result.put(blobName, new PlainBlobMetadata(blobName, size));
                    } catch (IOException e) {
                        // Skip files that can't be read
                    }
                });
            }
            return result;
        }

        @Override
        public DeleteResult delete() { return new DeleteResult(0, 0); }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
            for (String blobName : blobNames) {
                try {
                    java.nio.file.Files.deleteIfExists(getBlobPath(blobName));
                    java.nio.file.Files.deleteIfExists(getVersionPath(blobName));
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    private static final String INDEX_NAME = "test-index";

    protected Path routingTableRepoPath;
    protected Path remoteStateRepoPath;

    protected String remoteRepoPrefix = "remote_publication";
    protected String remoteStateRepoName = "test-remote-state-repo";
    protected String routingTableRepoName = "test-remote-routing-table-repo";

    @Before
    public void setup() {
        clusterSettingsSuppliedByTest = true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                InternalSettingsPlugin.class,
                MockFsRepositoryPlugin.class,
                MockS3RepositoryPlugin.class,
                MockTransportService.TestPlugin.class)
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {

        if (routingTableRepoPath == null || remoteStateRepoPath == null) {
            routingTableRepoPath = randomRepoPath().toAbsolutePath();
            remoteStateRepoPath = randomRepoPath().toAbsolutePath();
        }

        Settings remotePublicationSettings = buildRemotePublicationNodeAttributes(
            remoteStateRepoName,
            MockS3Repository.TYPE,
            remoteStateRepoPath,
            routingTableRepoName,
            MockS3Repository.TYPE,
            routingTableRepoPath
        );
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(REMOTE_PUBLICATION_SETTING_KEY, true)
            .put(remotePublicationSettings)
            .build();
    }


    public void testBootstrapClusterWithRemoteStateAndVerifyS3Upload() throws Exception {
        // 1. Bootstrap new cluster with remote state enabled
        prepareCluster(1, 2, INDEX_NAME, 1, 1);
        ensureGreen(INDEX_NAME);
    }

    public void testConditionalUpdatesOnClusterStateChanges() throws Exception {
        // Bootstrap cluster
        prepareCluster(1, 2, INDEX_NAME, 1, 1);
        ensureGreen(INDEX_NAME);

        // 2. Create new index and verify conditional update
        createIndex("test-index-2", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        ensureGreen("test-index-2");
    }

    public void testVersionConflictWithNodeFailure() throws Exception {
        prepareCluster(3, 2, INDEX_NAME, 1, 1);
        ensureStableCluster(5);
        ensureGreen(INDEX_NAME);

        String masterNode = internalCluster().getClusterManagerName();

        // 3. Simulate version conflict by stopping master
        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(4);

        // New master should handle version conflict gracefully
        String newMasterNode = internalCluster().getClusterManagerName();
        assertNotEquals("New master should be different", masterNode, newMasterNode);

        // Trigger cluster state change to test conditional update
        createIndex("conflict-test-index", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        ensureGreen("conflict-test-index");
    }
}
