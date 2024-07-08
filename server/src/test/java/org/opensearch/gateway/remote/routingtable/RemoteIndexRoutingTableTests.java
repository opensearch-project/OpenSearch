/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE_PREFIX;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexRoutingTableTests extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final String INDEX_ROUTING_TABLE_TYPE = "test-index-routing-table";
    private static final long STATE_VERSION = 3L;
    private static final long STATE_TERM = 2L;
    private String clusterUUID;
    private BlobStoreTransferService blobStoreTransferService;
    private BlobStoreRepository blobStoreRepository;
    private String clusterName;
    private ClusterSettings clusterSettings;
    private Compressor compressor;
    private NamedWriteableRegistry namedWriteableRegistry;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        this.clusterUUID = "test-cluster-uuid";
        this.blobStoreTransferService = mock(BlobStoreTransferService.class);
        this.blobStoreRepository = mock(BlobStoreRepository.class);
        BlobPath blobPath = new BlobPath().add("/path");
        when(blobStoreRepository.basePath()).thenReturn(blobPath);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        compressor = new NoneCompressor();
        namedWriteableRegistry = writableRegistry();
        this.clusterName = "test-cluster-name";
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testClusterUUID() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );
            assertEquals(remoteObjectForUpload.clusterUUID(), clusterUUID);

            RemoteIndexRoutingTable remoteObjectForDownload = new RemoteIndexRoutingTable(TEST_BLOB_NAME, clusterUUID, compressor);
            assertEquals(remoteObjectForDownload.clusterUUID(), clusterUUID);
        });
    }

    public void testFullBlobName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );
            assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

            RemoteIndexRoutingTable remoteObjectForDownload = new RemoteIndexRoutingTable(TEST_BLOB_NAME, clusterUUID, compressor);
            assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
        });
    }

    public void testBlobFileName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );
            assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

            RemoteIndexRoutingTable remoteObjectForDownload = new RemoteIndexRoutingTable(TEST_BLOB_NAME, clusterUUID, compressor);
            assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
        });
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/routingTable";
        RemoteIndexRoutingTable remoteObjectForDownload = new RemoteIndexRoutingTable(uploadedFile, clusterUUID, compressor);
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "opensearch", "routingTable" }));
    }

    public void testBlobPathParameters() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );
            assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

            BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
            assertThat(params.getPathTokens(), is(List.of(indexRoutingTable.getIndex().getUUID())));
            String expectedPrefix = "";
            assertThat(params.getFilePrefix(), is(expectedPrefix));
        });
    }

    public void testGenerateBlobFileName() {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );

            String blobFileName = remoteObjectForUpload.generateBlobFileName();
            String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
            assertEquals(nameTokens[0], INDEX_ROUTING_TABLE_PREFIX);
            assertEquals(nameTokens[1], RemoteStoreUtils.invertLong(STATE_TERM));
            assertEquals(nameTokens[2], RemoteStoreUtils.invertLong(STATE_VERSION));
            assertThat(RemoteStoreUtils.invertLong(nameTokens[3]), lessThanOrEqualTo(System.currentTimeMillis()));
        });
    }

    public void testGetUploadedMetadata() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );

            assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

            try (InputStream inputStream = remoteObjectForUpload.serialize()) {
                remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
                ClusterMetadataManifest.UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
                String expectedPrefix = String.join(CUSTOM_DELIMITER, INDEX_ROUTING_TABLE, indexRoutingTable.getIndex().getName());
                assertThat(uploadedMetadata.getComponent(), is(expectedPrefix));
                assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testSerDe() throws IOException {
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards)
                    .numberOfReplicas(numberOfReplicas)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
                indexRoutingTable,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );

            assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

            try (InputStream inputStream = remoteObjectForUpload.serialize()) {
                remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
                assertThat(inputStream.available(), greaterThan(0));
                IndexRoutingTable readIndexRoutingTable = remoteObjectForUpload.deserialize(inputStream);
                assertEquals(readIndexRoutingTable, indexRoutingTable);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
