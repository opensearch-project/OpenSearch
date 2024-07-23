/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.RoutingTableIncrementalDiff;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.gateway.remote.routingtable.RemoteRoutingTableDiff.ROUTING_TABLE_DIFF_FILE;
import static org.opensearch.gateway.remote.routingtable.RemoteRoutingTableDiff.ROUTING_TABLE_DIFF_METADATA_PREFIX;
import static org.opensearch.gateway.remote.routingtable.RemoteRoutingTableDiff.ROUTING_TABLE_DIFF_PATH_TOKEN;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexRoutingTableDiffTests extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long STATE_VERSION = 3L;
    private static final long STATE_TERM = 2L;
    private String clusterUUID;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private ClusterSettings clusterSettings;
    private Compressor compressor;

    private String clusterName;
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
        Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex()).initializeAsNew(indexMetadata).build();

        diffs.put(indexName, indexRoutingTable.diff(indexRoutingTable));

        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(diffs);

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );
        assertEquals(remoteDiffForUpload.clusterUUID(), clusterUUID);

        RemoteRoutingTableDiff remoteDiffForDownload = new RemoteRoutingTableDiff(TEST_BLOB_NAME, clusterUUID, compressor);
        assertEquals(remoteDiffForDownload.clusterUUID(), clusterUUID);
    }

    public void testFullBlobName() {
        Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex()).initializeAsNew(indexMetadata).build();

        diffs.put(indexName, indexRoutingTable.diff(indexRoutingTable));
        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(diffs);

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );
        assertThat(remoteDiffForUpload.getFullBlobName(), nullValue());

        RemoteRoutingTableDiff remoteDiffForDownload = new RemoteRoutingTableDiff(TEST_BLOB_NAME, clusterUUID, compressor);
        assertThat(remoteDiffForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex()).initializeAsNew(indexMetadata).build();

        diffs.put(indexName, indexRoutingTable.diff(indexRoutingTable));
        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(diffs);

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );
        assertThat(remoteDiffForUpload.getBlobFileName(), nullValue());

        RemoteRoutingTableDiff remoteDiffForDownload = new RemoteRoutingTableDiff(TEST_BLOB_NAME, clusterUUID, compressor);
        assertThat(remoteDiffForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathParameters() {
        Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex()).initializeAsNew(indexMetadata).build();

        diffs.put(indexName, indexRoutingTable.diff(indexRoutingTable));
        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(diffs);

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );
        assertThat(remoteDiffForUpload.getBlobFileName(), nullValue());

        BlobPathParameters params = remoteDiffForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(ROUTING_TABLE_DIFF_PATH_TOKEN)));
        String expectedPrefix = ROUTING_TABLE_DIFF_METADATA_PREFIX;
        assertThat(params.getFilePrefix(), is(expectedPrefix));
    }

    public void testGenerateBlobFileName() {
        Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex()).initializeAsNew(indexMetadata).build();

        diffs.put(indexName, indexRoutingTable.diff(indexRoutingTable));
        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(diffs);

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );

        String blobFileName = remoteDiffForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split("__");
        assertEquals(ROUTING_TABLE_DIFF_METADATA_PREFIX, nameTokens[0]);
        assertEquals(RemoteStoreUtils.invertLong(STATE_TERM), nameTokens[1]);
        assertEquals(RemoteStoreUtils.invertLong(STATE_VERSION), nameTokens[2]);
        assertThat(RemoteStoreUtils.invertLong(nameTokens[3]), lessThanOrEqualTo(System.currentTimeMillis()));
    }

    public void testGetUploadedMetadata() throws IOException {
        Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();
        String indexName = randomAlphaOfLength(randomIntBetween(1, 50));
        int numberOfShards = randomIntBetween(1, 10);
        int numberOfReplicas = randomIntBetween(1, 10);

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex()).initializeAsNew(indexMetadata).build();

        diffs.put(indexName, indexRoutingTable.diff(indexRoutingTable));
        RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(diffs);

        RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
            routingTableIncrementalDiff,
            clusterUUID,
            compressor,
            STATE_TERM,
            STATE_VERSION
        );

        remoteDiffForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
        ClusterMetadataManifest.UploadedMetadata uploadedMetadataAttribute = remoteDiffForUpload.getUploadedMetadata();
        assertEquals(ROUTING_TABLE_DIFF_FILE, uploadedMetadataAttribute.getComponent());
    }

    public void testStreamOperations() throws IOException {
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
        Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();

        initialRoutingTable.getIndicesRouting().values().forEach(indexRoutingTable -> {
            diffs.put(indexName, indexRoutingTable.diff(indexRoutingTable));
            RoutingTableIncrementalDiff routingTableIncrementalDiff = new RoutingTableIncrementalDiff(diffs);

            RemoteRoutingTableDiff remoteDiffForUpload = new RemoteRoutingTableDiff(
                routingTableIncrementalDiff,
                clusterUUID,
                compressor,
                STATE_TERM,
                STATE_VERSION
            );

            assertThrows(AssertionError.class, remoteDiffForUpload::getUploadedMetadata);

            try (InputStream inputStream = remoteDiffForUpload.serialize()) {
                remoteDiffForUpload.setFullBlobName(BlobPath.cleanPath());
                assertThat(inputStream.available(), greaterThan(0));

                routingTableIncrementalDiff = remoteDiffForUpload.deserialize(inputStream);
                assertEquals(remoteDiffForUpload.getDiffs().size(), routingTableIncrementalDiff.getDiffs().size());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
