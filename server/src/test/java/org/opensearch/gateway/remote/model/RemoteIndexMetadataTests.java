/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX_METADATA_CURRENT_CODEC_VERSION;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexMetadataTests extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long VERSION = 5L;

    private String clusterUUID;
    private BlobStoreTransferService blobStoreTransferService;
    private BlobStoreRepository blobStoreRepository;
    private String clusterName;
    private ClusterSettings clusterSettings;
    private Compressor compressor;
    private NamedXContentRegistry namedXContentRegistry;
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
        this.clusterName = "test-cluster-name";
        compressor = new NoneCompressor();
        namedXContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testClusterUUID() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, compressor, namedXContentRegistry);
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, compressor, namedXContentRegistry);
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, compressor, namedXContentRegistry);
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathParameters() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, compressor, namedXContentRegistry);
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(INDEX, indexMetadata.getIndexUUID())));
        assertThat(params.getFilePrefix(), is("metadata"));
    }

    public void testGenerateBlobFileName() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, compressor, namedXContentRegistry);
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0], is("metadata"));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(VERSION));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[3], is(String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION)));
    }

    public void testGetUploadedMetadata() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, compressor, namedXContentRegistry);
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);
        remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
        UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
        assertEquals(uploadedMetadata.getUploadedFilename(), remoteObjectForUpload.getFullBlobName());
    }

    public void testSerDe() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, compressor, namedXContentRegistry);
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            assertThat(inputStream.available(), greaterThan(0));
            IndexMetadata readIndexMetadata = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readIndexMetadata, is(indexMetadata));
        }
    }

    public void testPrefixedPath() {
        IndexMetadata indexMetadata = getIndexMetadata();
        String fixedPrefix = "*";
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(
            indexMetadata,
            clusterUUID,
            compressor,
            namedXContentRegistry,
            PathType.HASHED_PREFIX,
            PathHashAlgorithm.FNV_1A_COMPOSITE_1,
            fixedPrefix
        );
        String testPath = "test-path";
        String expectedPath = fixedPrefix + "410100110100101/test-path/index-uuid/";
        BlobPath prefixedPath = remoteObjectForUpload.getPrefixedPath(BlobPath.cleanPath().add(testPath));
        assertThat(prefixedPath.buildAsString(), is(expectedPath));

    }

    private IndexMetadata getIndexMetadata() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .version(VERSION)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
