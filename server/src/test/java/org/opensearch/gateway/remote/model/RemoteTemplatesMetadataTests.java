/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
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
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteTemplatesMetadataTests extends OpenSearchTestCase {
    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long METADATA_VERSION = 3L;
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
        compressor = new NoneCompressor();
        namedXContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        this.clusterName = "test-cluster-name";
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testClusterUUID() {
        TemplatesMetadata settings = getTemplatesMetadata();
        RemoteTemplatesMetadata remoteObjectForUpload = new RemoteTemplatesMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteTemplatesMetadata remoteObjectForDownload = new RemoteTemplatesMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        TemplatesMetadata settings = getTemplatesMetadata();
        RemoteTemplatesMetadata remoteObjectForUpload = new RemoteTemplatesMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteTemplatesMetadata remoteObjectForDownload = new RemoteTemplatesMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        TemplatesMetadata settings = getTemplatesMetadata();
        RemoteTemplatesMetadata remoteObjectForUpload = new RemoteTemplatesMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteTemplatesMetadata remoteObjectForDownload = new RemoteTemplatesMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/settings";
        RemoteTemplatesMetadata remoteObjectForDownload = new RemoteTemplatesMetadata(
            uploadedFile,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "opensearch", "settings" }));
    }

    public void testBlobPathParameters() {
        TemplatesMetadata settings = getTemplatesMetadata();
        RemoteTemplatesMetadata remoteObjectForUpload = new RemoteTemplatesMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN)));
        assertThat(params.getFilePrefix(), is(RemoteTemplatesMetadata.TEMPLATES_METADATA));
    }

    public void testGenerateBlobFileName() {
        TemplatesMetadata settings = getTemplatesMetadata();
        RemoteTemplatesMetadata remoteObjectForUpload = new RemoteTemplatesMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0], is(RemoteTemplatesMetadata.TEMPLATES_METADATA));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(METADATA_VERSION));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[3], is(String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)));

    }

    public void testGetUploadedMetadata() throws IOException {
        TemplatesMetadata settings = getTemplatesMetadata();
        RemoteTemplatesMetadata remoteObjectForUpload = new RemoteTemplatesMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertThat(uploadedMetadata.getComponent(), is(RemoteTemplatesMetadata.TEMPLATES_METADATA));
            assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        TemplatesMetadata settings = getTemplatesMetadata();
        RemoteTemplatesMetadata remoteObjectForUpload = new RemoteTemplatesMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertThat(inputStream.available(), greaterThan(0));
            TemplatesMetadata readsettings = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readsettings, is(settings));
        }
    }

    public static TemplatesMetadata getTemplatesMetadata() {
        return TemplatesMetadata.builder()
            .put(
                IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                    .order(1234)
                    .patterns(Arrays.asList("bar-*", "foo-*"))
                    .settings(
                        Settings.builder().put("index.random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build()
                    )
                    .build()
            )
            .put(
                IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                    .order(5678)
                    .patterns(Arrays.asList("test-*"))
                    .settings(
                        Settings.builder().put("index.random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build()
                    )
                    .build()
            )
            .build();
    }
}
