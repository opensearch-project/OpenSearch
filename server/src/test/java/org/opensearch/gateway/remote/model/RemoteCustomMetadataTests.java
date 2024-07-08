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
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.persistent.PersistentTaskParams;
import org.opensearch.persistent.PersistentTasksCustomMetadata;
import org.opensearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_METADATA;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteCustomMetadataTests extends OpenSearchTestCase {
    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final String CUSTOM_TYPE = "test-custom";
    private static final long METADATA_VERSION = 3L;
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
        Custom customMetadata = getCustomMetadata();
        RemoteCustomMetadata remoteObjectForUpload = new RemoteCustomMetadata(
            customMetadata,
            "test-custom",
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteCustomMetadata remoteObjectForDownload = new RemoteCustomMetadata(
            TEST_BLOB_NAME,
            "test-custom",
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        Custom customMetadata = getCustomMetadata();
        RemoteCustomMetadata remoteObjectForUpload = new RemoteCustomMetadata(
            customMetadata,
            "test-custom",
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteCustomMetadata remoteObjectForDownload = new RemoteCustomMetadata(
            TEST_BLOB_NAME,
            "test-custom",
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        Custom customMetadata = getCustomMetadata();
        RemoteCustomMetadata remoteObjectForUpload = new RemoteCustomMetadata(
            customMetadata,
            "test-custom",
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteCustomMetadata remoteObjectForDownload = new RemoteCustomMetadata(
            TEST_BLOB_NAME,
            "test-custom",
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/customMetadata";
        RemoteCustomMetadata remoteObjectForDownload = new RemoteCustomMetadata(
            uploadedFile,
            "test-custom",
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "opensearch", "customMetadata" }));
    }

    public void testBlobPathParameters() {
        Custom customMetadata = getCustomMetadata();
        RemoteCustomMetadata remoteObjectForUpload = new RemoteCustomMetadata(
            customMetadata,
            "test-custom",
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN)));
        String expectedPrefix = CUSTOM_METADATA + CUSTOM_DELIMITER + CUSTOM_TYPE;
        assertThat(params.getFilePrefix(), is(expectedPrefix));
    }

    public void testGenerateBlobFileName() {
        Custom customMetadata = getCustomMetadata();
        RemoteCustomMetadata remoteObjectForUpload = new RemoteCustomMetadata(
            customMetadata,
            "test-custom",
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        String expectedPrefix = CUSTOM_METADATA + CUSTOM_DELIMITER + CUSTOM_TYPE;
        assertThat(nameTokens[0], is(expectedPrefix));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(METADATA_VERSION));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[3], is(String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)));

    }

    public void testGetUploadedMetadata() throws IOException {
        Custom customMetadata = getCustomMetadata();
        RemoteCustomMetadata remoteObjectForUpload = new RemoteCustomMetadata(
            customMetadata,
            "test-custom",
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            String expectedPrefix = CUSTOM_METADATA + CUSTOM_DELIMITER + CUSTOM_TYPE;
            assertThat(uploadedMetadata.getComponent(), is(expectedPrefix));
            assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        Custom customMetadata = getCustomMetadata();
        verifySerDe(customMetadata, IndexGraveyard.TYPE);
    }

    public void testSerDeForPersistentTasks() throws IOException {
        Custom customMetadata = getPersistentTasksMetadata();
        verifySerDe(customMetadata, PersistentTasksCustomMetadata.TYPE);
    }

    private void verifySerDe(Custom objectToUpload, String objectType) throws IOException {
        RemoteCustomMetadata remoteObjectForUpload = new RemoteCustomMetadata(
            objectToUpload,
            objectType,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            customWritableRegistry()
        );
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertThat(inputStream.available(), greaterThan(0));
            Custom readCustomMetadata = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readCustomMetadata, is(objectToUpload));
        }
    }

    private NamedWriteableRegistry customWritableRegistry() {
        List<Entry> entries = ClusterModule.getNamedWriteables();
        entries.add(new Entry(PersistentTaskParams.class, TestPersistentTaskParams.PARAM_NAME, TestPersistentTaskParams::new));
        return new NamedWriteableRegistry(entries);
    }

    public static Custom getCustomMetadata() {
        return IndexGraveyard.builder().addTombstone(new Index("test-index", "3q2423")).build();
    }

    private static Custom getPersistentTasksMetadata() {
        return PersistentTasksCustomMetadata.builder()
            .addTask("_task_1", "testTaskName", new TestPersistentTaskParams("task param data"), new Assignment(null, "_reason"))
            .build();
    }

    public static class TestPersistentTaskParams implements PersistentTaskParams {

        private static final String PARAM_NAME = "testTaskName";

        private final String data;

        public TestPersistentTaskParams(String data) {
            this.data = data;
        }

        public TestPersistentTaskParams(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public String getWriteableName() {
            return PARAM_NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_2_13_0;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(data);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("data_field", data);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestPersistentTaskParams that = (TestPersistentTaskParams) o;
            return Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }
    }

}
