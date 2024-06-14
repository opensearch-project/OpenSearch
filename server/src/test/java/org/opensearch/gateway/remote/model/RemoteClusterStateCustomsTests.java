/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.ClusterState.Custom;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.opensearch.Version.CURRENT;
import static org.opensearch.cluster.SnapshotsInProgress.State.INIT;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.model.RemoteClusterStateCustoms.CLUSTER_STATE_CUSTOM;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClusterStateCustomsTests extends OpenSearchTestCase {
    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final String CUSTOM_TYPE = "test-custom";
    private static final long STATE_VERSION = 3L;
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
        Custom clusterStateCustoms = getClusterStateCustom();
        RemoteClusterStateCustoms remoteObjectForUpload = new RemoteClusterStateCustoms(
            clusterStateCustoms,
            "test-custom",
            STATE_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteClusterStateCustoms remoteObjectForDownload = new RemoteClusterStateCustoms(
            TEST_BLOB_NAME,
            "test-custom",
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        Custom clusterStateCustoms = getClusterStateCustom();
        RemoteClusterStateCustoms remoteObjectForUpload = new RemoteClusterStateCustoms(
            clusterStateCustoms,
            "test-custom",
            STATE_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteClusterStateCustoms remoteObjectForDownload = new RemoteClusterStateCustoms(
            TEST_BLOB_NAME,
            "test-custom",
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        Custom clusterStateCustoms = getClusterStateCustom();
        RemoteClusterStateCustoms remoteObjectForUpload = new RemoteClusterStateCustoms(
            clusterStateCustoms,
            "test-custom",
            STATE_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteClusterStateCustoms remoteObjectForDownload = new RemoteClusterStateCustoms(
            TEST_BLOB_NAME,
            "test-custom",
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/clusterStateCustoms";
        RemoteClusterStateCustoms remoteObjectForDownload = new RemoteClusterStateCustoms(
            uploadedFile,
            "test-custom",
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "opensearch", "clusterStateCustoms" }));
    }

    public void testBlobPathParameters() {
        Custom clusterStateCustoms = getClusterStateCustom();
        RemoteClusterStateCustoms remoteObjectForUpload = new RemoteClusterStateCustoms(
            clusterStateCustoms,
            "test-custom",
            STATE_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(RemoteClusterStateUtils.CLUSTER_STATE_EPHEMERAL_PATH_TOKEN)));
        String expectedPrefix = String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, "test-custom");
        assertThat(params.getFilePrefix(), is(expectedPrefix));
    }

    public void testGenerateBlobFileName() {
        Custom clusterStateCustoms = getClusterStateCustom();
        RemoteClusterStateCustoms remoteObjectForUpload = new RemoteClusterStateCustoms(
            clusterStateCustoms,
            "test-custom",
            STATE_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        String expectedPrefix = String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, "test-custom");
        assertThat(nameTokens[0], is(expectedPrefix));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(STATE_VERSION));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[3], is(String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)));

    }

    public void testGetUploadedMetadata() throws IOException {
        Custom clusterStateCustoms = getClusterStateCustom();
        RemoteClusterStateCustoms remoteObjectForUpload = new RemoteClusterStateCustoms(
            clusterStateCustoms,
            "test-custom",
            STATE_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            String expectedPrefix = String.join(CUSTOM_DELIMITER, CLUSTER_STATE_CUSTOM, "test-custom");
            assertThat(uploadedMetadata.getComponent(), is(expectedPrefix));
            assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        Custom clusterStateCustoms = getClusterStateCustom();
        RemoteClusterStateCustoms remoteObjectForUpload = new RemoteClusterStateCustoms(
            clusterStateCustoms,
            SnapshotsInProgress.TYPE,
            STATE_VERSION,
            clusterUUID,
            compressor,
            namedWriteableRegistry
        );
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertThat(inputStream.available(), greaterThan(0));
            Custom readClusterStateCustoms = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readClusterStateCustoms, is(clusterStateCustoms));
        }
    }

    public static SnapshotsInProgress getClusterStateCustom() {
        return SnapshotsInProgress.of(
            List.of(
                new SnapshotsInProgress.Entry(
                    new Snapshot("repo", new SnapshotId("test-snapshot", "test-snapshot-uuid")),
                    false,
                    false,
                    INIT,
                    emptyList(),
                    emptyList(),
                    0L,
                    0L,
                    emptyMap(),
                    emptyMap(),
                    CURRENT,
                    false
                )
            )
        );
    }
}
