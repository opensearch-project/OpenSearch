/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.indices.IndicesModule;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;

public class RemoteTransientSettingsMetadataTests extends OpenSearchTestCase {
    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long METADATA_VERSION = 3L;
    private String clusterUUID;
    private Compressor compressor;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void setup() {
        this.clusterUUID = "test-cluster-uuid";
        compressor = new NoneCompressor();
        namedXContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
    }

    public void testClusterUUID() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(remoteObjectForUpload.clusterUUID(), clusterUUID);

        RemoteTransientSettingsMetadata remoteObjectForDownload = new RemoteTransientSettingsMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(remoteObjectForDownload.clusterUUID(), clusterUUID);
    }

    public void testFullBlobName() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertNull(remoteObjectForUpload.getFullBlobName());

        RemoteTransientSettingsMetadata remoteObjectForDownload = new RemoteTransientSettingsMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(remoteObjectForDownload.getFullBlobName(), TEST_BLOB_NAME);
    }

    public void testBlobFileName() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertNull(remoteObjectForUpload.getBlobFileName());

        RemoteTransientSettingsMetadata remoteObjectForDownload = new RemoteTransientSettingsMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(remoteObjectForDownload.getBlobFileName(), TEST_BLOB_FILE_NAME);
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/settings";
        RemoteTransientSettingsMetadata remoteObjectForDownload = new RemoteTransientSettingsMetadata(
            uploadedFile,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertArrayEquals(remoteObjectForDownload.getBlobPathTokens(), new String[] { "user", "local", "opensearch", "settings" });
    }

    public void testBlobPathParameters() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertEquals(params.getPathTokens(), List.of(RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN));
        assertEquals(params.getFilePrefix(), TRANSIENT_SETTING_METADATA);
    }

    public void testGenerateBlobFileName() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertEquals(nameTokens[0], TRANSIENT_SETTING_METADATA);
        assertEquals(RemoteStoreUtils.invertLong(nameTokens[1]), METADATA_VERSION);
        assertTrue(RemoteStoreUtils.invertLong(nameTokens[2]) <= System.currentTimeMillis());
        assertEquals(nameTokens[3], String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION));

    }

    public void testGetUploadedMetadata() throws IOException {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            ClusterMetadataManifest.UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertEquals(uploadedMetadata.getComponent(), TRANSIENT_SETTING_METADATA);
            assertEquals(uploadedMetadata.getUploadedFilename(), remoteObjectForUpload.getFullBlobName());
        }
    }

    public void testSerDe() throws IOException {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertTrue(inputStream.available() > 0);
            Settings readsettings = remoteObjectForUpload.deserialize(inputStream);
            assertEquals(readsettings, settings);
        }
    }

    private Settings getSettings() {
        return Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build();
    }
}
