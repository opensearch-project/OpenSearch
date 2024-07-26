/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.remote.BlobPathParameters;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;
import static org.opensearch.gateway.remote.model.RemoteHashesOfConsistentSettings.HASHES_OF_CONSISTENT_SETTINGS;

public class RemoteHashesOfConsistentSettingsTests extends OpenSearchTestCase {
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
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings remoteObjectForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            clusterUUID,
            compressor
        );
        assertEquals(remoteObjectForUpload.clusterUUID(), clusterUUID);

        RemoteHashesOfConsistentSettings remoteObjectForDownload = new RemoteHashesOfConsistentSettings(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor
        );
        assertEquals(remoteObjectForDownload.clusterUUID(), clusterUUID);
    }

    public void testFullBlobName() {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings remoteObjectForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            clusterUUID,
            compressor
        );
        assertNull(remoteObjectForUpload.getFullBlobName());

        RemoteHashesOfConsistentSettings remoteObjectForDownload = new RemoteHashesOfConsistentSettings(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor
        );
        assertEquals(remoteObjectForDownload.getFullBlobName(), TEST_BLOB_NAME);
    }

    public void testBlobFileName() {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings remoteObjectForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            clusterUUID,
            compressor
        );
        assertNull(remoteObjectForUpload.getBlobFileName());

        RemoteHashesOfConsistentSettings remoteObjectForDownload = new RemoteHashesOfConsistentSettings(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor
        );
        assertEquals(remoteObjectForDownload.getBlobFileName(), TEST_BLOB_FILE_NAME);
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/hashes-of-consistent-settings";
        RemoteHashesOfConsistentSettings remoteObjectForDownload = new RemoteHashesOfConsistentSettings(
            uploadedFile,
            clusterUUID,
            compressor
        );
        assertArrayEquals(
            remoteObjectForDownload.getBlobPathTokens(),
            new String[] { "user", "local", "opensearch", "hashes-of-consistent-settings" }
        );
    }

    public void testBlobPathParameters() {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings remoteObjectForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            clusterUUID,
            compressor
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertEquals(params.getPathTokens(), List.of(GLOBAL_METADATA_PATH_TOKEN));
        assertEquals(params.getFilePrefix(), HASHES_OF_CONSISTENT_SETTINGS);
    }

    public void testGenerateBlobFileName() {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings remoteObjectForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            clusterUUID,
            compressor
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertEquals(nameTokens[0], HASHES_OF_CONSISTENT_SETTINGS);
        assertEquals(RemoteStoreUtils.invertLong(nameTokens[1]), METADATA_VERSION);
        assertTrue(RemoteStoreUtils.invertLong(nameTokens[2]) <= System.currentTimeMillis());
        assertEquals(nameTokens[3], String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION));
    }

    public void testGetUploadedMetadata() throws IOException {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings remoteObjectForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            clusterUUID,
            compressor
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);
        remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
        assertEquals(uploadedMetadata.getComponent(), HASHES_OF_CONSISTENT_SETTINGS);
        assertEquals(uploadedMetadata.getUploadedFilename(), remoteObjectForUpload.getFullBlobName());
    }

    public void testSerDe() throws IOException {
        DiffableStringMap hashesOfConsistentSettings = getHashesOfConsistentSettings();
        RemoteHashesOfConsistentSettings remoteObjectForUpload = new RemoteHashesOfConsistentSettings(
            hashesOfConsistentSettings,
            METADATA_VERSION,
            clusterUUID,
            compressor
        );
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertTrue(inputStream.available() > 0);
            DiffableStringMap readHashesOfConsistentSettings = remoteObjectForUpload.deserialize(inputStream);
            assertEquals(hashesOfConsistentSettings.entrySet(), readHashesOfConsistentSettings.entrySet());
        }
    }

    public static DiffableStringMap getHashesOfConsistentSettings() {
        Map<String, String> hashesOfConsistentSettings = new HashMap<>();
        hashesOfConsistentSettings.put("secure-setting-key", "secure-setting-value");
        return new DiffableStringMap(hashesOfConsistentSettings);
    }
}
