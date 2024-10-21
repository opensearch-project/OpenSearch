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
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V0;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V1;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V2;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClusterMetadataManifestTests extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";

    private static final long TERM = 2L;
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
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(
            manifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(
            manifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(
            manifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/manifest";
        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(
            uploadedFile,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "opensearch", "manifest" }));
    }

    public void testBlobPathParameters() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(
            manifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(MANIFEST)));
        assertThat(params.getFilePrefix(), is(RemoteClusterMetadataManifest.MANIFEST));
    }

    public void testGenerateBlobFileName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(
            manifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0], is(RemoteClusterMetadataManifest.MANIFEST));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(TERM));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), is(VERSION));
        assertThat(nameTokens[3], is("C"));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[4]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[5], is(String.valueOf(MANIFEST_CURRENT_CODEC_VERSION)));
    }

    public void testGetUploadedMetadata() throws IOException {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(
            manifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertThat(uploadedMetadata.getComponent(), is(MANIFEST));
            assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        for (int codecVersion : ClusterMetadataManifest.CODEC_VERSIONS) {
            ClusterMetadataManifest manifest = getClusterMetadataManifestForCodecVersion(codecVersion);
            RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(
                manifest,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            try (InputStream inputStream = remoteObjectForUpload.serialize()) {
                remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
                assertThat(inputStream.available(), greaterThan(0));
                ClusterMetadataManifest readManifest = remoteObjectForUpload.deserialize(inputStream);
                validateManifest(manifest, readManifest);
            }
        }

        String blobName = "/usr/local/manifest__1__2__3__4__5__6";
        RemoteClusterMetadataManifest invalidRemoteObject = new RemoteClusterMetadataManifest(
            blobName,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(IllegalArgumentException.class, () -> invalidRemoteObject.deserialize(new ByteArrayInputStream(new byte[0])));
    }

    public void testGetManifestCodecVersion() {
        String manifestFileWithDelimiterInPath =
            "123456789012_test-cluster/cluster-state/dsgYj10__Nkso7/manifest/manifest__9223372036854775806__9223372036854775804__C__9223370319103329556__2";
        RemoteClusterMetadataManifest remoteManifestForDownload = new RemoteClusterMetadataManifest(
            manifestFileWithDelimiterInPath,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(CODEC_V2, remoteManifestForDownload.getManifestCodecVersion());

        String v0ManifestFileWithDelimiterInPath =
            "123456789012_test-cluster/cluster-state/dsgYj10__Nkso7/manifest/manifest__9223372036854775806__9223372036854775804__C__9223370319103329556";
        RemoteClusterMetadataManifest remoteManifestV0ForDownload = new RemoteClusterMetadataManifest(
            v0ManifestFileWithDelimiterInPath,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(CODEC_V0, remoteManifestV0ForDownload.getManifestCodecVersion());
    }

    private ClusterMetadataManifest getClusterMetadataManifest() {
        return getClusterMetadataManifestForCodecVersion(MANIFEST_CURRENT_CODEC_VERSION);
    }

    private ClusterMetadataManifest getClusterMetadataManifestForCodecVersion(int codecVersion) {
        ClusterMetadataManifest.Builder builder = ClusterMetadataManifest.builder();
        builder.opensearchVersion(Version.CURRENT)
            .codecVersion(codecVersion)
            .nodeId("test-node")
            .clusterUUID("test-uuid")
            .previousClusterUUID("_NA_")
            .stateUUID("state-uuid")
            .clusterTerm(TERM)
            .stateVersion(VERSION)
            .committed(true)
            .coordinationMetadata(new UploadedMetadataAttribute("test-attr", "uploaded-file"))
            .indices(List.of(new UploadedIndexMetadata("test-index", "tst-idx", "uploaded-index-file", codecVersion)))
            .clusterUUIDCommitted(true);

        if (codecVersion == CODEC_V1) {
            builder.globalMetadataFileName("global-metadata-file-name");
        }
        if (codecVersion >= CODEC_V2) {
            builder.coordinationMetadata(new UploadedMetadataAttribute("uploaded_coordination", "coordination-metadata-file"));
            builder.settingMetadata(new UploadedMetadataAttribute("uploaded_settings", "settings-metadata-file"));
            builder.templatesMetadata(new UploadedMetadataAttribute("uploaded_templates", "templates-metadata-file"));
            builder.customMetadataMap(Map.of("uploaded_custom", new UploadedMetadataAttribute("uploaded_custom", "custom-metadata-file")));
            builder.routingTableVersion(1L);
            builder.indicesRouting(List.of(new UploadedIndexMetadata("test-index", "tst-idx", "uploaded_routing", "routing--")));
            builder.discoveryNodesMetadata(new UploadedMetadataAttribute("uploaded_discovery", "discovery-metadata-file"));
            builder.clusterBlocksMetadata(new UploadedMetadataAttribute("uploaded_blocks", "blocks-metadata-file"));
            builder.metadataVersion(1L);
            builder.transientSettingsMetadata(
                new UploadedMetadataAttribute("uploaded_transient_settings", "transient-settings-metadata-file")
            );
            builder.hashesOfConsistentSettings(new UploadedMetadataAttribute("uploaded_hashes_settings", "hashes-settings-metadata-file"));
            builder.clusterStateCustomMetadataMap(
                Map.of("uploaded_custom", new UploadedMetadataAttribute("uploaded_custom", "custom-metadata-file"))
            );
        }

        return builder.build();
    }

    private void validateManifest(ClusterMetadataManifest writeManifest, ClusterMetadataManifest readManifest) {
        assertThat(readManifest.getOpensearchVersion(), is(writeManifest.getOpensearchVersion()));
        assertThat(readManifest.getCodecVersion(), is(writeManifest.getCodecVersion()));
        assertThat(readManifest.getNodeId(), is(writeManifest.getNodeId()));
        assertThat(readManifest.getClusterUUID(), is(writeManifest.getClusterUUID()));
        assertThat(readManifest.getPreviousClusterUUID(), is(writeManifest.getPreviousClusterUUID()));
        assertThat(readManifest.getStateUUID(), is(writeManifest.getStateUUID()));
        assertThat(readManifest.getClusterTerm(), is(writeManifest.getClusterTerm()));
        assertThat(readManifest.getStateVersion(), is(writeManifest.getStateVersion()));
        assertThat(readManifest.isCommitted(), is(writeManifest.isCommitted()));
        assertThat(readManifest.getPreviousClusterUUID(), is(writeManifest.getPreviousClusterUUID()));
        assertThat(readManifest.isClusterUUIDCommitted(), is(writeManifest.isClusterUUIDCommitted()));
        assertThat(readManifest.getIndices(), is(writeManifest.getIndices()));
        if (writeManifest.getCodecVersion() == CODEC_V1) {
            assertThat(readManifest.getGlobalMetadataFileName(), is(writeManifest.getGlobalMetadataFileName()));
        }
        if (writeManifest.getCodecVersion() >= CODEC_V2) {
            assertThat(readManifest.getCoordinationMetadata(), is(writeManifest.getCoordinationMetadata()));
            assertThat(readManifest.getSettingsMetadata(), is(writeManifest.getSettingsMetadata()));
            assertThat(readManifest.getTemplatesMetadata(), is(writeManifest.getTemplatesMetadata()));
            assertThat(readManifest.getCustomMetadataMap(), is(writeManifest.getCustomMetadataMap()));
            assertThat(readManifest.getRoutingTableVersion(), is(writeManifest.getRoutingTableVersion()));
            assertThat(readManifest.getIndicesRouting(), is(writeManifest.getIndicesRouting()));
            assertThat(readManifest.getDiscoveryNodesMetadata(), is(writeManifest.getDiscoveryNodesMetadata()));
            assertThat(readManifest.getClusterBlocksMetadata(), is(writeManifest.getClusterBlocksMetadata()));
            assertThat(readManifest.getMetadataVersion(), is(writeManifest.getMetadataVersion()));
            assertThat(readManifest.getDiffManifest(), is(writeManifest.getDiffManifest()));
            assertThat(readManifest.getTransientSettingsMetadata(), is(writeManifest.getTransientSettingsMetadata()));
            assertThat(readManifest.getHashesOfConsistentSettings(), is(writeManifest.getHashesOfConsistentSettings()));
            assertThat(readManifest.getClusterStateCustomMap(), is(writeManifest.getClusterStateCustomMap()));
        }
    }
}
