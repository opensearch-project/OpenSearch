/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.gateway.remote.model.RemoteGlobalMetadata.GLOBAL_METADATA_FORMAT;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteGlobalMetadataTests extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long TERM = 3L;
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
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/opensearch/globalMetadata";
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            uploadedFile,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThat(remoteObjectForDownload.getBlobPathTokens(), is(new String[] { "user", "local", "opensearch", "globalMetadata" }));
    }

    public void testBlobPathParameters() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(UnsupportedOperationException.class, remoteObjectForDownload::getBlobPathParameters);
    }

    public void testGenerateBlobFileName() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(UnsupportedOperationException.class, remoteObjectForDownload::generateBlobFileName);
    }

    public void testGetUploadedMetadata() {
        RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(UnsupportedOperationException.class, remoteObjectForDownload::getUploadedMetadata);
    }

    public void testSerDe() throws IOException {
        Metadata globalMetadata = getGlobalMetadata();
        try (
            InputStream inputStream = GLOBAL_METADATA_FORMAT.serialize(
                globalMetadata,
                TEST_BLOB_FILE_NAME,
                compressor,
                RemoteClusterStateUtils.FORMAT_PARAMS
            ).streamInput()
        ) {
            RemoteGlobalMetadata remoteObjectForDownload = new RemoteGlobalMetadata(
                TEST_BLOB_NAME,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            assertThat(inputStream.available(), greaterThan(0));
            Metadata readglobalMetadata = remoteObjectForDownload.deserialize(inputStream);
            assertTrue(Metadata.isGlobalStateEquals(readglobalMetadata, globalMetadata));
        }
    }

    private Metadata getGlobalMetadata() {
        return Metadata.builder()
            .templates(
                TemplatesMetadata.builder()
                    .put(
                        IndexTemplateMetadata.builder("template" + randomAlphaOfLength(3))
                            .patterns(Arrays.asList("bar-*", "foo-*"))
                            .settings(
                                Settings.builder()
                                    .put("index.random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5))
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .coordinationMetadata(
                CoordinationMetadata.builder()
                    .term(TERM)
                    .lastAcceptedConfiguration(new VotingConfiguration(Set.of("node1")))
                    .lastCommittedConfiguration(new VotingConfiguration(Set.of("node1")))
                    .addVotingConfigExclusion(new VotingConfigExclusion("node2", " node-2"))
                    .build()
            )
            .build();
    }
}
