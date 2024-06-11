/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.remote.RemoteWritableEntityStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.gateway.remote.model.RemoteIndexMetadata;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.when;
import static org.opensearch.threadpool.ThreadPool.Names.REMOTE_STATE_READ;
import static org.mockito.Mockito.mock;

public class RemoteIndexMetadataManagerTests extends OpenSearchTestCase {
    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private BlobStoreRepository blobStoreRepository;
    private ClusterSettings clusterSettings;
    private BlobStoreTransferService blobStoreTransferService;
    private ThreadPool threadPool;

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        threadPool = new TestThreadPool("test");
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        Compressor compressor = new NoneCompressor();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(
            clusterSettings,
            "cluster-name",
            blobStoreRepository,
            blobStoreTransferService,
            threadPool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testIndexMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteIndexMetadataManager.INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteIndexMetadataManager.getIndexMetadataUploadTimeout()
        );

        // verify update index metadata upload timeout
        int indexMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.index_metadata.upload_timeout", indexMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(indexMetadataUploadTimeout, remoteIndexMetadataManager.getIndexMetadataUploadTimeout().seconds());
    }
}
