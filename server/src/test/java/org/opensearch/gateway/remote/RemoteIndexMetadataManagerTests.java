/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.TestCapturingListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.gateway.remote.model.RemoteIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.gateway.remote.RemoteClusterStateService.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX_METADATA_FORMAT;
import static org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexMetadataManagerTests extends OpenSearchTestCase {

    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private Compressor compressor;
    private ClusterSettings clusterSettings;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        BlobPath blobPath = new BlobPath().add("random-path");
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        blobStoreTransferService = mock(BlobStoreTransferService.class);
        compressor = new NoneCompressor();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(
            clusterSettings,
            "test-cluster",
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

    public void testGetAsyncWriteRunnable_Success() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        BlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);
        String expectedFilePrefix = String.join(DELIMITER, "metadata", RemoteStoreUtils.invertLong(indexMetadata.getVersion()));

        doAnswer((invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        })).when(blobStoreTransferService).uploadBlob(any(), any(), any(), eq(WritePriority.URGENT), any(ActionListener.class));

        remoteIndexMetadataManager.writeAsync(
            INDEX,
            new RemoteIndexMetadata(indexMetadata, "cluster-uuid", compressor, null, null, null, null),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();

        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = listener.getResult();
        assertEquals(INDEX + "--" + indexMetadata.getIndex().getName(), uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(7, pathTokens.length);
        assertEquals(INDEX, pathTokens[4]);
        assertEquals(indexMetadata.getIndex().getUUID(), pathTokens[5]);
        assertTrue(pathTokens[6].startsWith(expectedFilePrefix));
    }

    public void testGetAsyncWriteRunnable_IOFailure() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        BlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        TestCapturingListener<ClusterMetadataManifest.UploadedMetadata> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        doAnswer((invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onFailure(new IOException("failure"));
            return null;
        })).when(blobStoreTransferService).uploadBlob(any(), any(), any(), eq(WritePriority.URGENT), any(ActionListener.class));

        remoteIndexMetadataManager.writeAsync(
            INDEX,
            new RemoteIndexMetadata(indexMetadata, "cluster-uuid", compressor, null, null, null, null),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getResult());
        assertNotNull(listener.getFailure());
        assertTrue(listener.getFailure() instanceof RemoteStateTransferException);
    }

    public void testGetAsyncReadRunnable_Success() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        String fileName = randomAlphaOfLength(10);
        fileName = fileName + DELIMITER + '2';
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            INDEX_METADATA_FORMAT.serialize(indexMetadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteIndexMetadataManager.readAsync(
            INDEX,
            new RemoteIndexMetadata(fileName, "cluster-uuid", compressor, null),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getFailure());
        assertNotNull(listener.getResult());
        assertEquals(indexMetadata, listener.getResult().getObj());
    }

    public void testGetAsyncReadRunnable_IOFailure() throws Exception {
        String fileName = randomAlphaOfLength(10);
        fileName = fileName + DELIMITER + '2';
        Exception exception = new IOException("testing failure");
        doThrow(exception).when(blobStoreTransferService).downloadBlob(anyIterable(), anyString());
        TestCapturingListener<RemoteReadResult> listener = new TestCapturingListener<>();
        CountDownLatch latch = new CountDownLatch(1);

        remoteIndexMetadataManager.readAsync(
            INDEX,
            new RemoteIndexMetadata(fileName, "cluster-uuid", compressor, null),
            new LatchedActionListener<>(listener, latch)
        );
        latch.await();
        assertNull(listener.getResult());
        assertNotNull(listener.getFailure());
        assertEquals(exception, listener.getFailure().getCause());
        assertTrue(listener.getFailure() instanceof RemoteStateTransferException);
    }

    public void testRemoteIndexMetadataPathTypeSetting() {
        // Assert the default is HASHED_PREFIX
        assertEquals(HASHED_PREFIX.toString(), remoteIndexMetadataManager.getPathTypeSetting().toString());

        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.index_metadata.path_type", RemoteStoreEnums.PathType.FIXED.toString())
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(RemoteStoreEnums.PathType.FIXED.toString(), remoteIndexMetadataManager.getPathTypeSetting().toString());
    }

    public void testRemoteIndexMetadataHashAlgoSetting() {
        // Assert the default is FNV_1A_BASE64
        assertEquals(FNV_1A_BASE64.toString(), remoteIndexMetadataManager.getPathHashAlgoSetting().toString());

        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.index_metadata.path_hash_algo", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString())
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString(),
            remoteIndexMetadataManager.getPathHashAlgoSetting().toString()
        );
    }

    private IndexMetadata getIndexMetadata(String name, @Nullable Boolean writeIndex, String... aliases) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put("index.version.created", Version.CURRENT.id)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
            );
        for (String alias : aliases) {
            builder.putAlias(AliasMetadata.builder(alias).writeIndex(writeIndex).build());
        }
        return builder.build();
    }
}
