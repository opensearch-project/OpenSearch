/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.core.action.ActionListener.onResponse;
import static org.opensearch.gateway.remote.RemoteClusterStateService.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX_METADATA_FORMAT;

public class RemoteIndexMetadataManagerTests extends OpenSearchTestCase {

    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private Compressor compressor;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    @Before
    public void setup() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
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

    public void testGetAsyncIndexMetadataWriteAction_Success() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        BlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener = mock(LatchedActionListener.class);
        ArgumentCaptor<ClusterMetadataManifest.UploadedMetadata> savedResult = ArgumentCaptor.forClass(ClusterMetadataManifest.UploadedMetadata.class);
        String expectedFilePrefix = String.join(
            DELIMITER,
            "metadata",
            RemoteStoreUtils.invertLong(indexMetadata.getVersion())
        );

        doAnswer((invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onResponse(null);
            return null;
        })).when(blobStoreTransferService).uploadBlob(any(), any(), any(), eq(WritePriority.URGENT), any(ActionListener.class));

        CheckedRunnable<IOException> runnable = remoteIndexMetadataManager.getAsyncIndexMetadataWriteAction(
            indexMetadata,
            "cluster-uuid",
            latchedActionListener
        );
        runnable.run();
        assertBusy(() -> verify(latchedActionListener, times(1)).onResponse(savedResult.capture()));

        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = savedResult.getValue();
        assertNotNull(uploadedMetadata);
        assertEquals(INDEX + "--" + indexMetadata.getIndex().getName(), uploadedMetadata.getComponent());
        String uploadedFileName = uploadedMetadata.getUploadedFilename();
        String[] pathTokens = uploadedFileName.split(PATH_DELIMITER);
        assertEquals(7, pathTokens.length);
        assertEquals(INDEX, pathTokens[4]);
        assertEquals(indexMetadata.getIndex().getUUID(), pathTokens[5]);
        assertTrue(pathTokens[6].startsWith(expectedFilePrefix));
    }

    public void testGetAsyncIndexMetadataWriteAction_IOFailure() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        BlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener = mock(LatchedActionListener.class);
        ArgumentCaptor<Exception> savedException = ArgumentCaptor.forClass(Exception.class);
        String expectedFilePrefix = String.join(
            DELIMITER,
            "metadata",
            RemoteStoreUtils.invertLong(indexMetadata.getVersion())
        );

        doAnswer((invocationOnMock -> {
            invocationOnMock.getArgument(4, ActionListener.class).onFailure(new IOException("failure"));
            return null;
        })).when(blobStoreTransferService).uploadBlob(any(), any(), any(), eq(WritePriority.URGENT), any(ActionListener.class));

        CheckedRunnable<IOException> runnable = remoteIndexMetadataManager.getAsyncIndexMetadataWriteAction(
            indexMetadata,
            "cluster-uuid",
            latchedActionListener
        );
        runnable.run();
        assertBusy(() -> verify(latchedActionListener, times(1)).onFailure(savedException.capture()));

        Exception exception = savedException.getValue();
        assertNotNull(exception);
        assertTrue(exception instanceof RemoteStateTransferException);
    }
    public void testGetAsyncIndexMetadataReadAction_Success() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata(randomAlphaOfLength(10), randomBoolean(), randomAlphaOfLength(10));
        String fileName = randomAlphaOfLength(10);
        fileName = fileName + DELIMITER + '2';
        when(blobStoreTransferService.downloadBlob(anyIterable(), anyString())).thenReturn(
            INDEX_METADATA_FORMAT.serialize(indexMetadata, fileName, compressor, FORMAT_PARAMS).streamInput()
        );
        AtomicReference<IndexMetadata> actualResponse = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<RemoteReadResult> latchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap(response -> actualResponse.set((IndexMetadata) response.getObj()), Assert::assertNull)
            , latch
        );

        CheckedRunnable<IOException> runnable = remoteIndexMetadataManager.getAsyncIndexMetadataReadAction(
            "cluster-uuid",
            fileName,
            latchedActionListener
        );
        assertNotNull(runnable);
        try {
            runnable.run();
            latch.await();
            assertEquals(indexMetadata, actualResponse.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testGetAsyncIndexMetadataReadAction_IOFailure() throws Exception {
        String fileName = randomAlphaOfLength(10);
        fileName = fileName + DELIMITER + '2';
        doThrow(new IOException("testing failure")).when(blobStoreTransferService).downloadBlob(anyIterable(), anyString());
        LatchedActionListener<RemoteReadResult> latchedActionListener = mock(LatchedActionListener.class);

        CheckedRunnable<IOException> runnable = remoteIndexMetadataManager.getAsyncIndexMetadataReadAction(
            "cluster-uuid",
            fileName,
            latchedActionListener
        );
        assertNotNull(runnable);
        runnable.run();

        verify(latchedActionListener, times(1)).onFailure(any(IOException.class));
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
