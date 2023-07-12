/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.VerifyingMultiStreamBlobContainer;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlobStoreTransferServiceMockRepositoryTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    private BlobStore blobStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blobStore = mock(BlobStore.class);
        threadPool = new TestThreadPool(getClass().getName());
    }

    public void testUploadBlobs() throws Exception {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
            testFile,
            randomNonNegativeLong(),
            0L
        );

        VerifyingMultiStreamBlobContainer blobContainer = mock(VerifyingMultiStreamBlobContainer.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(blobContainer).asyncBlobUpload(any(WriteContext.class), any());
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean onResponseCalled = new AtomicBoolean(false);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        AtomicReference<FileSnapshot.TransferFileSnapshot> fileSnapshotRef = new AtomicReference<>();
        transferService.uploadBlobs(Collections.singleton(transferFileSnapshot), new HashMap<>() {
            {
                put(transferFileSnapshot.getPrimaryTerm(), new BlobPath().add("sample_path"));
            }
        }, new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(FileSnapshot.TransferFileSnapshot fileSnapshot) {
                onResponseCalled.set(true);
                fileSnapshotRef.set(fileSnapshot);
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
            }
        }, latch), WritePriority.HIGH);

        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        verify(blobContainer).asyncBlobUpload(any(WriteContext.class), any());
        assertTrue(onResponseCalled.get());
        assertEquals(transferFileSnapshot.getPrimaryTerm(), fileSnapshotRef.get().getPrimaryTerm());
        assertEquals(transferFileSnapshot.getName(), fileSnapshotRef.get().getName());
        assertNull(exceptionRef.get());
    }

    public void testUploadBlobsIOException() throws Exception {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
            testFile,
            randomNonNegativeLong(),
            0L
        );

        VerifyingMultiStreamBlobContainer blobContainer = mock(VerifyingMultiStreamBlobContainer.class);
        doThrow(new IOException()).when(blobContainer).asyncBlobUpload(any(WriteContext.class), any());
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean onResponseCalled = new AtomicBoolean(false);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        transferService.uploadBlobs(Collections.singleton(transferFileSnapshot), new HashMap<>() {
            {
                put(transferFileSnapshot.getPrimaryTerm(), new BlobPath().add("sample_path"));
            }
        }, new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(FileSnapshot.TransferFileSnapshot fileSnapshot) {
                onResponseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
            }
        }, latch), WritePriority.HIGH);

        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        verify(blobContainer).asyncBlobUpload(any(WriteContext.class), any());
        assertFalse(onResponseCalled.get());
        assertTrue(exceptionRef.get() instanceof FileTransferException);
    }

    public void testUploadBlobsUploadFutureCompletedExceptionally() throws Exception {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
            testFile,
            randomNonNegativeLong(),
            0L
        );

        VerifyingMultiStreamBlobContainer blobContainer = mock(VerifyingMultiStreamBlobContainer.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onFailure(new Exception("Test exception"));
            return null;
        }).when(blobContainer).asyncBlobUpload(any(WriteContext.class), any());

        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean onResponseCalled = new AtomicBoolean(false);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        LatchedActionListener<FileSnapshot.TransferFileSnapshot> listener = new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(FileSnapshot.TransferFileSnapshot fileSnapshot) {
                onResponseCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
            }
        }, latch);
        transferService.uploadBlobs(Collections.singleton(transferFileSnapshot), new HashMap<>() {
            {
                put(transferFileSnapshot.getPrimaryTerm(), new BlobPath().add("sample_path"));
            }
        }, listener, WritePriority.HIGH);

        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        verify(blobContainer).asyncBlobUpload(any(WriteContext.class), any());
        assertFalse(onResponseCalled.get());
        assertTrue(exceptionRef.get() instanceof FileTransferException);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
