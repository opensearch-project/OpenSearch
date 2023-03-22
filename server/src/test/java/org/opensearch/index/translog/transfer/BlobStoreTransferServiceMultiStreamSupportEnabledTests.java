/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlobStoreTransferServiceMultiStreamSupportEnabledTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    private BlobStore blobStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blobStore = mock(BlobStore.class);
        threadPool = new TestThreadPool(getClass().getName());
    }

    public void testUploadBlob() throws IOException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(testFile, randomNonNegativeLong());

        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.isMultiStreamUploadSupported()).thenReturn(true);
        doNothing().when(blobContainer).writeStreams(any(WriteContext.class));
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
        transferService.uploadBlob(transferFileSnapshot, new BlobPath().add("sample_path"), WritePriority.HIGH);

        verify(blobContainer).writeStreams(any(WriteContext.class));
    }

    public void testUploadBlobIOException() throws IOException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(testFile, randomNonNegativeLong());

        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.isMultiStreamUploadSupported()).thenReturn(true);
        doThrow(new IOException()).when(blobContainer).writeStreams(any(WriteContext.class));
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
        assertThrows(
            IOException.class,
            () -> transferService.uploadBlob(transferFileSnapshot, new BlobPath().add("sample_path"), WritePriority.HIGH)
        );

        verify(blobContainer).writeStreams(any(WriteContext.class));
    }

    public void testUploadBlobAsync() throws IOException, InterruptedException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(testFile, randomNonNegativeLong());

        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.isMultiStreamUploadSupported()).thenReturn(true);
        doNothing().when(blobContainer).writeStreams(any(WriteContext.class));
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean succeeded = new AtomicBoolean(false);
        transferService.uploadBlobAsync(
            ThreadPool.Names.TRANSLOG_TRANSFER,
            transferFileSnapshot,
            new BlobPath().add("sample_path"),
            new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(FileSnapshot.TransferFileSnapshot fileSnapshot) {
                    assert succeeded.compareAndSet(false, true);
                    assertEquals(transferFileSnapshot.getPrimaryTerm(), fileSnapshot.getPrimaryTerm());
                    assertEquals(transferFileSnapshot.getName(), fileSnapshot.getName());
                    try {
                        verify(blobContainer).writeStreams(any(WriteContext.class));
                    } catch (IOException ex) {
                        fail();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("Failed to perform uploadBlobAsync", e);
                }
            }, latch),
            WritePriority.HIGH
        );

        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
        assertTrue(succeeded.get());
    }

    public void testUploadBlobAsyncIOException() throws IOException, InterruptedException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(testFile, randomNonNegativeLong());

        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.isMultiStreamUploadSupported()).thenReturn(true);
        doThrow(new IOException()).when(blobContainer).writeStreams(any(WriteContext.class));
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        TransferService transferService = new BlobStoreTransferService(blobStore, threadPool);
        CountDownLatch latch = new CountDownLatch(1);
        transferService.uploadBlobAsync(
            ThreadPool.Names.TRANSLOG_TRANSFER,
            transferFileSnapshot,
            new BlobPath().add("sample_path"),
            new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(FileSnapshot.TransferFileSnapshot fileSnapshot) {
                    fail("Did not expect uploadBlobAsync to succeed");
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        verify(blobContainer).writeStreams(any(WriteContext.class));
                    } catch (IOException ex) {
                        fail();
                    }
                    assertTrue(e instanceof FileTransferException);
                    assertTrue(e.getCause() instanceof IOException);
                }
            }, latch),
            WritePriority.HIGH
        );

        assertTrue(latch.await(1000, TimeUnit.MILLISECONDS));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }
}
