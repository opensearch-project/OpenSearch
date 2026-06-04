/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.Randomness;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.read.listener.ListenerTestUtils;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AsyncMultiStreamEncryptedBlobContainerTests extends OpenSearchTestCase {

    // Tests the happy path scenario for decrypting a read context
    @SuppressWarnings("unchecked")
    public void testReadBlobAsync() throws Exception {
        String testBlobName = "testBlobName";
        int size = 100;

        // Mock objects needed for the test
        AsyncMultiStreamBlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        Object cryptoContext = mock(Object.class);
        when(cryptoHandler.loadEncryptionMetadata(any())).thenReturn(cryptoContext);
        when(cryptoHandler.estimateDecryptedLength(any(), anyLong())).thenReturn((long) size);
        long[] adjustedRanges = { 0, size - 1 };
        DecryptedRangedStreamProvider rangedStreamProvider = new DecryptedRangedStreamProvider(adjustedRanges, UnaryOperator.identity());
        when(cryptoHandler.createDecryptingStreamOfRange(eq(cryptoContext), anyLong(), anyLong())).thenReturn(rangedStreamProvider);

        // Objects needed for API call
        final byte[] data = new byte[size];
        Randomness.get().nextBytes(data);

        final InputStreamContainer inputStreamContainer = new InputStreamContainer(new ByteArrayInputStream(data), data.length, 0);
        final ListenerTestUtils.CountingCompletionListener<ReadContext> completionListener =
            new ListenerTestUtils.CountingCompletionListener<>();
        final CompletableFuture<InputStreamContainer> streamContainerFuture = CompletableFuture.completedFuture(inputStreamContainer);
        final ReadContext readContext = new ReadContext.Builder(size, List.of(() -> streamContainerFuture)).build();

        Mockito.doAnswer(invocation -> {
            ActionListener<ReadContext> readContextActionListener = invocation.getArgument(1);
            readContextActionListener.onResponse(readContext);
            return null;
        }).when(blobContainer).readBlobAsync(eq(testBlobName), any());

        AsyncMultiStreamEncryptedBlobContainer<Object, Object> asyncMultiStreamEncryptedBlobContainer =
            new AsyncMultiStreamEncryptedBlobContainer<>(blobContainer, cryptoHandler);
        asyncMultiStreamEncryptedBlobContainer.readBlobAsync(testBlobName, completionListener);

        // Assert results
        ReadContext response = completionListener.getResponse();
        assertEquals(0, completionListener.getFailureCount());
        assertEquals(1, completionListener.getResponseCount());
        assertNull(completionListener.getException());

        assertTrue(response instanceof AsyncMultiStreamEncryptedBlobContainer.DecryptedReadContext);
        assertEquals(1, response.getNumberOfParts());
        assertEquals(size, response.getBlobSize());

        InputStreamContainer responseContainer = response.getPartStreams().get(0).get().join();
        assertEquals(0, responseContainer.getOffset());
        assertEquals(size, responseContainer.getContentLength());
        assertEquals(100, responseContainer.getInputStream().available());
    }

    // Tests the exception scenario for decrypting a read context
    @SuppressWarnings("unchecked")
    public void testReadBlobAsyncException() throws Exception {
        String testBlobName = "testBlobName";
        int size = 100;

        // Mock objects needed for the test
        AsyncMultiStreamBlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        when(cryptoHandler.loadEncryptionMetadata(any())).thenThrow(new IOException());

        // Objects needed for API call
        final byte[] data = new byte[size];
        Randomness.get().nextBytes(data);
        final InputStreamContainer inputStreamContainer = new InputStreamContainer(new ByteArrayInputStream(data), data.length, 0);
        final ListenerTestUtils.CountingCompletionListener<ReadContext> completionListener =
            new ListenerTestUtils.CountingCompletionListener<>();
        final CompletableFuture<InputStreamContainer> streamContainerFuture = CompletableFuture.completedFuture(inputStreamContainer);
        final ReadContext readContext = new ReadContext.Builder(size, List.of(() -> streamContainerFuture)).build();

        Mockito.doAnswer(invocation -> {
            ActionListener<ReadContext> readContextActionListener = invocation.getArgument(1);
            readContextActionListener.onResponse(readContext);
            return null;
        }).when(blobContainer).readBlobAsync(eq(testBlobName), any());

        AsyncMultiStreamEncryptedBlobContainer<Object, Object> asyncMultiStreamEncryptedBlobContainer =
            new AsyncMultiStreamEncryptedBlobContainer<>(blobContainer, cryptoHandler);
        asyncMultiStreamEncryptedBlobContainer.readBlobAsync(testBlobName, completionListener);

        // Assert results
        assertEquals(1, completionListener.getFailureCount());
        assertEquals(0, completionListener.getResponseCount());
        assertNull(completionListener.getResponse());
        assertTrue(completionListener.getException() instanceof IOException);
    }

}
