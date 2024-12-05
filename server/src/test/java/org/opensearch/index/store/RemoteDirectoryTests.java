/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.remote.utils.BlockIOContext;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.Mockito;

import static org.opensearch.common.blobstore.BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteDirectoryTests extends OpenSearchTestCase {
    private BlobContainer blobContainer;

    private RemoteDirectory remoteDirectory;

    @Before
    public void setup() {
        blobContainer = mock(BlobContainer.class);
        remoteDirectory = new RemoteDirectory(blobContainer);
    }

    public void testListAllEmpty() throws IOException {
        when(blobContainer.listBlobs()).thenReturn(Collections.emptyMap());

        String[] actualFileNames = remoteDirectory.listAll();
        String[] expectedFileName = new String[] {};
        assertArrayEquals(expectedFileName, actualFileNames);
    }

    public void testCopyFrom() throws IOException, InterruptedException {
        AtomicReference<Boolean> postUploadInvoked = new AtomicReference<>(false);
        String filename = "_100.si";
        AsyncMultiStreamBlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(blobContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = LuceneTestCase.newDirectory();
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("Hello World!");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        RemoteDirectory remoteDirectory = new RemoteDirectory(blobContainer);
        remoteDirectory.copyFrom(
            storeDirectory,
            filename,
            filename,
            IOContext.READ,
            () -> postUploadInvoked.set(true),
            new ActionListener<>() {
                @Override
                public void onResponse(Void t) {
                    countDownLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Listener responded with exception" + e);
                }
            },
            false
        );
        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        assertTrue(postUploadInvoked.get());
        storeDirectory.close();
    }

    public void testCopyFromWithException() throws IOException, InterruptedException {
        AtomicReference<Boolean> postUploadInvoked = new AtomicReference<>(false);
        String filename = "_100.si";
        AsyncMultiStreamBlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(blobContainer).asyncBlobUpload(any(WriteContext.class), any());

        Directory storeDirectory = LuceneTestCase.newDirectory();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        RemoteDirectory remoteDirectory = new RemoteDirectory(blobContainer);
        remoteDirectory.copyFrom(
            storeDirectory,
            filename,
            filename,
            IOContext.READ,
            () -> postUploadInvoked.set(true),
            new ActionListener<>() {
                @Override
                public void onResponse(Void t) {
                    fail("Listener responded with success");
                }

                @Override
                public void onFailure(Exception e) {
                    countDownLatch.countDown();
                }
            },
            false
        );
        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        assertFalse(postUploadInvoked.get());
        storeDirectory.close();
    }

    public void testListAll() throws IOException {
        Map<String, BlobMetadata> fileNames = Stream.of("abc", "xyz", "pqr", "lmn", "jkl")
            .collect(Collectors.toMap(filename -> filename, filename -> new PlainBlobMetadata(filename, 100)));

        when(blobContainer.listBlobs()).thenReturn(fileNames);

        String[] actualFileNames = remoteDirectory.listAll();
        String[] expectedFileName = new String[] { "abc", "jkl", "lmn", "pqr", "xyz" };
        assertArrayEquals(expectedFileName, actualFileNames);
    }

    public void testListAllException() throws IOException {
        when(blobContainer.listBlobs()).thenThrow(new IOException("Error reading blob store"));

        assertThrows(IOException.class, () -> remoteDirectory.listAll());
    }

    public void testListFilesByPrefix() throws IOException {
        Map<String, BlobMetadata> fileNames = Stream.of("abc", "abd", "abe", "abf", "abg")
            .collect(Collectors.toMap(filename -> filename, filename -> new PlainBlobMetadata(filename, 100)));

        when(blobContainer.listBlobsByPrefix("ab")).thenReturn(fileNames);

        Collection<String> actualFileNames = remoteDirectory.listFilesByPrefix("ab");
        Collection<String> expectedFileName = Set.of("abc", "abd", "abe", "abf", "abg");
        assertEquals(expectedFileName, actualFileNames);
    }

    public void testListFilesByPrefixException() throws IOException {
        when(blobContainer.listBlobsByPrefix("abc")).thenThrow(new IOException("Error reading blob store"));

        assertThrows(IOException.class, () -> remoteDirectory.listFilesByPrefix("abc"));
        verify(blobContainer).listBlobsByPrefix("abc");
    }

    public void testDeleteFile() throws IOException {
        remoteDirectory.deleteFile("segment_1");

        verify(blobContainer).deleteBlobsIgnoringIfNotExists(Collections.singletonList("segment_1"));
    }

    public void testDeleteFileException() throws IOException {
        doThrow(new IOException("Error writing to blob store")).when(blobContainer)
            .deleteBlobsIgnoringIfNotExists(Collections.singletonList("segment_1"));

        assertThrows(IOException.class, () -> remoteDirectory.deleteFile("segment_1"));
    }

    public void testCreateOutput() {
        IndexOutput indexOutput = remoteDirectory.createOutput("segment_1", IOContext.DEFAULT);
        assertTrue(indexOutput instanceof RemoteIndexOutput);
        assertEquals("segment_1", indexOutput.getName());
    }

    public void testOpenInput() throws IOException {
        InputStream mockInputStream = mock(InputStream.class);
        when(blobContainer.readBlob("segment_1")).thenReturn(mockInputStream);

        BlobMetadata blobMetadata = new PlainBlobMetadata("segment_1", 100);

        when(blobContainer.listBlobsByPrefixInSortedOrder("segment_1", 1, LEXICOGRAPHIC)).thenReturn(List.of(blobMetadata));

        IndexInput indexInput = remoteDirectory.openInput("segment_1", IOContext.DEFAULT);
        assertTrue(indexInput instanceof RemoteIndexInput);
        assertEquals(100, indexInput.length());
        verify(blobContainer).listBlobsByPrefixInSortedOrder("segment_1", 1, LEXICOGRAPHIC);

        BlockIOContext blockIOContextInvalidValues = new BlockIOContext(IOContext.DEFAULT, 10, 1000);
        assertThrows(IllegalArgumentException.class, () -> remoteDirectory.openInput("segment_1", blockIOContextInvalidValues));

        BlockIOContext blockIOContext = new BlockIOContext(IOContext.DEFAULT, 10, 50);
        when(blobContainer.readBlob("segment_1", 10, 50)).thenReturn(mockInputStream);
        byte[] bytes = new byte[(int) blockIOContext.getBlockSize()];
        when(mockInputStream.readAllBytes()).thenReturn(bytes);
        indexInput = remoteDirectory.openInput("segment_1", blockIOContext);
        assertTrue(indexInput instanceof ByteArrayIndexInput);
        assertEquals(blockIOContext.getBlockSize(), indexInput.length());
    }

    public void testOpenInputWithLength() throws IOException {
        InputStream mockInputStream = mock(InputStream.class);
        when(blobContainer.readBlob("segment_1")).thenReturn(mockInputStream);

        BlobMetadata blobMetadata = new PlainBlobMetadata("segment_1", 100);

        when(blobContainer.listBlobsByPrefixInSortedOrder("segment_1", 1, LEXICOGRAPHIC)).thenReturn(List.of(blobMetadata));

        IndexInput indexInput = remoteDirectory.openInput("segment_1", 100, IOContext.DEFAULT);
        assertTrue(indexInput instanceof RemoteIndexInput);
        assertEquals(100, indexInput.length());
        verify(blobContainer, times(0)).listBlobsByPrefixInSortedOrder("segment_1", 1, LEXICOGRAPHIC);
    }

    public void testOpenInputIOException() throws IOException {
        when(blobContainer.readBlob("segment_1")).thenThrow(new IOException("Error while reading"));

        assertThrows(IOException.class, () -> remoteDirectory.openInput("segment_1", IOContext.DEFAULT));
    }

    public void testOpenInputNoSuchFileException() throws IOException {
        InputStream mockInputStream = mock(InputStream.class);
        when(blobContainer.readBlob("segment_1")).thenReturn(mockInputStream);
        when(blobContainer.listBlobsByPrefix("segment_1")).thenThrow(new NoSuchFileException("segment_1"));

        assertThrows(NoSuchFileException.class, () -> remoteDirectory.openInput("segment_1", IOContext.DEFAULT));
    }

    public void testFileLength() throws IOException {
        BlobMetadata blobMetadata = new PlainBlobMetadata("segment_1", 100);
        when(blobContainer.listBlobsByPrefixInSortedOrder("segment_1", 1, LEXICOGRAPHIC)).thenReturn(List.of(blobMetadata));

        assertEquals(100, remoteDirectory.fileLength("segment_1"));
    }

    public void testFileLengthIOException() throws IOException {
        when(blobContainer.listBlobsByPrefix("segment_1")).thenThrow(new NoSuchFileException("segment_1"));

        assertThrows(IOException.class, () -> remoteDirectory.fileLength("segment_1"));
    }

    public void testListFilesByPrefixInLexicographicOrder() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            latchedActionListener.onResponse(List.of(new PlainBlobMetadata("metadata_1", 1)));
            return null;
        }).when(blobContainer).listBlobsByPrefixInSortedOrder(eq("metadata"), eq(1), eq(LEXICOGRAPHIC), any(ActionListener.class));

        assertEquals(List.of("metadata_1"), remoteDirectory.listFilesByPrefixInLexicographicOrder("metadata", 1));
    }

    public void testListFilesByPrefixInLexicographicOrderEmpty() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            latchedActionListener.onResponse(List.of());
            return null;
        }).when(blobContainer).listBlobsByPrefixInSortedOrder(eq("metadata"), eq(1), eq(LEXICOGRAPHIC), any(ActionListener.class));

        assertEquals(List.of(), remoteDirectory.listFilesByPrefixInLexicographicOrder("metadata", 1));
    }

    public void testListFilesByPrefixInLexicographicOrderException() {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            latchedActionListener.onFailure(new IOException("Error"));
            return null;
        }).when(blobContainer).listBlobsByPrefixInSortedOrder(eq("metadata"), eq(1), eq(LEXICOGRAPHIC), any(ActionListener.class));

        assertThrows(IOException.class, () -> remoteDirectory.listFilesByPrefixInLexicographicOrder("metadata", 1));
    }

    public void testGetPendingDeletions() {
        assertThrows(UnsupportedOperationException.class, () -> remoteDirectory.getPendingDeletions());
    }

    public void testCreateTempOutput() {
        assertThrows(UnsupportedOperationException.class, () -> remoteDirectory.createTempOutput("segment_1", "tmp", IOContext.DEFAULT));
    }

    public void testSync() {
        assertThrows(UnsupportedOperationException.class, () -> remoteDirectory.sync(Collections.emptyList()));
    }

    public void testRename() {
        assertThrows(UnsupportedOperationException.class, () -> remoteDirectory.rename("segment_1", "segment_2"));
    }

    public void testObtainLock() {
        assertThrows(UnsupportedOperationException.class, () -> remoteDirectory.obtainLock("segment_1"));
    }
}
