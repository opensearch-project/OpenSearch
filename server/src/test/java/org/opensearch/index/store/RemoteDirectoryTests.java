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
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
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
            IOContext.DEFAULT,
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
            false,
            null
        );
        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        assertTrue(postUploadInvoked.get());
        storeDirectory.close();
    }

    /**
     * Regression test for PR #22309 ported to the non-composite (Lucene-only) upload path in RemoteDirectory.
     *
     * <p>The multipart upload supplier must hand each part an <b>independent</b> IndexInput (via slice()), not a
     * clone() that shares the master's MemorySegment[] array. With clone(), closing one part's stream runs
     * Arrays.fill(segments, null) on the shared array, so the <i>next</i> part's provideStream() throws
     * AlreadyClosedException inside the OffsetRangeIndexInputStream constructor (indexInput.seek()). The parts are
     * therefore served interleaved — provideStream(i) then close(i) then provideStream(i+1) — which is how the
     * async transfer manager consumes them and is what surfaces the corruption. Fails with clone(), passes with
     * slice(). Ported from PR #22309's DataFormatAwareRemoteDirectory coverage.
     */
    public void testCopyFromMultiPartStreamsAreIndependent() throws Exception {
        String filename = "_100.si";
        // Build a real on-disk file large enough to split into several parts.
        byte[] payload = new byte[65536];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }

        // Use a real MMapDirectory with a small mmap chunk size so the file spans MULTIPLE segments,
        // producing a MultiSegmentImpl-backed MemorySegmentIndexInput. That is the only shape where
        // clone() shares the segments[] array by reference (MultiSegmentImpl), so closing one part
        // corrupts the others via Arrays.fill(segments, null). A single-segment input clones to
        // SingleSegmentImpl and would not reproduce the bug. maxChunkSize must be a power of two.
        Directory storeDirectory = new MMapDirectory(createTempDir(), 4096L);
        try (IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT)) {
            indexOutput.writeBytes(payload, payload.length);
            CodecUtil.writeFooter(indexOutput);
        }
        storeDirectory.sync(List.of(filename));
        long fileLength = storeDirectory.fileLength(filename);
        byte[] fileBytes = new byte[(int) fileLength];
        try (IndexInput verify = storeDirectory.openInput(filename, IOContext.DEFAULT)) {
            verify.readBytes(fileBytes, 0, fileBytes.length);
        }

        // Capture the WriteContext and defer the completion listener, so the master IndexInput stays
        // open while we serve part streams — mirroring how the async transfer manager consumes parts.
        AtomicReference<WriteContext> capturedWriteContext = new AtomicReference<>();
        AtomicReference<ActionListener<Void>> capturedListener = new AtomicReference<>();
        CountDownLatch uploadInvoked = new CountDownLatch(1);
        AsyncMultiStreamBlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(blobContainer.remoteIntegrityCheckSupported()).thenReturn(false);
        Mockito.doAnswer(invocation -> {
            capturedWriteContext.set(invocation.getArgument(0));
            capturedListener.set(invocation.getArgument(1));
            uploadInvoked.countDown();
            return null;
        }).when(blobContainer).asyncBlobUpload(any(WriteContext.class), any());

        CountDownLatch done = new CountDownLatch(1);
        RemoteDirectory remoteDirectory = new RemoteDirectory(blobContainer);
        remoteDirectory.copyFrom(
            storeDirectory,
            filename,
            filename,
            IOContext.DEFAULT,
            () -> {},
            new LatchedActionListener<>(ActionListener.wrap(r -> {}, e -> fail("upload failed: " + e)), done),
            false,
            null
        );
        assertTrue(uploadInvoked.await(10, TimeUnit.SECONDS));

        // Split the file into 4 parts and serve them interleaved: provideStream(p) then close before
        // provideStream(p+1). With clone(), closing part p corrupts the shared segments[] and the next
        // provideStream() throws AlreadyClosedException; with slice() each part is independent.
        long partSize = (fileLength + 3) / 4;
        StreamContext streamContext = capturedWriteContext.get().getStreamProvider(partSize);
        int numberOfParts = streamContext.getNumberOfParts();
        assertTrue("expected multiple parts, got " + numberOfParts, numberOfParts > 1);

        for (int p = 0; p < numberOfParts; p++) {
            long offset = p * partSize;
            long size = Math.min(partSize, fileLength - offset);
            InputStreamContainer container;
            try {
                container = streamContext.provideStream(p);
            } catch (org.apache.lucene.store.AlreadyClosedException e) {
                throw new AssertionError(
                    "provideStream("
                        + p
                        + ") threw AlreadyClosedException — a prior part's close() corrupted the "
                        + "shared MemorySegment[] array. This is the clone() bug that slice() fixes (PR #22309).",
                    e
                );
            }
            assertEquals(size, container.getContentLength());
            byte[] actual = new byte[(int) size];
            InputStream in = container.getInputStream();
            int read = 0;
            while (read < actual.length) {
                int n = in.read(actual, read, actual.length - read);
                assertTrue("unexpected EOF on part " + p, n > 0);
                read += n;
            }
            for (int i = 0; i < size; i++) {
                assertEquals("mismatch part=" + p + " byte=" + i, fileBytes[(int) offset + i], actual[i]);
            }
            // Close this part before serving the next — this is what triggers the shared-array corruption
            // under clone().
            in.close();
        }

        // Complete the upload so the master IndexInput is closed via the completion listener.
        capturedListener.get().onResponse(null);
        assertTrue(done.await(10, TimeUnit.SECONDS));

        storeDirectory.close();
    }

    /**
     * The lifecycle-tracking wrapper (PR #22309, centralized in RemoteDirectory#wrapWithLifecycleTracking) must:
     * transparently delegate reads and length; survive a double-close without throwing (it delegates close each
     * time, but tracks and logs the second one as a lifecycle bug); and produce independent, still-readable clones.
     */
    public void testWrapWithLifecycleTracking() throws Exception {
        String filename = "_wrap.bin";
        byte[] payload = new byte[512];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        Directory storeDirectory = new MMapDirectory(createTempDir());
        try (IndexOutput out = storeDirectory.createOutput(filename, IOContext.DEFAULT)) {
            out.writeBytes(payload, payload.length);
            CodecUtil.writeFooter(out);
        }
        storeDirectory.sync(List.of(filename));
        long fileLength = storeDirectory.fileLength(filename);

        RemoteDirectory remoteDirectory = new RemoteDirectory(mock(AsyncMultiStreamBlobContainer.class));
        IndexInput raw = storeDirectory.openInput(filename, IOContext.DEFAULT);
        IndexInput tracked = remoteDirectory.wrapWithLifecycleTracking(raw, filename);

        // Delegates length and reads.
        assertEquals(fileLength, tracked.length());
        byte[] head = new byte[payload.length];
        tracked.readBytes(head, 0, head.length);
        for (int i = 0; i < payload.length; i++) {
            assertEquals("byte " + i, payload[i], head[i]);
        }

        // clone() yields an independent, readable input that seeks without disturbing the master.
        IndexInput clone = tracked.clone();
        clone.seek(0);
        assertEquals(payload[0], clone.readByte());
        clone.close();

        // Master still usable after clone closes (slice()/clone() independence).
        tracked.seek(0);
        assertEquals(payload[0], tracked.readByte());

        // Double-close must not throw (it is tracked and logged, not fatal).
        tracked.close();
        tracked.close();

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
            IOContext.DEFAULT,
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
            false,
            null
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

    /**
     *
     * Tests that deleteFiles successfully deletes multiple files from the remote store.
     */
    public void testDeleteFiles() throws IOException {
        List<String> filesToDelete = List.of("segment_1", "segment_2", "segment_3");

        remoteDirectory.deleteFiles(filesToDelete);

        verify(blobContainer).deleteBlobsIgnoringIfNotExists(filesToDelete);
    }

    /**
     *
     * Tests that deleteFiles handles empty collection gracefully without attempting any deletions.
     */
    public void testDeleteFilesEmptyCollection() throws IOException {
        remoteDirectory.deleteFiles(Collections.emptyList());

        verify(blobContainer, times(0)).deleteBlobsIgnoringIfNotExists(any());
    }

    /**
     *
     * Tests that deleteFiles handles null collection gracefully without attempting any deletions.
     */
    public void testDeleteFilesNullCollection() throws IOException {
        remoteDirectory.deleteFiles(null);
        verify(blobContainer, times(0)).deleteBlobsIgnoringIfNotExists(any());
    }

    /**
     *
     * Tests that deleteFiles completes successfully even when some files don't exist.
     * The underlying deleteBlobsIgnoringIfNotExists should handle non-existent files gracefully.
     */
    public void testDeleteFilesWithNonExistentFiles() throws IOException {
        List<String> filesToDelete = List.of("segment_1", "non_existent", "segment_2");

        remoteDirectory.deleteFiles(filesToDelete);

        verify(blobContainer).deleteBlobsIgnoringIfNotExists(filesToDelete);
    }

    /**
     *
     * Tests that deleteFiles propagates IOException when the underlying blob container operation fails.
     */
    public void testDeleteFilesException() throws IOException {
        List<String> filesToDelete = List.of("segment_1", "segment_2");
        doThrow(new IOException("Error writing to blob store")).when(blobContainer).deleteBlobsIgnoringIfNotExists(filesToDelete);

        assertThrows(IOException.class, () -> remoteDirectory.deleteFiles(filesToDelete));
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
