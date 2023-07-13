/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;

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
        Map<String, BlobMetadata> fileInfo = new HashMap<>();
        fileInfo.put("segment_1", new PlainBlobMetadata("segment_1", 100));
        when(blobContainer.listBlobsByPrefix("segment_1")).thenReturn(fileInfo);

        IndexInput indexInput = remoteDirectory.openInput("segment_1", IOContext.DEFAULT);
        assertTrue(indexInput instanceof RemoteIndexInput);
        assertEquals(100, indexInput.length());
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
        Map<String, BlobMetadata> fileInfo = new HashMap<>();
        fileInfo.put("segment_1", new PlainBlobMetadata("segment_1", 100));
        when(blobContainer.listBlobsByPrefix("segment_1")).thenReturn(fileInfo);

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
        }).when(blobContainer)
            .listBlobsByPrefixInSortedOrder(
                eq("metadata"),
                eq(1),
                eq(BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC),
                any(ActionListener.class)
            );

        assertEquals(List.of("metadata_1"), remoteDirectory.listFilesByPrefixInLexicographicOrder("metadata", 1));
    }

    public void testListFilesByPrefixInLexicographicOrderEmpty() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            latchedActionListener.onResponse(List.of());
            return null;
        }).when(blobContainer)
            .listBlobsByPrefixInSortedOrder(
                eq("metadata"),
                eq(1),
                eq(BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC),
                any(ActionListener.class)
            );

        assertEquals(List.of(), remoteDirectory.listFilesByPrefixInLexicographicOrder("metadata", 1));
    }

    public void testListFilesByPrefixInLexicographicOrderException() {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            latchedActionListener.onFailure(new IOException("Error"));
            return null;
        }).when(blobContainer)
            .listBlobsByPrefixInSortedOrder(
                eq("metadata"),
                eq(1),
                eq(BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC),
                any(ActionListener.class)
            );

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
