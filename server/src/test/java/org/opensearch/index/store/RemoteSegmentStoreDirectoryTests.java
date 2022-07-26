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
import org.junit.Before;
import org.opensearch.common.collect.Set;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.startsWith;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteSegmentStoreDirectoryTests extends OpenSearchTestCase {
    private RemoteDirectory remoteDataDirectory;
    private RemoteDirectory remoteMetadataDirectory;

    private RemoteSegmentStoreDirectory remoteSegmentStoreDirectory;

    @Before
    public void setup() throws IOException {
        remoteDataDirectory = mock(RemoteDirectory.class);
        remoteMetadataDirectory = mock(RemoteDirectory.class);

        remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(remoteDataDirectory, remoteMetadataDirectory);
    }

    public void testUploadedSegmentMetadataToString() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata = new RemoteSegmentStoreDirectory.UploadedSegmentMetadata(
            "abc",
            "pqr",
            "123456"
        );
        assertEquals("abc::pqr::123456", metadata.toString());
    }

    public void testUploadedSegmentMetadataFromString() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata = RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString(
            "_0.cfe::_0.cfe__uuidxyz::4567"
        );
        assertEquals("_0.cfe::_0.cfe__uuidxyz::4567", metadata.toString());
    }

    public void testGetMetadataFilename() {
        // Generation 23 is replaced by n due to radix 32
        assertTrue(RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename("abc", 12, 23).startsWith("abc__12__n__"));
        assertEquals(
            "abc__12__n__uuid_xyz",
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename("abc", 12, 23, "uuid_xyz")
        );
    }

    public void testGetPrimaryTermGenerationUuid() {
        assertEquals(12, RemoteSegmentStoreDirectory.MetadataFilenameUtils.getPrimaryTerm("abc__12__n__uuid_xyz"));
        assertEquals(23, RemoteSegmentStoreDirectory.MetadataFilenameUtils.getGeneration("abc__12__n__uuid_xyz"));
        assertEquals("uuid_xyz", RemoteSegmentStoreDirectory.MetadataFilenameUtils.getUuid("abc__12__n__uuid_xyz"));
    }

    public void testMetadataFilenameComparator() {
        List<String> metadataFilenames = new ArrayList<>(
            List.of(
                "abc__10__20__uuid1",
                "abc__12__2__uuid2",
                "pqr__1__1__uuid0",
                "abc__3__n__uuid3",
                "abc__10__8__uuid8",
                "abc__3__a__uuid4",
                "abc__3__a__uuid5"
            )
        );
        RemoteSegmentStoreDirectory.MetadataFilenameUtils.MetadataFilenameComparator metadataFilenameComparator =
            new RemoteSegmentStoreDirectory.MetadataFilenameUtils.MetadataFilenameComparator();
        metadataFilenames.sort(metadataFilenameComparator);
        assertEquals(
            List.of(
                "abc__3__a__uuid4",
                "abc__3__a__uuid5",
                "abc__3__n__uuid3",
                "abc__10__8__uuid8",
                "abc__10__20__uuid1",
                "abc__12__2__uuid2",
                "pqr__1__1__uuid0"
            ),
            metadataFilenames
        );
    }

    public void testInitException() throws IOException {
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.COMMIT_METADATA_PREFIX))
            .thenReturn(List.of());
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.REFRESH_METADATA_PREFIX))
            .thenThrow(new IOException("Error"));

        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testInitNoMetadataFile() throws IOException {
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.COMMIT_METADATA_PREFIX))
            .thenReturn(List.of());
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.REFRESH_METADATA_PREFIX))
            .thenReturn(List.of());

        remoteSegmentStoreDirectory.init();
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> actualCache = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertEquals(Set.of(), actualCache.keySet());
    }

    private Map<String, String> getDummyMetadata(String prefix, int commitGeneration) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(prefix + ".cfe", prefix + ".cfe::" + prefix + ".cfe__qrt::" + randomIntBetween(1000, 5000));
        metadata.put(prefix + ".cfs", prefix + ".cfs::" + prefix + ".cfs__zxd::" + randomIntBetween(1000, 5000));
        metadata.put(prefix + ".si", prefix + ".si::" + prefix + ".si__yui::" + randomIntBetween(1000, 5000));
        metadata.put(
            "segments_" + commitGeneration,
            "segments_" + commitGeneration + "::segments_" + commitGeneration + "__exv::" + randomIntBetween(1000, 5000)
        );
        return metadata;
    }

    private void populateCommitMetadata() throws IOException {
        List<String> commitFiles = List.of("commit_metadata__1__5__abc", "commit_metadata__1__6__pqr", "commit_metadata__2__1__zxv");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.COMMIT_METADATA_PREFIX))
            .thenReturn(commitFiles);

        IndexInput indexInput = mock(IndexInput.class);
        Map<String, String> commitMetadata = getDummyMetadata("_0", 1);
        when(indexInput.readMapOfStrings()).thenReturn(commitMetadata);
        when(remoteMetadataDirectory.openInput("commit_metadata__2__1__zxv", IOContext.DEFAULT)).thenReturn(indexInput);
    }

    private void populateRefreshMetadata() throws IOException {
        List<String> refreshedFiles = List.of("refresh_metadata__1__5__abc", "refresh_metadata__1__6__pqr", "refresh_metadata__2__1__zxv");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.REFRESH_METADATA_PREFIX))
            .thenReturn(refreshedFiles);

        IndexInput indexInput = mock(IndexInput.class);
        Map<String, String> refreshMetadata = getDummyMetadata("_1", 2);
        when(indexInput.readMapOfStrings()).thenReturn(refreshMetadata);
        when(remoteMetadataDirectory.openInput("refresh_metadata__2__1__zxv", IOContext.DEFAULT)).thenReturn(indexInput);
    }

    public void testInitOnlyCommit() throws IOException {
        populateCommitMetadata();

        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.REFRESH_METADATA_PREFIX))
            .thenReturn(List.of());

        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> actualCache = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertEquals(Set.of("_0.cfe", "_0.cfs", "_0.si", "segments_1"), actualCache.keySet());
    }

    public void testInitOnlyRefresh() throws IOException {
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.COMMIT_METADATA_PREFIX))
            .thenReturn(List.of());

        populateRefreshMetadata();

        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> actualCache = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertEquals(Set.of("_1.cfe", "_1.cfs", "_1.si", "segments_2"), actualCache.keySet());
    }

    public void testInitBothCommitAndRefresh() throws IOException {
        populateCommitMetadata();
        populateRefreshMetadata();

        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> actualCache = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertEquals(Set.of("_0.cfe", "_0.cfs", "_0.si", "segments_1", "_1.cfe", "_1.cfs", "_1.si", "segments_2"), actualCache.keySet());
    }

    public void testListAll() throws IOException {
        populateCommitMetadata();
        populateRefreshMetadata();

        assertEquals(
            Set.of("_0.cfe", "_0.cfs", "_0.si", "segments_1", "_1.cfe", "_1.cfs", "_1.si", "segments_2"),
            Set.of(remoteSegmentStoreDirectory.listAll())
        );
    }

    public void testDeleteFile() throws IOException {
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertTrue(uploadedSegments.containsKey("_0.si"));
        assertFalse(uploadedSegments.containsKey("_100.si"));

        remoteSegmentStoreDirectory.deleteFile("_0.si");
        remoteSegmentStoreDirectory.deleteFile("_100.si");

        verify(remoteDataDirectory).deleteFile(startsWith("_0.si"));
        verify(remoteDataDirectory, times(0)).deleteFile(startsWith("_100.si"));
        assertFalse(uploadedSegments.containsKey("_0.si"));
    }

    public void testDeleteFileException() throws IOException {
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        doThrow(new IOException("Error")).when(remoteDataDirectory).deleteFile(any());
        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.deleteFile("_0.si"));
    }

    public void testFileLenght() throws IOException {
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertTrue(uploadedSegments.containsKey("_0.si"));

        when(remoteDataDirectory.fileLength(startsWith("_0.si"))).thenReturn(1234L);

        assertEquals(1234L, remoteSegmentStoreDirectory.fileLength("_0.si"));
    }

    public void testFileLenghtNoSuchFile() throws IOException {
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertFalse(uploadedSegments.containsKey("_100.si"));
        assertThrows(NoSuchFileException.class, () -> remoteSegmentStoreDirectory.fileLength("_100.si"));
    }

    public void testCreateOutput() throws IOException {
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(remoteDataDirectory.createOutput(startsWith("abc"), eq(IOContext.DEFAULT))).thenReturn(indexOutput);

        assertEquals(indexOutput, remoteSegmentStoreDirectory.createOutput("abc", IOContext.DEFAULT));
    }

    public void testCreateOutputException() {
        when(remoteDataDirectory.createOutput(startsWith("abc"), eq(IOContext.DEFAULT))).thenThrow(new IOException("Error"));

        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.createOutput("abc", IOContext.DEFAULT));
    }

    public void testOpenInput() throws IOException {
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        IndexInput indexInput = mock(IndexInput.class);
        when(remoteDataDirectory.openInput(startsWith("_0.si"), eq(IOContext.DEFAULT))).thenReturn(indexInput);

        assertEquals(indexInput, remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testOpenInputNoSuchFile() {
        assertThrows(NoSuchFileException.class, () -> remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testOpenInputException() throws IOException {
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        when(remoteDataDirectory.openInput(startsWith("_0.si"), eq(IOContext.DEFAULT))).thenThrow(new IOException("Error"));

        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testCopyFrom() throws IOException {
        String filename = "_100.si";
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        Directory storeDirectory = LuceneTestCase.newDirectory();
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("Hello World!");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        assertFalse(remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().containsKey(filename));
        remoteSegmentStoreDirectory.copyFrom(storeDirectory, filename, filename, IOContext.DEFAULT);
        assertTrue(remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().containsKey(filename));

        storeDirectory.close();
    }

    public void testCopyFromException() throws IOException {
        String filename = "_100.si";
        Directory storeDirectory = LuceneTestCase.newDirectory();
        assertFalse(remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().containsKey(filename));
        doThrow(new IOException("Error")).when(remoteDataDirectory).copyFrom(storeDirectory, filename, filename, IOContext.DEFAULT);

        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.copyFrom(storeDirectory, filename, filename, IOContext.DEFAULT));
        assertFalse(remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().containsKey(filename));

        storeDirectory.close();
    }

    public void testContainsFile() throws IOException {
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        // This is not the correct way to add files but the other way is to open up access to fields in UploadedSegmentMetadata
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();
        uploadedSegmentMetadataMap.put(
            "_100.si",
            new RemoteSegmentStoreDirectory.UploadedSegmentMetadata("_100.si", "_100.si__uuid1", "1234")
        );

        assertTrue(remoteSegmentStoreDirectory.containsFile("_100.si", "1234"));
        assertFalse(remoteSegmentStoreDirectory.containsFile("_100.si", "2345"));
        assertFalse(remoteSegmentStoreDirectory.containsFile("_200.si", "1234"));
    }

    public void testUploadCommitMetadataEmpty() throws IOException {
        Directory storeDirectory = mock(Directory.class);
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(storeDirectory.createOutput(startsWith("commit_metadata__12__o"), eq(IOContext.DEFAULT))).thenReturn(indexOutput);

        Collection<String> commitFiles = List.of("s1", "s2", "s3");
        remoteSegmentStoreDirectory.uploadCommitMetadata(commitFiles, storeDirectory, 12L, 24L);

        verify(remoteMetadataDirectory).copyFrom(
            eq(storeDirectory),
            startsWith("commit_metadata__12__o"),
            startsWith("commit_metadata__12__o"),
            eq(IOContext.DEFAULT)
        );
        verify(indexOutput).writeMapOfStrings(Map.of());
    }

    public void testUploadRefreshMetadataNonEmpty() throws IOException {
        populateCommitMetadata();
        remoteSegmentStoreDirectory.init();

        Directory storeDirectory = mock(Directory.class);
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(storeDirectory.createOutput(startsWith("refresh_metadata__12__o"), eq(IOContext.DEFAULT))).thenReturn(indexOutput);

        Collection<String> commitFiles = List.of("_0.si");
        remoteSegmentStoreDirectory.uploadRefreshMetadata(commitFiles, storeDirectory, 12L, 24L);

        verify(remoteMetadataDirectory).copyFrom(
            eq(storeDirectory),
            startsWith("refresh_metadata__12__o"),
            startsWith("refresh_metadata__12__o"),
            eq(IOContext.DEFAULT)
        );
        String metadataString = remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().get("_0.si").toString();
        verify(indexOutput).writeMapOfStrings(Map.of("_0.si", metadataString));
    }
}
