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
import org.opensearch.common.UUIDs;
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
        assertEquals(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX + "__12__n__uuid1",
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(12, 23, "uuid1")
        );
    }

    public void testGetPrimaryTermGenerationUuid() {
        String[] filenameTokens = "abc__12__n__uuid_xyz".split(RemoteSegmentStoreDirectory.MetadataFilenameUtils.SEPARATOR);
        assertEquals(12, RemoteSegmentStoreDirectory.MetadataFilenameUtils.getPrimaryTerm(filenameTokens));
        assertEquals(23, RemoteSegmentStoreDirectory.MetadataFilenameUtils.getGeneration(filenameTokens));
        assertEquals("uuid_xyz", RemoteSegmentStoreDirectory.MetadataFilenameUtils.getUuid(filenameTokens));
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
        metadataFilenames.sort(RemoteSegmentStoreDirectory.METADATA_FILENAME_COMPARATOR);
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
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenThrow(
            new IOException("Error")
        );

        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testInitNoMetadataFile() throws IOException {
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            List.of()
        );

        remoteSegmentStoreDirectory.init();
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> actualCache = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertEquals(Set.of(), actualCache.keySet());
    }

    private Map<String, String> getDummyMetadata(String prefix, int commitGeneration) {
        Map<String, String> metadata = new HashMap<>();

        metadata.put(prefix + ".cfe", prefix + ".cfe::" + prefix + ".cfe__" + UUIDs.base64UUID() + "::" + randomIntBetween(1000, 5000));
        metadata.put(prefix + ".cfs", prefix + ".cfs::" + prefix + ".cfs__" + UUIDs.base64UUID() + "::" + randomIntBetween(1000, 5000));
        metadata.put(prefix + ".si", prefix + ".si::" + prefix + ".si__" + UUIDs.base64UUID() + "::" + randomIntBetween(1000, 5000));
        metadata.put(
            "segments_" + commitGeneration,
            "segments_"
                + commitGeneration
                + "::segments_"
                + commitGeneration
                + "__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
        );
        return metadata;
    }

    private Map<String, Map<String, String>> populateMetadata() throws IOException {
        List<String> metadataFiles = List.of("metadata__1__5__abc", "metadata__1__6__pqr", "metadata__2__1__zxv");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            metadataFiles
        );

        Map<String, Map<String, String>> metadataFilenameContentMapping = Map.of(
            "metadata__1__5__abc",
            getDummyMetadata("_0", 1),
            "metadata__1__6__pqr",
            getDummyMetadata("_0", 1),
            "metadata__2__1__zxv",
            getDummyMetadata("_0", 1)
        );

        IndexInput indexInput1 = mock(IndexInput.class);
        when(indexInput1.readMapOfStrings()).thenReturn(metadataFilenameContentMapping.get("metadata__1__5__abc"));
        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(indexInput1);

        IndexInput indexInput2 = mock(IndexInput.class);
        when(indexInput2.readMapOfStrings()).thenReturn(metadataFilenameContentMapping.get("metadata__1__6__pqr"));
        when(remoteMetadataDirectory.openInput("metadata__1__6__pqr", IOContext.DEFAULT)).thenReturn(indexInput2);

        IndexInput indexInput3 = mock(IndexInput.class);
        when(indexInput3.readMapOfStrings()).thenReturn(metadataFilenameContentMapping.get("metadata__2__1__zxv"));
        when(remoteMetadataDirectory.openInput("metadata__2__1__zxv", IOContext.DEFAULT)).thenReturn(indexInput3);

        return metadataFilenameContentMapping;
    }

    public void testInit() throws IOException {
        populateMetadata();

        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            List.of("metadata__1__5__abc", "metadata__1__6__pqr", "metadata__2__1__zxv")
        );

        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> actualCache = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertEquals(Set.of("_0.cfe", "_0.cfs", "_0.si", "segments_1"), actualCache.keySet());
    }

    public void testListAll() throws IOException {
        populateMetadata();

        assertEquals(Set.of("_0.cfe", "_0.cfs", "_0.si", "segments_1"), Set.of(remoteSegmentStoreDirectory.listAll()));
    }

    public void testDeleteFile() throws IOException {
        populateMetadata();
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
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        doThrow(new IOException("Error")).when(remoteDataDirectory).deleteFile(any());
        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.deleteFile("_0.si"));
    }

    public void testFileLenght() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertTrue(uploadedSegments.containsKey("_0.si"));

        when(remoteDataDirectory.fileLength(startsWith("_0.si"))).thenReturn(1234L);

        assertEquals(1234L, remoteSegmentStoreDirectory.fileLength("_0.si"));
    }

    public void testFileLenghtNoSuchFile() throws IOException {
        populateMetadata();
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
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        IndexInput indexInput = mock(IndexInput.class);
        when(remoteDataDirectory.openInput(startsWith("_0.si"), eq(IOContext.DEFAULT))).thenReturn(indexInput);

        assertEquals(indexInput, remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testOpenInputNoSuchFile() {
        assertThrows(NoSuchFileException.class, () -> remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testOpenInputException() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        when(remoteDataDirectory.openInput(startsWith("_0.si"), eq(IOContext.DEFAULT))).thenThrow(new IOException("Error"));

        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testCopyFrom() throws IOException {
        String filename = "_100.si";
        populateMetadata();
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

    public void testCopyFromOverride() throws IOException {
        String filename = "_100.si";
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        Directory storeDirectory = LuceneTestCase.newDirectory();
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("Hello World!");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        assertFalse(remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().containsKey(filename));
        remoteSegmentStoreDirectory.copyFrom(storeDirectory, filename, filename, IOContext.DEFAULT, true);
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore()
            .get(filename);
        assertNotNull(uploadedSegmentMetadata);
        remoteSegmentStoreDirectory.copyFrom(storeDirectory, filename, filename, IOContext.DEFAULT, true);
        assertEquals(
            uploadedSegmentMetadata.toString(),
            remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().get(filename).toString()
        );

        storeDirectory.close();
    }

    public void testContainsFile() throws IOException {
        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            metadataFiles
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234");
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345");

        Map<String, Map<String, String>> metadataFilenameContentMapping = Map.of("metadata__1__5__abc", metadata);

        IndexInput indexInput1 = mock(IndexInput.class);
        when(indexInput1.readMapOfStrings()).thenReturn(metadataFilenameContentMapping.get("metadata__1__5__abc"));
        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(indexInput1);

        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertThrows(
            UnsupportedOperationException.class,
            () -> uploadedSegmentMetadataMap.put(
                "_100.si",
                new RemoteSegmentStoreDirectory.UploadedSegmentMetadata("_100.si", "_100.si__uuid1", "1234")
            )
        );

        assertTrue(remoteSegmentStoreDirectory.containsFile("_0.cfe", "1234"));
        assertTrue(remoteSegmentStoreDirectory.containsFile("_0.cfs", "2345"));
        assertFalse(remoteSegmentStoreDirectory.containsFile("_0.cfe", "1234000"));
        assertFalse(remoteSegmentStoreDirectory.containsFile("_0.cfs", "2345000"));
        assertFalse(remoteSegmentStoreDirectory.containsFile("_0.si", "23"));
    }

    public void testUploadMetadataEmpty() throws IOException {
        Directory storeDirectory = mock(Directory.class);
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(storeDirectory.createOutput(startsWith("metadata__12__o"), eq(IOContext.DEFAULT))).thenReturn(indexOutput);

        Collection<String> segmentFiles = List.of("s1", "s2", "s3");
        assertThrows(NoSuchFileException.class, () -> remoteSegmentStoreDirectory.uploadMetadata(segmentFiles, storeDirectory, 12L, 24L));
    }

    public void testUploadMetadataNonEmpty() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        Directory storeDirectory = mock(Directory.class);
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(storeDirectory.createOutput(startsWith("metadata__12__o"), eq(IOContext.DEFAULT))).thenReturn(indexOutput);

        Collection<String> segmentFiles = List.of("_0.si");
        remoteSegmentStoreDirectory.uploadMetadata(segmentFiles, storeDirectory, 12L, 24L);

        verify(remoteMetadataDirectory).copyFrom(
            eq(storeDirectory),
            startsWith("metadata__12__o"),
            startsWith("metadata__12__o"),
            eq(IOContext.DEFAULT)
        );
        String metadataString = remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().get("_0.si").toString();
        verify(indexOutput).writeMapOfStrings(Map.of("_0.si", metadataString));
    }

    public void testDeleteStaleCommitsException() throws IOException {
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenThrow(
            new IOException("Error reading")
        );

        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.deleteStaleSegments(5));
    }

    public void testDeleteStaleCommitsWithinThreshold() throws IOException {
        populateMetadata();

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=5 here so that none of the metadata files will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegments(5);

        verify(remoteMetadataDirectory, times(0)).openInput(any(String.class), eq(IOContext.DEFAULT));
    }

    public void testDeleteStaleCommitsActualDelete() throws IOException {
        Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        remoteSegmentStoreDirectory.init();

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegments(2);

        for (String metadata : metadataFilenameContentMapping.get("metadata__1__5__abc").values()) {
            String uploadedFilename = metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
            verify(remoteDataDirectory).deleteFile(uploadedFilename);
        }
        ;
        verify(remoteMetadataDirectory).deleteFile("metadata__1__5__abc");
    }

    public void testDeleteStaleCommitsActualDeleteIOException() throws IOException {
        Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        remoteSegmentStoreDirectory.init();

        String segmentFileWithException = metadataFilenameContentMapping.get("metadata__1__5__abc")
            .values()
            .stream()
            .findAny()
            .get()
            .split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
        doThrow(new IOException("Error")).when(remoteDataDirectory).deleteFile(segmentFileWithException);
        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegments(2);

        for (String metadata : metadataFilenameContentMapping.get("metadata__1__5__abc").values()) {
            String uploadedFilename = metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
            verify(remoteDataDirectory).deleteFile(uploadedFilename);
        }
        ;
        verify(remoteMetadataDirectory, times(0)).deleteFile("metadata__1__5__abc");
    }

    public void testDeleteStaleCommitsActualDeleteNoSuchFileException() throws IOException {
        Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        remoteSegmentStoreDirectory.init();

        String segmentFileWithException = metadataFilenameContentMapping.get("metadata__1__5__abc")
            .values()
            .stream()
            .findAny()
            .get()
            .split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
        doThrow(new NoSuchFileException(segmentFileWithException)).when(remoteDataDirectory).deleteFile(segmentFileWithException);
        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegments(2);

        for (String metadata : metadataFilenameContentMapping.get("metadata__1__5__abc").values()) {
            String uploadedFilename = metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
            verify(remoteDataDirectory).deleteFile(uploadedFilename);
        }
        ;
        verify(remoteMetadataDirectory).deleteFile("metadata__1__5__abc");
    }
}
