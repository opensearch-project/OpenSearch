/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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

    /**
     * Prepares metadata file bytes with header and footer
     * @param segmentFilesMap: actual metadata content
     * @return ByteArrayIndexInput: metadata file bytes with header and footer
     * @throws IOException IOException
     */
    private ByteArrayIndexInput createMetadataFileBytes(Map<String, String> segmentFilesMap) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        CodecUtil.writeHeader(indexOutput, RemoteSegmentMetadata.METADATA_CODEC, RemoteSegmentMetadata.CURRENT_VERSION);
        indexOutput.writeMapOfStrings(segmentFilesMap);
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        return new ByteArrayIndexInput("segment metadata", BytesReference.toBytes(output.bytes()));
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

        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(
            createMetadataFileBytes(metadataFilenameContentMapping.get("metadata__1__5__abc"))
        );
        when(remoteMetadataDirectory.openInput("metadata__1__6__pqr", IOContext.DEFAULT)).thenReturn(
            createMetadataFileBytes(metadataFilenameContentMapping.get("metadata__1__6__pqr"))
        );
        when(remoteMetadataDirectory.openInput("metadata__2__1__zxv", IOContext.DEFAULT)).thenReturn(
            createMetadataFileBytes(metadataFilenameContentMapping.get("metadata__2__1__zxv")),
            createMetadataFileBytes(metadataFilenameContentMapping.get("metadata__2__1__zxv"))
        );

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

        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(createMetadataFileBytes(metadata));

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
        BytesStreamOutput output = new BytesStreamOutput();
        IndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
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

        ByteArrayIndexInput expectedMetadataFileContent = createMetadataFileBytes(Map.of("_0.si", metadataString));
        int expectedBytesLength = (int) expectedMetadataFileContent.length();
        byte[] expectedBytes = new byte[expectedBytesLength];
        expectedMetadataFileContent.readBytes(expectedBytes, 0, expectedBytesLength);

        assertArrayEquals(expectedBytes, BytesReference.toBytes(output.bytes()));
    }

    public void testNoMetadataHeaderCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            metadataFiles
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234");
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345");

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        indexOutput.writeMapOfStrings(metadata);
        indexOutput.close();
        ByteArrayIndexInput byteArrayIndexInput = new ByteArrayIndexInput("segment metadata", BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(byteArrayIndexInput);

        assertThrows(CorruptIndexException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testInvalidCodecHeaderCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            metadataFiles
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234");
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345");

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        CodecUtil.writeHeader(indexOutput, "invalidCodec", RemoteSegmentMetadata.CURRENT_VERSION);
        indexOutput.writeMapOfStrings(metadata);
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        ByteArrayIndexInput byteArrayIndexInput = new ByteArrayIndexInput("segment metadata", BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(byteArrayIndexInput);

        assertThrows(CorruptIndexException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testHeaderMinVersionCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            metadataFiles
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234");
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345");

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        CodecUtil.writeHeader(indexOutput, RemoteSegmentMetadata.METADATA_CODEC, -1);
        indexOutput.writeMapOfStrings(metadata);
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        ByteArrayIndexInput byteArrayIndexInput = new ByteArrayIndexInput("segment metadata", BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(byteArrayIndexInput);

        assertThrows(IndexFormatTooOldException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testHeaderMaxVersionCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            metadataFiles
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234");
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345");

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        CodecUtil.writeHeader(indexOutput, RemoteSegmentMetadata.METADATA_CODEC, 2);
        indexOutput.writeMapOfStrings(metadata);
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        ByteArrayIndexInput byteArrayIndexInput = new ByteArrayIndexInput("segment metadata", BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(byteArrayIndexInput);

        assertThrows(IndexFormatTooNewException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testIncorrectChecksumCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(remoteMetadataDirectory.listFilesByPrefix(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX)).thenReturn(
            metadataFiles
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234");
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345");

        BytesStreamOutput output = new BytesStreamOutput();
        IndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        IndexOutput wrappedIndexOutput = new WrapperIndexOutput(indexOutput);
        IndexOutput indexOutputSpy = spy(wrappedIndexOutput);
        CodecUtil.writeHeader(indexOutputSpy, RemoteSegmentMetadata.METADATA_CODEC, RemoteSegmentMetadata.CURRENT_VERSION);
        indexOutputSpy.writeMapOfStrings(metadata);
        doReturn(12345L).when(indexOutputSpy).getChecksum();
        CodecUtil.writeFooter(indexOutputSpy);
        indexOutputSpy.close();

        ByteArrayIndexInput byteArrayIndexInput = new ByteArrayIndexInput("segment metadata", BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.openInput("metadata__1__5__abc", IOContext.DEFAULT)).thenReturn(byteArrayIndexInput);

        assertThrows(CorruptIndexException.class, () -> remoteSegmentStoreDirectory.init());
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

    public void testSegmentMetadataCurrentVersion() {
        /*
          This is a fake test which will fail whenever the CURRENT_VERSION is incremented.
          This is to bring attention of the author towards backward compatibility of metadata files.
          If there is any breaking change the author needs to specify how old metadata file will be supported after
          this change
          If author doesn't want to support old metadata files. Then this can be ignored.
          After taking appropriate action, fix this test by setting the correct version here
         */
        assertEquals(RemoteSegmentMetadata.CURRENT_VERSION, 1);
    }

    private static class WrapperIndexOutput extends IndexOutput {
        public IndexOutput indexOutput;

        public WrapperIndexOutput(IndexOutput indexOutput) {
            super(indexOutput.toString(), indexOutput.getName());
            this.indexOutput = indexOutput;
        }

        @Override
        public final void writeByte(byte b) throws IOException {
            this.indexOutput.writeByte(b);
        }

        @Override
        public final void writeBytes(byte[] b, int offset, int length) throws IOException {
            this.indexOutput.writeBytes(b, offset, length);
        }

        @Override
        public void writeShort(short i) throws IOException {
            this.indexOutput.writeShort(i);
        }

        @Override
        public void writeInt(int i) throws IOException {
            this.indexOutput.writeInt(i);
        }

        @Override
        public void writeLong(long i) throws IOException {
            this.indexOutput.writeLong(i);
        }

        @Override
        public void close() throws IOException {
            this.indexOutput.close();
        }

        @Override
        public final long getFilePointer() {
            return this.indexOutput.getFilePointer();
        }

        @Override
        public long getChecksum() throws IOException {
            return this.indexOutput.getChecksum();
        }
    }
}
