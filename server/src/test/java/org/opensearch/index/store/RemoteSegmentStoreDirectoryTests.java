/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandler;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.mockito.Mockito;

import static org.opensearch.index.store.RemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH;
import static org.opensearch.index.store.RemoteSegmentStoreDirectory.MetadataFilenameUtils.SEPARATOR;
import static org.opensearch.test.RemoteStoreTestUtils.createMetadataFileBytes;
import static org.opensearch.test.RemoteStoreTestUtils.getDummyMetadata;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.anyLong;
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

public class RemoteSegmentStoreDirectoryTests extends IndexShardTestCase {
    private static final Logger logger = LogManager.getLogger(RemoteSegmentStoreDirectoryTests.class);
    private RemoteDirectory remoteDataDirectory;
    private RemoteDirectory remoteMetadataDirectory;
    private RemoteStoreMetadataLockManager mdLockManager;

    private RemoteSegmentStoreDirectory remoteSegmentStoreDirectory;
    private TestUploadListener testUploadTracker;
    private IndexShard indexShard;
    private SegmentInfos segmentInfos;
    private ThreadPool threadPool;

    private final String metadataFilename = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        23,
        34,
        1,
        1,
        "node-1"
    );

    private final String metadataFilenameDup = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        23,
        34,
        2,
        1,
        "node-2"
    );
    private final String metadataFilename2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        13,
        34,
        1,
        1,
        "node-1"
    );
    private final String metadataFilename3 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        10,
        38,
        34,
        1,
        1,
        "node-1"
    );
    private final String metadataFilename4 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        10,
        36,
        34,
        1,
        1,
        "node-1"
    );

    @Before
    public void setup() throws IOException {
        remoteDataDirectory = mock(RemoteDirectory.class);
        remoteMetadataDirectory = mock(RemoteDirectory.class);
        mdLockManager = mock(RemoteStoreMetadataLockManager.class);
        threadPool = mock(ThreadPool.class);
        testUploadTracker = new TestUploadListener();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();

        indexShard = newStartedShard(false, indexSettings, new NRTReplicationEngineFactory());
        remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            remoteDataDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            indexShard.shardId()
        );
        try (Store store = indexShard.store()) {
            segmentInfos = store.readLastCommittedSegmentsInfo();
        }

        when(threadPool.executor(ThreadPool.Names.REMOTE_PURGE)).thenReturn(executorService);
        when(threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY)).thenReturn(executorService);
        when(threadPool.executor(ThreadPool.Names.SAME)).thenReturn(executorService);
    }

    @After
    public void tearDown() throws Exception {
        indexShard.close("test tearDown", true, false);
        super.tearDown();
    }

    public void testUploadedSegmentMetadataToString() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata = new RemoteSegmentStoreDirectory.UploadedSegmentMetadata(
            "abc",
            "pqr",
            "123456",
            1234
        );
        metadata.setWrittenByMajor(Version.LATEST.major);
        assertEquals("abc::pqr::123456::1234::" + Version.LATEST.major, metadata.toString());
    }

    public void testUploadedSegmentMetadataToStringExceptionTooNew() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata = new RemoteSegmentStoreDirectory.UploadedSegmentMetadata(
            "abc",
            "pqr",
            "123456",
            1234
        );
        assertThrows(IllegalArgumentException.class, () -> metadata.setWrittenByMajor(Version.LATEST.major + 1));
    }

    public void testUploadedSegmentMetadataToStringExceptionTooOld() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata = new RemoteSegmentStoreDirectory.UploadedSegmentMetadata(
            "abc",
            "pqr",
            "123456",
            1234
        );
        assertThrows(IllegalArgumentException.class, () -> metadata.setWrittenByMajor(Version.LATEST.major - 2));
    }

    public void testUploadedSegmentMetadataFromString() {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata = RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString(
            "_0.cfe::_0.cfe__uuidxyz::4567::372000::" + Version.LATEST.major
        );
        assertEquals("_0.cfe::_0.cfe__uuidxyz::4567::372000::" + Version.LATEST.major, metadata.toString());
    }

    public void testUploadedSegmentMetadataFromStringException() {
        assertThrows(
            ArrayIndexOutOfBoundsException.class,
            () -> RemoteSegmentStoreDirectory.UploadedSegmentMetadata.fromString("_0.cfe::_0.cfe__uuidxyz::4567::372000")
        );
    }

    public void testGetPrimaryTermGenerationUuid() {
        String[] filenameTokens = "abc__9223372036854775795__9223372036854775784__uuid_xyz".split(SEPARATOR);
        assertEquals(12, RemoteSegmentStoreDirectory.MetadataFilenameUtils.getPrimaryTerm(filenameTokens));
        assertEquals(23, RemoteSegmentStoreDirectory.MetadataFilenameUtils.getGeneration(filenameTokens));
    }

    public void testInitException() throws IOException {
        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH)).thenThrow(
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

    public void testInitMultipleMetadataFile() throws IOException {
        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH)).thenReturn(
            List.of(metadataFilename, metadataFilenameDup)
        );
        assertThrows(IllegalStateException.class, () -> remoteSegmentStoreDirectory.init());
    }

    private Map<String, Map<String, String>> populateMetadata() throws IOException {
        List<String> metadataFiles = new ArrayList<>();

        metadataFiles.add(metadataFilename);
        metadataFiles.add(metadataFilename2);
        metadataFiles.add(metadataFilename3);

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(List.of(metadataFilename));
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                Integer.MAX_VALUE
            )
        ).thenReturn(metadataFiles);

        Map<String, Map<String, String>> metadataFilenameContentMapping = Map.of(
            metadataFilename,
            getDummyMetadata("_0", 1),
            metadataFilename2,
            getDummyMetadata("_0", 1),
            metadataFilename3,
            getDummyMetadata("_0", 1)
        );

        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenAnswer(
            I -> createMetadataFileBytes(
                metadataFilenameContentMapping.get(metadataFilename),
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename2)).thenAnswer(
            I -> createMetadataFileBytes(
                metadataFilenameContentMapping.get(metadataFilename2),
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );
        when(remoteMetadataDirectory.getBlobStream(metadataFilename3)).thenAnswer(
            I -> createMetadataFileBytes(
                metadataFilenameContentMapping.get(metadataFilename3),
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );

        return metadataFilenameContentMapping;
    }

    public void testInit() throws IOException {
        populateMetadata();

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(List.of(metadataFilename));

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

    public void testFileLength() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertTrue(uploadedSegments.containsKey("_0.si"));

        assertEquals(uploadedSegments.get("_0.si").getLength(), remoteSegmentStoreDirectory.fileLength("_0.si"));
    }

    public void testFileLengthNoSuchFile() throws IOException {
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
        when(remoteDataDirectory.openInput(startsWith("_0.si"), anyLong(), eq(IOContext.DEFAULT))).thenReturn(indexInput);

        assertEquals(indexInput, remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testOpenInputNoSuchFile() {
        assertThrows(NoSuchFileException.class, () -> remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testOpenInputException() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        when(remoteDataDirectory.openInput(startsWith("_0.si"), anyLong(), eq(IOContext.DEFAULT))).thenThrow(new IOException("Error"));

        assertThrows(IOException.class, () -> remoteSegmentStoreDirectory.openInput("_0.si", IOContext.DEFAULT));
    }

    public void testAcquireLock() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        String acquirerId = "test-acquirer";
        long testPrimaryTerm = 1;
        long testGeneration = 5;

        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilePrefixForCommit(testPrimaryTerm, testGeneration),
                1
            )
        ).thenReturn(metadataFiles);

        remoteSegmentStoreDirectory.acquireLock(testPrimaryTerm, testGeneration, acquirerId);
        verify(mdLockManager).acquire(any());
    }

    public void testAcquireLockNoSuchFile() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        String testAcquirerId = "test-acquirer";
        long testPrimaryTerm = 2;
        long testGeneration = 3;

        assertThrows(
            NoSuchFileException.class,
            () -> remoteSegmentStoreDirectory.acquireLock(testPrimaryTerm, testGeneration, testAcquirerId)
        );
    }

    public void testReleaseLock() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        String testAcquirerId = "test-acquirer";
        long testPrimaryTerm = 1;
        long testGeneration = 5;

        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilePrefixForCommit(testPrimaryTerm, testGeneration),
                1
            )
        ).thenReturn(metadataFiles);

        remoteSegmentStoreDirectory.releaseLock(testPrimaryTerm, testGeneration, testAcquirerId);
        verify(mdLockManager).release(any());
    }

    public void testIsAcquired() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        long testPrimaryTerm = 1;
        long testGeneration = 5;

        List<String> metadataFiles = List.of("metadata__1__5__abc");
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilePrefixForCommit(testPrimaryTerm, testGeneration),
                1
            )
        ).thenReturn(metadataFiles);

        remoteSegmentStoreDirectory.isLockAcquired(testPrimaryTerm, testGeneration);
        verify(mdLockManager).isAcquired(any());
    }

    public void testIsAcquiredException() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        long testPrimaryTerm = 1;
        long testGeneration = 5;

        List<String> metadataFiles = new ArrayList<>();
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilePrefixForCommit(testPrimaryTerm, testGeneration),
                1
            )
        ).thenReturn(metadataFiles);

        assertThrows(NoSuchFileException.class, () -> remoteSegmentStoreDirectory.isLockAcquired(testPrimaryTerm, testGeneration));
    }

    private List<String> getDummyMetadataFiles(int count) {
        List<String> sortedMetadataFiles = new ArrayList<>();
        for (int counter = 0; counter < count; counter++) {
            sortedMetadataFiles.add(RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(counter, 23, 34, 1, 1, "node-1"));
        }
        return sortedMetadataFiles;
    }

    public void testGetMetadataFilesForActiveSegments() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        // scenario 1: if activeSegments([[0, 1, 2], 3(l), 4(l), 5(l), 6(l), 7(l), 8(l), 9]) => [9]
        List<String> sortedMdFiles = getDummyMetadataFiles(10);
        Set<String> lockedMdFiles = new HashSet<>();
        for (int idx = 3; idx <= 8; idx++) {
            lockedMdFiles.add(sortedMdFiles.get(idx));
        }
        Set<String> expectedMdFilesForActiveSegments = Set.of(sortedMdFiles.get(8));
        assertEquals(
            "scenario 1 failed",
            expectedMdFilesForActiveSegments,
            remoteSegmentStoreDirectory.getMetadataFilesToFilterActiveSegments(3, sortedMdFiles, lockedMdFiles)
        );

        // scenario 2: if activeSegments([[0, 1, 2], 3, 4, 5, 6(l), 7(l), 8(l), 9]) => [2, 6, 8]
        lockedMdFiles.clear();
        lockedMdFiles.add(sortedMdFiles.get(6));
        lockedMdFiles.add(sortedMdFiles.get(7));
        lockedMdFiles.add(sortedMdFiles.get(8));
        expectedMdFilesForActiveSegments = Set.of(sortedMdFiles.get(2), sortedMdFiles.get(6), sortedMdFiles.get(8));
        assertEquals(
            "scenario 2 failed",
            expectedMdFilesForActiveSegments,
            remoteSegmentStoreDirectory.getMetadataFilesToFilterActiveSegments(3, sortedMdFiles, lockedMdFiles)
        );

        // scenario 3: if activeSegments([[0, 1, 2], 3, 4, 5(l), 6, 7(l), 8(l), 9]) => [3, 5, 7, 8]
        lockedMdFiles.clear();
        lockedMdFiles.add(sortedMdFiles.get(5));
        lockedMdFiles.add(sortedMdFiles.get(7));
        lockedMdFiles.add(sortedMdFiles.get(8));
        expectedMdFilesForActiveSegments = Set.of(sortedMdFiles.get(2), sortedMdFiles.get(5), sortedMdFiles.get(7), sortedMdFiles.get(8));
        assertEquals(
            "scenario 3 failed",
            expectedMdFilesForActiveSegments,
            remoteSegmentStoreDirectory.getMetadataFilesToFilterActiveSegments(3, sortedMdFiles, lockedMdFiles)
        );

        // scenario 3: if activeSegments([[0(l), 1(l), 2(l), 3(l), 4(l), 5(l), 6(l), 7(l), 8(l), 9(l)])
        lockedMdFiles.addAll(sortedMdFiles);
        expectedMdFilesForActiveSegments = Set.of();
        assertEquals(
            "scenario 4 failed",
            expectedMdFilesForActiveSegments,
            remoteSegmentStoreDirectory.getMetadataFilesToFilterActiveSegments(0, sortedMdFiles, lockedMdFiles)
        );

        // scenario 5: if (activeSegments([[0, 1, 2, 3]]) => []
        sortedMdFiles = sortedMdFiles.subList(0, 4);
        lockedMdFiles.clear();
        expectedMdFilesForActiveSegments = Set.of();
        assertEquals(
            "scenario 5 failed",
            expectedMdFilesForActiveSegments,
            remoteSegmentStoreDirectory.getMetadataFilesToFilterActiveSegments(4, sortedMdFiles, lockedMdFiles)
        );
    }

    public void testGetMetadataFileForCommit() throws IOException {
        long testPrimaryTerm = 2;
        long testGeneration = 3;
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilePrefixForCommit(testPrimaryTerm, testGeneration),
                1
            )
        ).thenReturn(List.of("metadata__" + testPrimaryTerm + "__" + testGeneration + "__pqr"));

        String output = remoteSegmentStoreDirectory.getMetadataFileForCommit(testPrimaryTerm, testGeneration);
        assertEquals("metadata__" + testPrimaryTerm + "__" + testGeneration + "__pqr", output);
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

    public void testCopyFilesFromMultipart() throws Exception {
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

        AsyncMultiStreamBlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(remoteDataDirectory.getBlobContainer()).thenReturn(blobContainer);
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onResponse(null);
            return null;
        }).when(blobContainer).asyncBlobUpload(any(WriteContext.class), any());

        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Void> completionListener = new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {}
        };
        remoteSegmentStoreDirectory.copyFrom(storeDirectory, filename, IOContext.DEFAULT, completionListener);
        assertTrue(latch.await(5000, TimeUnit.SECONDS));
        assertTrue(remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().containsKey(filename));
        storeDirectory.close();
    }

    public void testCopyFilesFromMultipartIOException() throws Exception {
        String filename = "_100.si";
        AsyncMultiStreamBlobContainer blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        remoteDataDirectory = new RemoteDirectory(blobContainer);
        remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            remoteDataDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            indexShard.shardId()
        );

        populateMetadata();
        remoteSegmentStoreDirectory.init();

        Directory storeDirectory = LuceneTestCase.newDirectory();
        IndexOutput indexOutput = storeDirectory.createOutput(filename, IOContext.DEFAULT);
        indexOutput.writeString("Hello World!");
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        storeDirectory.sync(List.of(filename));

        assertFalse(remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().containsKey(filename));
        Mockito.doAnswer(invocation -> {
            ActionListener<Void> completionListener = invocation.getArgument(1);
            completionListener.onFailure(new Exception("Test exception"));
            return null;
        }).when(blobContainer).asyncBlobUpload(any(WriteContext.class), any());
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Void> completionListener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        };
        remoteSegmentStoreDirectory.copyFrom(storeDirectory, filename, IOContext.DEFAULT, completionListener);
        assertTrue(latch.await(5000, TimeUnit.SECONDS));
        assertFalse(remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore().containsKey(filename));

        storeDirectory.close();
    }

    public void testCleanupAsync() throws Exception {
        populateMetadata();
        RemoteSegmentStoreDirectoryFactory remoteSegmentStoreDirectoryFactory = mock(RemoteSegmentStoreDirectoryFactory.class);
        RemoteSegmentStoreDirectory remoteSegmentDirectory = new RemoteSegmentStoreDirectory(
            remoteDataDirectory,
            remoteMetadataDirectory,
            mdLockManager,
            threadPool,
            indexShard.shardId()
        );
        when(remoteSegmentStoreDirectoryFactory.newDirectory(any(), any(), any(), any())).thenReturn(remoteSegmentDirectory);
        String repositoryName = "test-repository";
        String indexUUID = "test-idx-uuid";
        ShardId shardId = new ShardId(Index.UNKNOWN_INDEX_NAME, indexUUID, Integer.parseInt("0"));
        RemoteStorePathStrategy pathStrategy = new RemoteStorePathStrategy(
            randomFrom(PathType.values()),
            randomFrom(PathHashAlgorithm.values())
        );

        RemoteSegmentStoreDirectory.remoteDirectoryCleanup(
            remoteSegmentStoreDirectoryFactory,
            repositoryName,
            indexUUID,
            shardId,
            pathStrategy
        );
        verify(remoteSegmentStoreDirectoryFactory).newDirectory(repositoryName, indexUUID, shardId, pathStrategy);
        verify(threadPool, times(0)).executor(ThreadPool.Names.REMOTE_PURGE);
        verify(remoteMetadataDirectory).delete();
        verify(remoteDataDirectory).delete();
        verify(mdLockManager).delete();
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
        List<String> metadataFiles = List.of(metadataFilename);
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(metadataFiles);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234::512::" + Version.LATEST.major);
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345::1024::" + Version.LATEST.major);

        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenReturn(
            createMetadataFileBytes(metadata, indexShard.getLatestReplicationCheckpoint(), segmentInfos)
        );

        remoteSegmentStoreDirectory.init();

        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegmentMetadataMap = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        assertThrows(
            UnsupportedOperationException.class,
            () -> uploadedSegmentMetadataMap.put(
                "_100.si",
                new RemoteSegmentStoreDirectory.UploadedSegmentMetadata("_100.si", "_100.si__uuid1", "1234", 500)
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
        final long primaryTerm = indexShard.getOperationPrimaryTerm();
        when(storeDirectory.createOutput(startsWith("metadata__" + primaryTerm + "__o"), eq(IOContext.DEFAULT))).thenReturn(indexOutput);

        Collection<String> segmentFiles = List.of("_s1.si", "_s1.cfe", "_s3.cfs");
        assertThrows(
            NoSuchFileException.class,
            () -> remoteSegmentStoreDirectory.uploadMetadata(
                segmentFiles,
                segmentInfos,
                storeDirectory,
                34L,
                indexShard.getLatestReplicationCheckpoint(),
                ""
            )
        );
    }

    public void testUploadMetadataNonEmpty() throws IOException {
        indexDocs(142364, 5);
        flushShard(indexShard, true);
        SegmentInfos segInfos = indexShard.store().readLastCommittedSegmentsInfo();
        long primaryTerm = indexShard.getLatestReplicationCheckpoint().getPrimaryTerm();
        String primaryTermLong = RemoteStoreUtils.invertLong(primaryTerm);
        long generation = segInfos.getGeneration();
        String generationLong = RemoteStoreUtils.invertLong(generation);
        String latestMetadataFileName = "metadata__" + primaryTermLong + "__" + generationLong + "__abc";
        List<String> metadataFiles = List.of(latestMetadataFileName);
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(metadataFiles);
        Map<String, Map<String, String>> metadataFilenameContentMapping = Map.of(
            latestMetadataFileName,
            getDummyMetadata("_0", (int) generation)
        );
        when(remoteMetadataDirectory.getBlobStream(latestMetadataFileName)).thenReturn(
            createMetadataFileBytes(
                metadataFilenameContentMapping.get(latestMetadataFileName),
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );

        remoteSegmentStoreDirectory.init();

        Directory storeDirectory = mock(Directory.class);
        BytesStreamOutput output = new BytesStreamOutput();
        IndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        when(storeDirectory.createOutput(startsWith("metadata__" + primaryTermLong + "__" + generationLong), eq(IOContext.DEFAULT)))
            .thenReturn(indexOutput);

        remoteSegmentStoreDirectory.uploadMetadata(
            segInfos.files(true),
            segInfos,
            storeDirectory,
            generation,
            indexShard.getLatestReplicationCheckpoint(),
            ""
        );

        verify(remoteMetadataDirectory).copyFrom(
            eq(storeDirectory),
            startsWith("metadata__" + primaryTermLong + "__" + generationLong),
            startsWith("metadata__" + primaryTermLong + "__" + generationLong),
            eq(IOContext.DEFAULT)
        );
        VersionedCodecStreamWrapper<RemoteSegmentMetadata> streamWrapper = new VersionedCodecStreamWrapper<>(
            new RemoteSegmentMetadataHandler(),
            RemoteSegmentMetadata.CURRENT_VERSION,
            RemoteSegmentMetadata.METADATA_CODEC
        );
        RemoteSegmentMetadata remoteSegmentMetadata = streamWrapper.readStream(
            new ByteArrayIndexInput("expected", BytesReference.toBytes(output.bytes()))
        );
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> actual = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> expected = remoteSegmentMetadata.getMetadata();
        for (String filename : expected.keySet()) {
            assertEquals(expected.get(filename).toString(), actual.get(filename).toString());
        }
    }

    public void testUploadMetadataMissingSegment() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();

        Directory storeDirectory = mock(Directory.class);
        IndexOutput indexOutput = mock(IndexOutput.class);

        String generation = RemoteStoreUtils.invertLong(segmentInfos.getGeneration());
        long primaryTermLong = indexShard.getLatestReplicationCheckpoint().getPrimaryTerm();
        String primaryTerm = RemoteStoreUtils.invertLong(primaryTermLong);
        when(storeDirectory.createOutput(startsWith("metadata__" + primaryTerm + "__" + generation), eq(IOContext.DEFAULT))).thenReturn(
            indexOutput
        );

        Collection<String> segmentFiles = List.of("_123.si");
        assertThrows(
            NoSuchFileException.class,
            () -> remoteSegmentStoreDirectory.uploadMetadata(
                segmentFiles,
                segmentInfos,
                storeDirectory,
                12L,
                indexShard.getLatestReplicationCheckpoint(),
                ""
            )
        );
        verify(indexOutput).close();
        verify(storeDirectory).deleteFile(startsWith("metadata__" + primaryTerm + "__" + generation));
    }

    public void testUploadMetadataNoSegmentCommitInfos() throws IOException {
        SegmentInfos segInfos = indexShard.store().readLastCommittedSegmentsInfo();
        int numSegCommitInfos = segInfos.size();
        assertEquals(
            "For a fresh index, the number of SegmentCommitInfo instances associated with the SegmentInfos instance should be 0, but were found to be "
                + numSegCommitInfos,
            0,
            numSegCommitInfos
        );
    }

    public void testNoMetadataHeaderCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of(metadataFilename);
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(metadataFiles);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234::" + Version.LATEST.major);
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345::" + Version.LATEST.major);

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        indexOutput.writeMapOfStrings(metadata);
        indexOutput.close();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenReturn(inputStream);

        assertThrows(CorruptIndexException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testInvalidCodecHeaderCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of(metadataFilename);
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(metadataFiles);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234::" + Version.LATEST.major);
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345::" + Version.LATEST.major);

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        CodecUtil.writeHeader(indexOutput, "invalidCodec", RemoteSegmentMetadata.CURRENT_VERSION);
        indexOutput.writeMapOfStrings(metadata);
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenReturn(inputStream);

        assertThrows(CorruptIndexException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testHeaderMinVersionCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of(metadataFilename);
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(metadataFiles);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234::" + Version.LATEST.major);
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345::" + Version.LATEST.major);

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        CodecUtil.writeHeader(indexOutput, RemoteSegmentMetadata.METADATA_CODEC, -1);
        indexOutput.writeMapOfStrings(metadata);
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenReturn(inputStream);

        assertThrows(IndexFormatTooOldException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testHeaderMaxVersionCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of(metadataFilename);
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(metadataFiles);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234::" + Version.LATEST.major);
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345::" + Version.LATEST.major);

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        CodecUtil.writeHeader(indexOutput, RemoteSegmentMetadata.METADATA_CODEC, 2);
        indexOutput.writeMapOfStrings(metadata);
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenReturn(inputStream);

        assertThrows(IndexFormatTooNewException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testIncorrectChecksumCorruptIndexException() throws IOException {
        List<String> metadataFiles = List.of(metadataFilename);
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH
            )
        ).thenReturn(metadataFiles);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("_0.cfe", "_0.cfe::_0.cfe__" + UUIDs.base64UUID() + "::1234::512::" + Version.LATEST.major);
        metadata.put("_0.cfs", "_0.cfs::_0.cfs__" + UUIDs.base64UUID() + "::2345::1024::" + Version.LATEST.major);

        BytesStreamOutput output = new BytesStreamOutput();
        IndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        IndexOutput wrappedIndexOutput = new WrapperIndexOutput(indexOutput);
        IndexOutput indexOutputSpy = spy(wrappedIndexOutput);
        CodecUtil.writeHeader(indexOutputSpy, RemoteSegmentMetadata.METADATA_CODEC, RemoteSegmentMetadata.CURRENT_VERSION);
        indexOutputSpy.writeMapOfStrings(metadata);
        doReturn(12345L).when(indexOutputSpy).getChecksum();
        CodecUtil.writeFooter(indexOutputSpy);
        indexOutputSpy.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(BytesReference.toBytes(output.bytes()));
        when(remoteMetadataDirectory.getBlobStream(metadataFilename)).thenReturn(inputStream);

        assertThrows(CorruptIndexException.class, () -> remoteSegmentStoreDirectory.init());
    }

    public void testDeleteStaleCommitsException() throws Exception {
        populateMetadata();
        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                Integer.MAX_VALUE
            )
        ).thenThrow(new IOException("Error reading"));

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here to validate that in case of exception deleteFile is not
        // invoked
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(2);

        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory, times(0)).deleteFile(any(String.class));
    }

    public void testDeleteStaleCommitsExceptionWhileScheduling() throws Exception {
        populateMetadata();
        doThrow(new IllegalArgumentException()).when(threadPool).executor(any(String.class));

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here to validate that in case of exception deleteFile is not
        // invoked
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(2);

        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory, times(0)).deleteFile(any(String.class));
    }

    public void testDeleteStaleCommitsWithDeletionAlreadyInProgress() throws Exception {
        populateMetadata();
        remoteSegmentStoreDirectory.canDeleteStaleCommits.set(false);

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here to validate that in case of exception deleteFile is not
        // invoked
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(2);

        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(false)));
        verify(remoteMetadataDirectory, times(0)).deleteFile(any(String.class));
    }

    public void testDeleteStaleCommitsWithinThreshold() throws Exception {
        populateMetadata();

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=5 here so that none of the metadata files will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(5);

        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory, times(0)).openInput(any(String.class), eq(IOContext.DEFAULT));
    }

    @TestLogging(value = "_root:debug", reason = "Validate logging output")
    public void testDeleteStaleCommitsActualDelete() throws Exception {
        try (final MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getRootLogger())) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "Metadata files to delete message",
                    "org.opensearch.index.store.RemoteSegmentStoreDirectory",
                    Level.DEBUG,
                    "metadataFilesEligibleToDelete=\\[" + metadataFilename3 + "\\] metadataFilesToBeDeleted=\\[" + metadataFilename3 + "\\]"
                )
            );

            final Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
            final List<String> filesToBeDeleted = metadataFilenameContentMapping.get(metadataFilename3)
                .values()
                .stream()
                .map(metadata -> metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1])
                .collect(Collectors.toList());

            remoteSegmentStoreDirectory.init();

            // popluateMetadata() adds stub to return 3 metadata files
            // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
            remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(2);

            for (final String file : filesToBeDeleted) {
                verify(remoteDataDirectory).deleteFile(file);
            }
            assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
            verify(remoteMetadataDirectory).deleteFile(metadataFilename3);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testDeleteStaleCommitsActualDeleteWithLocks() throws Exception {
        Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        remoteSegmentStoreDirectory.init();

        // Locking one of the metadata files to ensure that it is not getting deleted.
        when(mdLockManager.fetchLockedMetadataFiles(any())).thenReturn(Set.of(metadataFilename2));

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(1);

        for (String metadata : metadataFilenameContentMapping.get(metadataFilename3).values()) {
            String uploadedFilename = metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
            verify(remoteDataDirectory).deleteFile(uploadedFilename);
        }
        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory).deleteFile(metadataFilename3);
        verify(remoteMetadataDirectory, times(0)).deleteFile(metadataFilename2);
    }

    public void testDeleteStaleCommitsNoDeletesDueToLocks() throws Exception {
        remoteSegmentStoreDirectory.init();

        // Locking all the old metadata files to ensure that none of the segment files are getting deleted.
        when(mdLockManager.fetchLockedMetadataFiles(any())).thenReturn(Set.of(metadataFilename2, metadataFilename3));

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(1);

        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory, times(0)).deleteFile(any());
    }

    public void testDeleteStaleCommitsExceptionWhileFetchingLocks() throws Exception {
        remoteSegmentStoreDirectory.init();

        // Locking one of the metadata files to ensure that it is not getting deleted.
        when(mdLockManager.fetchLockedMetadataFiles(any())).thenThrow(new RuntimeException("Rate limit exceeded"));

        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(1);

        verify(remoteMetadataDirectory, times(0)).deleteFile(any());
    }

    public void testDeleteStaleCommitsDeleteDedup() throws Exception {
        Map<String, Map<String, String>> metadataFilenameContentMapping = new HashMap<>(populateMetadata());
        metadataFilenameContentMapping.put(metadataFilename4, metadataFilenameContentMapping.get(metadataFilename3));

        when(
            remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
                RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
                Integer.MAX_VALUE
            )
        ).thenReturn(new ArrayList<>(List.of(metadataFilename, metadataFilename2, metadataFilename3, metadataFilename4)));

        when(remoteMetadataDirectory.getBlobStream(metadataFilename4)).thenAnswer(
            I -> createMetadataFileBytes(
                metadataFilenameContentMapping.get(metadataFilename4),
                indexShard.getLatestReplicationCheckpoint(),
                segmentInfos
            )
        );

        remoteSegmentStoreDirectory.init();

        // popluateMetadata() adds stub to return 4 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 2 metadata files will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(2);

        Set<String> staleSegmentFiles = new HashSet<>();
        for (String metadata : metadataFilenameContentMapping.get(metadataFilename3).values()) {
            staleSegmentFiles.add(metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1]);
        }
        for (String metadata : metadataFilenameContentMapping.get(metadataFilename4).values()) {
            staleSegmentFiles.add(metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1]);
        }
        staleSegmentFiles.forEach(file -> {
            try {
                // Even with the same files in 2 stale metadata files, delete should be called only once.
                verify(remoteDataDirectory, times(1)).deleteFile(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory).deleteFile(metadataFilename3);
        verify(remoteMetadataDirectory).deleteFile(metadataFilename4);
    }

    public void testDeleteStaleCommitsActualDeleteIOException() throws Exception {
        Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        remoteSegmentStoreDirectory.init();

        String segmentFileWithException = metadataFilenameContentMapping.get(metadataFilename3)
            .values()
            .stream()
            .findAny()
            .get()
            .split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
        doThrow(new IOException("Error")).when(remoteDataDirectory).deleteFile(segmentFileWithException);
        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(2);

        for (String metadata : metadataFilenameContentMapping.get(metadataFilename3).values()) {
            String uploadedFilename = metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
            verify(remoteDataDirectory).deleteFile(uploadedFilename);
        }
        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory, times(0)).deleteFile(metadataFilename3);
    }

    public void testDeleteStaleCommitsActualDeleteNoSuchFileException() throws Exception {
        Map<String, Map<String, String>> metadataFilenameContentMapping = populateMetadata();
        remoteSegmentStoreDirectory.init();

        String segmentFileWithException = metadataFilenameContentMapping.get(metadataFilename)
            .values()
            .stream()
            .findAny()
            .get()
            .split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
        doThrow(new NoSuchFileException(segmentFileWithException)).when(remoteDataDirectory).deleteFile(segmentFileWithException);
        // popluateMetadata() adds stub to return 3 metadata files
        // We are passing lastNMetadataFilesToKeep=2 here so that oldest 1 metadata file will be deleted
        remoteSegmentStoreDirectory.deleteStaleSegmentsAsync(2);

        for (String metadata : metadataFilenameContentMapping.get(metadataFilename3).values()) {
            String uploadedFilename = metadata.split(RemoteSegmentStoreDirectory.UploadedSegmentMetadata.SEPARATOR)[1];
            verify(remoteDataDirectory).deleteFile(uploadedFilename);
        }
        assertBusy(() -> assertThat(remoteSegmentStoreDirectory.canDeleteStaleCommits.get(), is(true)));
        verify(remoteMetadataDirectory).deleteFile(metadataFilename3);
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

    private void indexDocs(int startDocId, int numberOfDocs) throws IOException {
        for (int i = startDocId; i < startDocId + numberOfDocs; i++) {
            indexDoc(indexShard, "_doc", Integer.toString(i));
        }
    }

    public void testMetadataFileNameOrder() {
        String file1 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(15, 21, 23, 1, 1, "");
        String file2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(15, 38, 38, 1, 1, "");
        String file3 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(18, 12, 26, 1, 1, "");
        String file4 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(15, 38, 32, 10, 1, "");
        String file5 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(15, 38, 32, 1, 1, "");
        String file6 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(15, 38, 32, 5, 1, "");

        List<String> actualList = new ArrayList<>(List.of(file1, file2, file3, file4, file5, file6));
        actualList.sort(String::compareTo);

        assertEquals(List.of(file3, file2, file4, file6, file5, file1), actualList);

        long count = file1.chars().filter(ch -> ch == SEPARATOR.charAt(0)).count();
        // There should not be any `_` in mdFile name as it is used a separator .
        assertEquals(14, count);
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
