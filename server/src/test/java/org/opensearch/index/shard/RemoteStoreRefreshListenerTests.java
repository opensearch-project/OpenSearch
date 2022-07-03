/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.store.RawDirectoryWrapper;
import org.junit.After;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.store.Store;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.startsWith;
import static org.opensearch.index.shard.RemoteStoreRefreshListener.REFRESHED_SEGMENTINFOS_FILENAME;

public class RemoteStoreRefreshListenerTests extends OpenSearchTestCase {
    private IndexShard indexShard;
    private Directory storeDirectory;
    private Directory remoteDirectory;

    private RemoteStoreRefreshListener remoteStoreRefreshListener;

    public void setup(int numberOfDocuments) throws IOException {
        indexShard = mock(IndexShard.class);
        storeDirectory = newDirectory();
        remoteDirectory = mock(Directory.class);

        if (numberOfDocuments > 0) {
            writeDocsToLocalDirectory(storeDirectory, numberOfDocuments);
        }
        ThreadPool threadPool = mock(ThreadPool.class);
        when(indexShard.getThreadPool()).thenReturn(threadPool);

        GatedCloseable<SegmentInfos> segmentInfosWrapper = mock(GatedCloseable.class);
        when(segmentInfosWrapper.get()).thenAnswer(x -> {
            try {
                return SegmentInfos.readLatestCommit(storeDirectory);
            } catch (IndexNotFoundException e) {
                throw (new EngineException(new ShardId("a", "b", 0), "Error"));
            }
        });
        when(indexShard.getSegmentInfosSnapshot()).thenReturn(segmentInfosWrapper);
        when(remoteDirectory.listAll()).thenReturn(new String[0]);

        Store store = mock(Store.class);
        when(store.directory()).thenReturn(storeDirectory);
        when(indexShard.store()).thenReturn(store);

        Store remoteStore = mock(Store.class);
        when(remoteStore.directory()).thenReturn(new RawDirectoryWrapper(new RawDirectoryWrapper(remoteDirectory)));
        when(indexShard.remoteStore()).thenReturn(remoteStore);

        remoteStoreRefreshListener = new RemoteStoreRefreshListener(indexShard);
    }

    private void writeDocsToLocalDirectory(Directory storeDirectory, int numberOfFiles) throws IOException {
        IndexWriter writer = new IndexWriter(
            storeDirectory,
            new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
        );
        for (int i = 0; i < numberOfFiles; i++) {
            Document document = new Document();
            document.add(new StringField("field" + i, "value" + i, Field.Store.NO));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        storeDirectory.close();
    }

//    public void testReadRemoteSegmentsMetadata() throws IOException {
//        setup(3);
//
//        String[] localFiles = storeDirectory.listAll();
//        String[] remoteFiles = new String[localFiles.length + 1];
//        int i = 0;
//        for (String file : localFiles) {
//            remoteFiles[i] = file;
//            i++;
//        }
//        remoteFiles[i] = "random_segment";
//        when(remoteDirectory.listAll()).thenReturn(remoteFiles);
//
//        ConcurrentHashMap<String, StoreFileMetadata> actualMetadata = remoteStoreRefreshListener.readRemoteSegmentsMetadata(
//            storeDirectory,
//            remoteDirectory
//        );
//        for (Map.Entry<String, StoreFileMetadata> entry : actualMetadata.entrySet()) {
//            String file = entry.getKey();
//            StoreFileMetadata fileMetadata = entry.getValue();
//            assertEquals(file, fileMetadata.name());
//            assertEquals(storeDirectory.fileLength(file), fileMetadata.length());
//            try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.DEFAULT)) {
//                assertEquals(CodecUtil.retrieveChecksum(indexInput), Long.parseLong(fileMetadata.checksum()));
//            }
//        }
//        assertFalse(actualMetadata.containsKey("random_segment"));
//    }

//    public void testDeleteStaleSegmentsNoPriorRefresh() throws IOException {
//        setup(4);
//        when(remoteDirectory.listAll()).thenReturn(storeDirectory.listAll());
//
//        remoteStoreRefreshListener.deleteStaleSegments(localFiles);
//
//        verify(remoteDirectory, times(0)).openChecksumInput(any(), any());
//    }
//
//    public void testDeleteStaleSegmentsPostRefreshNoDelete() throws IOException {
//        setup(4);
//
//        remoteStoreRefreshListener.afterRefresh(true);
//        when(remoteDirectory.listAll()).thenReturn(storeDirectory.listAll());
//        for (String file : storeDirectory.listAll()) {
//            when(remoteDirectory.openChecksumInput(file, IOContext.READ)).thenAnswer(
//                i -> storeDirectory.openChecksumInput(file, IOContext.READ)
//            );
//        }
//        remoteStoreRefreshListener.deleteStaleSegments(localFiles);
//
//        verify(remoteDirectory, times(0)).openChecksumInput(startsWith(REFRESHED_SEGMENTINFOS_FILENAME), eq(IOContext.DEFAULT));
//        verify(remoteDirectory, times(0)).deleteFile(any());
//    }
//
//    public void testDeleteStaleSegmentsPostRefreshDelete() throws IOException {
//        setup(4);
//        remoteStoreRefreshListener.afterRefresh(true);
//        writeDocsToLocalDirectory(storeDirectory, 3);
//        remoteStoreRefreshListener.afterRefresh(true);
//        writeDocsToLocalDirectory(storeDirectory, 5);
//        remoteStoreRefreshListener.afterRefresh(true);
//
//        String[] localFiles = storeDirectory.listAll();
//        String[] remoteFiles = new String[localFiles.length + 1];
//        int i = 0;
//        for (String file : localFiles) {
//            remoteFiles[i] = file;
//            i++;
//        }
//        remoteFiles[i] = "random_segment";
//        when(remoteDirectory.listAll()).thenReturn(remoteFiles);
//
//        for (String file : storeDirectory.listAll()) {
//            when(remoteDirectory.openChecksumInput(file, IOContext.READ)).thenAnswer(
//                j -> storeDirectory.openChecksumInput(file, IOContext.READ)
//            );
//        }
//        remoteStoreRefreshListener.deleteStaleSegments(localFiles);
//
//        verify(remoteDirectory, times(0)).openChecksumInput(startsWith(REFRESHED_SEGMENTINFOS_FILENAME), eq(IOContext.DEFAULT));
//        verify(remoteDirectory, times(1)).deleteFile("random_segment");
//        verify(remoteDirectory, times(1)).deleteFile(REFRESHED_SEGMENTINFOS_FILENAME + 1);
//        verify(remoteDirectory, times(1)).deleteFile(REFRESHED_SEGMENTINFOS_FILENAME + 2);
//    }

    public void testAfterRefreshFalse() throws IOException {
        setup(0);

        remoteStoreRefreshListener.afterRefresh(false);

        verify(indexShard, times(0)).getSegmentInfosSnapshot();
        verify(remoteDirectory, times(0)).copyFrom(any(), any(), any(), any());
        verify(remoteDirectory, times(0)).copyFrom(eq(storeDirectory), any(), any(), eq(IOContext.DEFAULT));
        assertEquals(Set.of(), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testAfterRefreshTrueNoLocalFiles() throws IOException {
        setup(0);

        remoteStoreRefreshListener.afterRefresh(true);

        verify(indexShard).getSegmentInfosSnapshot();
        verify(remoteDirectory, times(0)).copyFrom(any(), any(), any(), any());
        assertEquals(Set.of(), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testAfterRefreshOnlyUploadFiles() throws IOException {
        setup(3);

        remoteStoreRefreshListener.afterRefresh(true);

        Set<String> localFiles = Arrays.stream(storeDirectory.listAll())
            .filter(file -> !file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .collect(Collectors.toSet());
        for (String file : localFiles) {
            verify(remoteDirectory).copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
        }
        assertEquals(localFiles, remoteStoreRefreshListener.getUploadedSegments().keySet());
        verify(remoteDirectory).copyFrom(
            eq(storeDirectory),
            startsWith(REFRESHED_SEGMENTINFOS_FILENAME),
            startsWith(REFRESHED_SEGMENTINFOS_FILENAME),
            eq(IOContext.DEFAULT)
        );
    }

    public void testAfterRefreshUploadWithExistingRemoteFiles() throws IOException {
        setup(3);
        remoteStoreRefreshListener.afterRefresh(true);

        Set<String> filesAfterFirstCommit = Arrays.stream(storeDirectory.listAll())
            .filter(file -> !file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .collect(Collectors.toSet());
        for (String file : filesAfterFirstCommit) {
            verify(remoteDirectory).copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
        }
        assertEquals(filesAfterFirstCommit, remoteStoreRefreshListener.getUploadedSegments().keySet());

        writeDocsToLocalDirectory(storeDirectory, 4);
        remoteStoreRefreshListener.afterRefresh(true);

        Set<String> filesAfterSecondCommit = Arrays.stream(storeDirectory.listAll())
            .filter(file -> !file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .collect(Collectors.toSet());
        filesAfterSecondCommit.add("segments_1");

        filesAfterSecondCommit.stream().filter(file -> !filesAfterFirstCommit.contains(file)).forEach(file -> {
            try {
                verify(remoteDirectory).copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(filesAfterSecondCommit, remoteStoreRefreshListener.getUploadedSegments().keySet());
        verify(remoteDirectory, times(2)).copyFrom(
            eq(storeDirectory),
            startsWith(REFRESHED_SEGMENTINFOS_FILENAME),
            startsWith(REFRESHED_SEGMENTINFOS_FILENAME),
            eq(IOContext.DEFAULT)
        );
    }

    public void testAfterRefreshNoUpload() throws IOException {
        setup(3);
        remoteStoreRefreshListener.afterRefresh(true);

        Set<String> filesAfterFirstCommit = Arrays.stream(storeDirectory.listAll())
            .filter(file -> !file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .collect(Collectors.toSet());
        assertEquals(filesAfterFirstCommit, remoteStoreRefreshListener.getUploadedSegments().keySet());

        remoteStoreRefreshListener.afterRefresh(true);

        assertEquals(filesAfterFirstCommit, remoteStoreRefreshListener.getUploadedSegments().keySet());
        for (String file : filesAfterFirstCommit) {
            verify(remoteDirectory, times(1)).copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
        }
        verify(remoteDirectory, times(1)).copyFrom(
            eq(storeDirectory),
            startsWith(REFRESHED_SEGMENTINFOS_FILENAME),
            startsWith(REFRESHED_SEGMENTINFOS_FILENAME),
            eq(IOContext.DEFAULT)
        );
    }

    public void testAfterRefreshPartialUpload() throws IOException {
        setup(3);

        Set<String> segmentsWithException = new HashSet<>();
        for (String file : storeDirectory.listAll()) {
            if (randomBoolean()) {
                segmentsWithException.add(file);
                doThrow(new NoSuchFileException("Not Found")).when(remoteDirectory).copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
            }
        }
        remoteStoreRefreshListener.afterRefresh(true);

        for (String file : Arrays.stream(storeDirectory.listAll())
            .filter(file -> !file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .collect(Collectors.toSet())) {
            verify(remoteDirectory, times(1)).copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
        }
        assertEquals(
            Stream.of(storeDirectory.listAll()).filter(file -> !segmentsWithException.contains(file)).collect(Collectors.toSet()),
            remoteStoreRefreshListener.getUploadedSegments().keySet()
        );
        verify(remoteDirectory, times(0)).copyFrom(
            eq(storeDirectory),
            startsWith(REFRESHED_SEGMENTINFOS_FILENAME),
            startsWith(REFRESHED_SEGMENTINFOS_FILENAME),
            eq(IOContext.DEFAULT)
        );
    }
}
