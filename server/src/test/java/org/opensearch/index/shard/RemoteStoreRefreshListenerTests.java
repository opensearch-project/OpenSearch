/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.opensearch.index.shard.RemoteStoreRefreshListener.REMOTE_SEGMENTS_METADATA;

public class RemoteStoreRefreshListenerTests extends OpenSearchTestCase {
    private Directory storeDirectory;
    private Directory remoteDirectory;
    private ScheduledThreadPoolExecutor executor;

    private List<IndexInput> openedInputs;

    private RemoteStoreRefreshListener remoteStoreRefreshListener;

    public void setup(Set<String> remoteFiles, Set<String> localFiles) throws IOException {
        storeDirectory = newDirectory();
        remoteDirectory = mock(Directory.class);
        executor = mock(ScheduledThreadPoolExecutor.class);

        openedInputs = new ArrayList<>();
        writeSegmentFilesToDir(localFiles);

        IndexOutput output = storeDirectory.createOutput(REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);
        for (String remoteFile : remoteFiles) {
            StoreFileMetadata storeFileMetadata;
            if (localFiles.contains(remoteFile)) {
                IndexInput indexInput = storeDirectory.openInput(remoteFile, IOContext.DEFAULT);
                storeFileMetadata = new StoreFileMetadata(
                    remoteFile,
                    storeDirectory.fileLength(remoteFile),
                    Long.toString(CodecUtil.retrieveChecksum(indexInput)),
                    org.opensearch.Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion
                );
                openedInputs.add(indexInput);
            } else {
                storeFileMetadata = new StoreFileMetadata(
                    remoteFile,
                    scaledRandomIntBetween(10, 100),
                    Long.toString(scaledRandomIntBetween(10, 100)),
                    org.opensearch.Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion
                );
            }
            writeMetadata(output, storeFileMetadata);
        }
        output.close();

        if (remoteFiles.isEmpty()) {
            when(remoteDirectory.openInput(REMOTE_SEGMENTS_METADATA, IOContext.READ)).thenThrow(
                new NoSuchFileException(REMOTE_SEGMENTS_METADATA)
            );
        } else {
            IndexInput indexInput = storeDirectory.openInput(REMOTE_SEGMENTS_METADATA, IOContext.READ);
            when(remoteDirectory.openInput(REMOTE_SEGMENTS_METADATA, IOContext.READ)).thenReturn(indexInput);
            openedInputs.add(indexInput);
        }
        when(remoteDirectory.listAll()).thenReturn(remoteFiles.toArray(new String[0]));

        remoteStoreRefreshListener = new RemoteStoreRefreshListener(storeDirectory, remoteDirectory, executor);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        for (IndexInput indexInput : openedInputs) {
            indexInput.close();
        }
        storeDirectory.close();
    }

    private void writeSegmentFilesToDir(Set<String> files) throws IOException {
        for (String file : files) {
            IndexOutput output = storeDirectory.createOutput(file, IOContext.DEFAULT);
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            CodecUtil.writeFooter(output);
            output.close();
        }
    }

    private void writeMetadata(IndexOutput indexOutput, StoreFileMetadata storeFileMetadata) throws IOException {
        indexOutput.writeString(storeFileMetadata.name());
        indexOutput.writeVLong(storeFileMetadata.length());
        indexOutput.writeString(storeFileMetadata.checksum());
        indexOutput.writeString(storeFileMetadata.writtenBy().toString());
    }

    public void testAfterRefreshFalse() throws IOException {
        setup(Set.of(), Set.of());

        remoteStoreRefreshListener.afterRefresh(false);

        verify(remoteDirectory, times(0)).copyFrom(any(), any(), any(), any());
        verify(remoteDirectory, times(0)).copyFrom(storeDirectory, REMOTE_SEGMENTS_METADATA, REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);
        assertEquals(Set.of(), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testAfterRefreshTrueNoLocalFiles() throws IOException {
        setup(Set.of(), Set.of());

        remoteStoreRefreshListener.afterRefresh(true);

        verify(remoteDirectory, times(0)).copyFrom(any(), any(), any(), any());
        verify(remoteDirectory, times(0)).copyFrom(storeDirectory, REMOTE_SEGMENTS_METADATA, REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);
        assertEquals(Set.of(), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testAfterRefreshOnlyUploadFiles() throws IOException {
        setup(Set.of(), Set.of("0.si", "0.cfs", "0.cfe", "write.lock"));

        remoteStoreRefreshListener.afterRefresh(true);

        verify(remoteDirectory).copyFrom(storeDirectory, "0.si", "0.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.cfs", "0.cfs", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.cfe", "0.cfe", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, REMOTE_SEGMENTS_METADATA, REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);
        assertEquals(Set.of("0.si", "0.cfs", "0.cfe"), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testAfterRefreshUploadWithExistingRemoteFiles() throws IOException {
        setup(Set.of("0.si", "0.cfs"), Set.of("1.si", "1.cfs", "1.cfe"));

        remoteStoreRefreshListener.afterRefresh(true);

        verify(remoteDirectory).copyFrom(storeDirectory, "1.si", "1.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.cfs", "1.cfs", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.cfe", "1.cfe", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, REMOTE_SEGMENTS_METADATA, REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);
        assertEquals(Set.of("0.si", "0.cfs", "1.si", "1.cfs", "1.cfe"), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testAfterRefreshNoUpload() throws IOException {
        setup(Set.of("0.si", "0.cfs"), Set.of("0.si"));

        remoteStoreRefreshListener.afterRefresh(true);

        verify(remoteDirectory, times(0)).copyFrom(any(), any(), any(), any());
        verify(remoteDirectory, times(0)).copyFrom(storeDirectory, REMOTE_SEGMENTS_METADATA, REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);
        assertEquals(Set.of("0.si", "0.cfs"), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testAfterRefreshPartialUpload() throws IOException {
        setup(Set.of("0.si", "0.cfs"), Set.of("0.si", "1.si", "1.cfs"));

        remoteStoreRefreshListener.afterRefresh(true);

        verify(remoteDirectory).copyFrom(storeDirectory, "1.si", "1.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.cfs", "1.cfs", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, REMOTE_SEGMENTS_METADATA, REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);
        assertEquals(Set.of("0.si", "0.cfs", "1.si", "1.cfs"), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testDeleteStaleSegmentsNoStale() throws IOException {
        setup(Set.of("0.si", "0.cfs", REMOTE_SEGMENTS_METADATA), Set.of("0.si", "1.si", "0.cfs"));

        remoteStoreRefreshListener.deleteStaleSegments(Set.of("0.si", "1.si", "0.cfs"));

        verify(remoteDirectory, times(0)).deleteFile(any());
        assertEquals(Set.of("0.si", "0.cfs", REMOTE_SEGMENTS_METADATA), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testDeleteStaleSegmentsFewStale() throws IOException {
        setup(Set.of("0.si", "0.cfs"), Set.of("0.si", "1.si", "1.cfs"));

        remoteStoreRefreshListener.deleteStaleSegments(Set.of("0.si", "1.si", "1.cfs"));

        verify(remoteDirectory).deleteFile("0.cfs");
        assertEquals(Set.of("0.si"), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testDeleteStaleSegmentsAllStale() throws IOException {
        setup(Set.of("0.si", "0.cfs", REMOTE_SEGMENTS_METADATA), Set.of("2.si", "1.si", "1.cfs"));

        remoteStoreRefreshListener.deleteStaleSegments(Set.of("2.si", "1.si", "1.cfs"));

        verify(remoteDirectory).deleteFile("0.cfs");
        verify(remoteDirectory).deleteFile("0.si");
        verify(remoteDirectory, times(0)).deleteFile(REMOTE_SEGMENTS_METADATA);
        assertEquals(Set.of(REMOTE_SEGMENTS_METADATA), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testDeleteStaleSegmentsException() throws IOException {
        setup(Set.of("0.si", "0.cfs", "0.cfe"), Set.of("2.si", "1.si", "1.cfs"));
        doThrow(new IOException()).when(remoteDirectory).deleteFile("0.cfs");

        remoteStoreRefreshListener.deleteStaleSegments(Set.of("2.si", "1.si", "1.cfs"));

        verify(remoteDirectory).deleteFile("0.cfs");
        verify(remoteDirectory).deleteFile("0.si");
        verify(remoteDirectory).deleteFile("0.cfs");
        assertEquals(Set.of("0.cfs"), remoteStoreRefreshListener.getUploadedSegments().keySet());
    }

    public void testSchedulerFlow() throws IOException {
        setup(Set.of(), Set.of("0.si", "1.si", "2.si", "3.si", "4.si", "write.lock"));

        // First Refresh
        remoteStoreRefreshListener.afterRefresh(true);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.si", "0.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.si", "1.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "2.si", "2.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "3.si", "3.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "4.si", "4.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, REMOTE_SEGMENTS_METADATA, REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);

        assertEquals(Set.of("0.si", "1.si", "2.si", "3.si", "4.si"), remoteStoreRefreshListener.getUploadedSegments().keySet());
        Map<String, StoreFileMetadata> metadata = remoteStoreRefreshListener.readRemoteSegmentsMetadata(storeDirectory);
        assertEquals(Set.of("0.si", "1.si", "2.si", "3.si", "4.si"), metadata.keySet());

        // Second Refresh
        when(remoteDirectory.listAll()).thenReturn(remoteStoreRefreshListener.getUploadedSegments().keySet().toArray(new String[0]));
        writeSegmentFilesToDir(Set.of("5.si", "6.si", "7.si"));
        storeDirectory.deleteFile("0.si");
        storeDirectory.deleteFile("1.si");
        storeDirectory.deleteFile("2.si");

        remoteStoreRefreshListener.afterRefresh(true);

        verify(remoteDirectory).copyFrom(storeDirectory, "5.si", "5.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "6.si", "6.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "7.si", "7.si", IOContext.DEFAULT);

        assertEquals(
            Set.of("0.si", "1.si", "2.si", "3.si", "4.si", "5.si", "6.si", "7.si"),
            remoteStoreRefreshListener.getUploadedSegments().keySet()
        );
        metadata = remoteStoreRefreshListener.readRemoteSegmentsMetadata(storeDirectory);
        assertEquals(Set.of("3.si", "4.si", "5.si", "6.si", "7.si"), metadata.keySet());

        // Schedule flow
        when(remoteDirectory.listAll()).thenReturn(remoteStoreRefreshListener.getUploadedSegments().keySet().toArray(new String[0]));
        writeSegmentFilesToDir(Set.of("8.si", "9.si", "10.si"));
        storeDirectory.deleteFile("3.si");
        storeDirectory.deleteFile("6.si");

        Set<String> localFiles = Set.of("4.si", "5.si", "7.si", "8.si", "9.si", "10.si");

        // Scheduler flow - upload new segments
        remoteStoreRefreshListener.uploadNewSegments(localFiles);

        verify(remoteDirectory).copyFrom(storeDirectory, "8.si", "8.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "9.si", "9.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "10.si", "10.si", IOContext.DEFAULT);
        assertEquals(
            Set.of("0.si", "1.si", "2.si", "3.si", "4.si", "5.si", "6.si", "7.si", "8.si", "9.si", "10.si"),
            remoteStoreRefreshListener.getUploadedSegments().keySet()
        );
        metadata = remoteStoreRefreshListener.readRemoteSegmentsMetadata(storeDirectory);
        assertEquals(Set.of("3.si", "4.si", "5.si", "6.si", "7.si"), metadata.keySet());

        // Scheduler flow - delete stale segments
        when(remoteDirectory.listAll()).thenReturn(remoteStoreRefreshListener.getUploadedSegments().keySet().toArray(new String[0]));
        remoteStoreRefreshListener.deleteStaleSegments(localFiles);

        verify(remoteDirectory).deleteFile("0.si");
        verify(remoteDirectory).deleteFile("1.si");
        verify(remoteDirectory).deleteFile("2.si");
        verify(remoteDirectory).deleteFile("3.si");
        verify(remoteDirectory).deleteFile("6.si");
        assertEquals(Set.of("4.si", "5.si", "7.si", "8.si", "9.si", "10.si"), remoteStoreRefreshListener.getUploadedSegments().keySet());
        metadata = remoteStoreRefreshListener.readRemoteSegmentsMetadata(storeDirectory);
        assertEquals(Set.of("3.si", "4.si", "5.si", "6.si", "7.si"), metadata.keySet());

        remoteStoreRefreshListener.uploadRemoteSegmentsMetadata(localFiles);

        assertEquals(Set.of("4.si", "5.si", "7.si", "8.si", "9.si", "10.si"), remoteStoreRefreshListener.getUploadedSegments().keySet());
        metadata = remoteStoreRefreshListener.readRemoteSegmentsMetadata(storeDirectory);
        assertEquals(Set.of("4.si", "5.si", "7.si", "8.si", "9.si", "10.si"), metadata.keySet());
    }
}
