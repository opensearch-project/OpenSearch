/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.IndexInput;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.MultiFileWriter;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.common.bytes.BytesArray;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MultiFileWriterTests extends OpenSearchTestCase {

    private Store segRepEnabledStore, docRepEnabledStore;
    private Directory dir, spyDir;
    private ReplicationLuceneIndex indexState;
    private MultiFileWriter multiFileWriter;
    private ShardId shardId;

    final IndexSettings SEGREP_INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build()
    );

    final IndexSettings DOCREP_INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .build()
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = new NIOFSDirectory(createTempDir());
        spyDir = spy(dir);
        shardId = new ShardId("index", UUIDs.randomBase64UUID(), 0);
        segRepEnabledStore = new Store(shardId, SEGREP_INDEX_SETTINGS, spyDir, new DummyShardLock(shardId));
        docRepEnabledStore = new Store(shardId, DOCREP_INDEX_SETTINGS, spyDir, new DummyShardLock(shardId));
        indexState = new ReplicationLuceneIndex();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        dir.close();
        spyDir.close();
        docRepEnabledStore.close();
        segRepEnabledStore.close();
    }

    private void createFileWithChecksum(String fileName, BytesRef data) throws IOException {
        final IndexOutput output = dir.createOutput(fileName, IOContext.DEFAULT);
        output.writeBytes(data.bytes, data.length);
        CodecUtil.writeFooter(output);
        output.close();
    }

    // Read file's length and checksum, needed to build StoreFileMetadata for MFW constructor
    private BytesRef readFileBytes(String fileName, IndexInput indexInput) throws IOException {
        indexInput.seek(0);
        long fileLen = indexInput.length();
        final BytesRef fileBytes = new BytesRef((int) fileLen);
        indexInput.readBytes(fileBytes.bytes, 0, (int) fileLen);
        // delete file as MFW creates it for writing
        dir.deleteFile(fileName);
        return fileBytes;
    }

    public void testMFWSegrepCallsFsyncForIncomingCommitPoint() throws IOException {
        // Generate a file with random text and appending the checksum
        final String fileName = IndexFileNames.SEGMENTS;
        createFileWithChecksum(fileName, new BytesRef("random text"));

        final IndexInput indexInput = dir.openInput(fileName, IOContext.DEFAULT);
        final String checksum = Store.digestToString(CodecUtil.retrieveChecksum(indexInput));
        long fileLen = indexInput.length();
        final BytesRef fileBytes = readFileBytes(fileName, indexInput);
        indexInput.close();

        final StoreFileMetadata fileMetadata = new StoreFileMetadata(fileName, fileLen, checksum, Version.LATEST);
        indexState.addFileDetail(fileName, fileLen, false);
        multiFileWriter = new MultiFileWriter(segRepEnabledStore, indexState, "", logger, () -> {});
        multiFileWriter.writeFileChunk(fileMetadata, 0, new BytesArray(fileBytes.bytes), true);
        // file name here "segments" represents incoming commit point and should result in sync call
        verify(spyDir, times(1)).sync(any());
    }

    public void testMFWSegrepSkipsFsyncForRandomFiles() throws IOException {
        // Generate a file with random text and appending the checksum
        final String fileName = "random_file";
        createFileWithChecksum(fileName, new BytesRef("random text"));

        // Read file's length and checksum, needed to build StoreFileMetadata for MFW constructor
        final IndexInput indexInput = dir.openInput(fileName, IOContext.DEFAULT);
        final String checksum = Store.digestToString(CodecUtil.retrieveChecksum(indexInput));
        long fileLen = indexInput.length();
        final BytesRef fileBytes = readFileBytes(fileName, indexInput);
        indexInput.close();

        final StoreFileMetadata fileMetadata = new StoreFileMetadata(fileName, fileLen, checksum, Version.LATEST);
        indexState.addFileDetail(fileName, fileLen, false);
        multiFileWriter = new MultiFileWriter(segRepEnabledStore, indexState, "", logger, () -> {});
        multiFileWriter.writeFileChunk(fileMetadata, 0, new BytesArray(fileBytes.bytes), true);
        // A random file (not a segments file) should not result in sync call
        verify(spyDir, times(0)).sync(any());
    }

    public void testMFWDocrepCallsFsyncForIncomingCommitPoint() throws IOException {
        // Generate a file with random text and appending the checksum
        final String fileName = IndexFileNames.SEGMENTS;
        final BytesRef fileText = new BytesRef("random text");
        createFileWithChecksum(fileName, fileText);

        // Read file's length and checksum, needed to build StoreFileMetadata for MFW constructor
        final IndexInput indexInput = dir.openInput(fileName, IOContext.DEFAULT);
        final String checksum = Store.digestToString(CodecUtil.retrieveChecksum(indexInput));
        long fileLen = indexInput.length();
        final BytesRef fileBytes = readFileBytes(fileName, indexInput);
        indexInput.close();

        final StoreFileMetadata fileMetadata = new StoreFileMetadata(fileName, fileLen, checksum, Version.LATEST);
        indexState.addFileDetail(fileName, fileLen, false);
        multiFileWriter = new MultiFileWriter(docRepEnabledStore, indexState, "", logger, () -> {});
        multiFileWriter.writeFileChunk(fileMetadata, 0, new BytesArray(fileBytes.bytes), true);
        // no change in fsync calls for document replication
        verify(spyDir, times(1)).sync(any());
    }

    public void testMFWDocrepCallsFsyncForRandomFile() throws IOException {
        // Generate a file with random text and appending the checksum
        final String fileName = "random_file";
        final BytesRef fileText = new BytesRef("random text");
        createFileWithChecksum(fileName, fileText);

        // Read file's length and checksum, needed to build StoreFileMetadata for MFW constructor
        final IndexInput indexInput = dir.openInput(fileName, IOContext.DEFAULT);
        final String checksum = Store.digestToString(CodecUtil.retrieveChecksum(indexInput));
        long fileLen = indexInput.length();
        final BytesRef fileBytes = readFileBytes(fileName, indexInput);
        indexInput.close();

        final StoreFileMetadata fileMetadata = new StoreFileMetadata(fileName, fileLen, checksum, Version.LATEST);
        indexState.addFileDetail(fileName, fileLen, false);
        multiFileWriter = new MultiFileWriter(docRepEnabledStore, indexState, "", logger, () -> {});
        multiFileWriter.writeFileChunk(fileMetadata, 0, new BytesArray(fileBytes.bytes), true);
        // no change in fsync calls for document replication
        verify(spyDir, times(1)).sync(any());
    }
}
