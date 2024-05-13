/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class TranslogCkpFilesTransferTrackerTests extends OpenSearchTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);
    protected long primaryTerm = 10;
    protected long generation = 5;
    protected long minTranslogGeneration = 2;
    TranslogCkpFilesTransferTracker fileTransferTracker;
    RemoteTranslogTransferTracker remoteTranslogTransferTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 20);
        fileTransferTracker = new TranslogCkpFilesTransferTracker(shardId, remoteTranslogTransferTracker);
    }

    public void testOnSuccess() throws IOException {
        Path location = createTempDir();
        Path testFile = Files.createFile(location.resolve("translog-5.tlog"));
        Path ckpFile = Files.createFile(location.resolve("translog-5.ckp"));
        int fileSize = 128;
        int ckpFileSize = 100;
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        Files.write(ckpFile, randomByteArrayOfLength(ckpFileSize), StandardOpenOption.APPEND);
        TranslogCheckpointSnapshot transferFileSnapshot = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            testFile,
            ckpFile,
            null,
            null,
            null,
            generation
        );
        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        toUpload.add(transferFileSnapshot);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        // idempotent
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        assertEquals(fileTransferTracker.allUploadedGenerationSize(), 1);
        try {
            remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
            fileTransferTracker.onFailure(transferFileSnapshot, new IOException("random exception"));
            fail("failure after succcess invalid");
        } catch (IllegalStateException ex) {
            // all good
        }
    }

    public void testOnFailure() throws IOException {
        Path location = createTempDir();
        Path tlogFile1 = Files.createFile(location.resolve("translog-5.tlog"));
        Path ckpFile1 = Files.createFile(location.resolve("translog-5.ckp"));
        Path tlogFile2 = Files.createFile(location.resolve("translog-6.tlog"));
        Path ckpFile2 = Files.createFile(location.resolve("translog-6.ckp"));

        int fileSize = 128;
        Files.write(tlogFile1, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        Files.write(ckpFile1, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);

        Files.write(tlogFile2, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        Files.write(ckpFile2, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        TranslogCheckpointSnapshot translogCheckpointSnapshot1 = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            tlogFile1,
            ckpFile2,
            null,
            null,
            null,
            generation
        );
        TranslogCheckpointSnapshot translogCheckpointSnapshot2 = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation + 1,
            minTranslogGeneration,
            tlogFile2,
            ckpFile2,
            null,
            null,
            null,
            generation + 1
        );
        TransferFileSnapshot checkpointFileSnapshot1 = translogCheckpointSnapshot1.getCheckpointFileSnapshot();
        TransferFileSnapshot translogFileSnapshot1 = translogCheckpointSnapshot1.getTranslogFileSnapshot();
        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        toUpload.add(translogCheckpointSnapshot1);
        toUpload.add(translogCheckpointSnapshot2);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize * 4);
        fileTransferTracker.onFailure(
            translogCheckpointSnapshot1,
            new TranslogTransferException(
                translogCheckpointSnapshot1,
                new IOException("random exception"),
                Set.of(checkpointFileSnapshot1, translogFileSnapshot1),
                null
            )
        );
        fileTransferTracker.onSuccess(translogCheckpointSnapshot2);
        assertEquals(fileTransferTracker.allUploadedGenerationSize(), 1);

        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize * 2);
        fileTransferTracker.onSuccess(translogCheckpointSnapshot1);
        assertEquals(fileTransferTracker.allUploadedGenerationSize(), 2);

        checkpointFileSnapshot1.close();
        translogFileSnapshot1.close();
    }

    public void testOnSuccessStatsFailure() throws IOException {
        RemoteTranslogTransferTracker localRemoteTranslogTransferTracker = spy(remoteTranslogTransferTracker);
        doAnswer((count) -> { throw new NullPointerException("Error while updating stats"); }).when(localRemoteTranslogTransferTracker)
            .addUploadBytesSucceeded(anyLong());

        FileTransferTracker localFileTransferTracker = FileTransferTrackerFactory.getFileTransferTracker(
            shardId,
            localRemoteTranslogTransferTracker,
            false
        );

        Path testFile = createTempFile();
        int fileSize = 128;
        Files.write(testFile, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);

        TranslogCheckpointSnapshot transferFileSnapshot = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            testFile,
            testFile,
            null,
            null,
            null,
            generation
        );

        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        toUpload.add(transferFileSnapshot);
        localFileTransferTracker.recordBytesForFiles(toUpload);
        localRemoteTranslogTransferTracker.addUploadBytesStarted(2 * fileSize);
        localFileTransferTracker.onSuccess(transferFileSnapshot);
        assertEquals(localFileTransferTracker.allUploadedGenerationSize(), 1);
    }

    public void testUploaded() throws IOException {
        Path testFile = createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.TRANSLOG_FILE_SUFFIX);
        Path ckpFile = createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.CHECKPOINT_SUFFIX);
        int fileSize = 128;
        Files.write(testFile, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        TranslogCheckpointSnapshot transferFileSnapshot = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            testFile,
            ckpFile,
            null,
            null,
            null,
            generation
        );

        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        toUpload.add(transferFileSnapshot);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(2 * fileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        String tlogFileName = String.valueOf(testFile.getFileName());
        String ckpFileName = String.valueOf(ckpFile.getFileName());
        assertTrue(fileTransferTracker.uploaded(tlogFileName));
        assertTrue(fileTransferTracker.uploaded(ckpFileName));
        assertTrue(fileTransferTracker.uploaded(generation));
        assertFalse(fileTransferTracker.uploaded(generation + 2));
        assertFalse(fileTransferTracker.uploaded("random-name"));

        fileTransferTracker.deleteGenerations(Set.of(generation));
        assertFalse(fileTransferTracker.uploaded(generation));

        fileTransferTracker.deleteGenerations(Set.of(generation));
        assertFalse(fileTransferTracker.uploaded(Long.toString(generation)));

    }

}
