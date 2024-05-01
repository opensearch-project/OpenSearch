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

public class FileTransferTrackerTests extends OpenSearchTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);
    protected long primaryTerm = 10;
    protected long generation = 5;
    protected long minTranslogGeneration = 2;
    FileTransferTracker fileTransferTracker;
    RemoteTranslogTransferTracker remoteTranslogTransferTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 20);
        fileTransferTracker = new FileTransferTracker(shardId, remoteTranslogTransferTracker, false);
    }

    public void testOnSuccess() throws IOException {
        Path testFile = createTempFile();
        Path ckpFile = createTempFile();
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
        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>(2);
        toUpload.add(transferFileSnapshot);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        // idempotent
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        assertEquals(fileTransferTracker.allUploadedGeneration().size(), 1);
        try {
            remoteTranslogTransferTracker.addUploadBytesStarted(fileSize + ckpFileSize);
            fileTransferTracker.onFailure(transferFileSnapshot, new IOException("random exception"));
            fail("failure after succcess invalid");
        } catch (IllegalStateException ex) {
            // all good
        }
    }

    public void testOnFailure() throws IOException {
        Path testFile = createTempFile();
        Path testFile2 = createTempFile();
        int fileSize = 128;
        Files.write(testFile, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);

        TranslogCheckpointSnapshot transferFileSnapshot = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            testFile,
            testFile2,
            null,
            null,
            null,
            generation
        );
        TranslogCheckpointSnapshot transferFileSnapshot2 = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation + 1,
            minTranslogGeneration,
            testFile,
            testFile2,
            null,
            null,
            null,
            generation + 1
        );

        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>(2);
        toUpload.add(transferFileSnapshot);
        toUpload.add(transferFileSnapshot2);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize);
        fileTransferTracker.onFailure(transferFileSnapshot, new IOException("random exception"));
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot2);
        assertEquals(fileTransferTracker.allUploadedGeneration().size(), 1);
        remoteTranslogTransferTracker.addUploadBytesStarted(fileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        assertEquals(fileTransferTracker.allUploadedGeneration().size(), 2);

    }

    public void testOnSuccessStatsFailure() throws IOException {
        RemoteTranslogTransferTracker localRemoteTranslogTransferTracker = spy(remoteTranslogTransferTracker);
        doAnswer((count) -> { throw new NullPointerException("Error while updating stats"); }).when(localRemoteTranslogTransferTracker)
            .addUploadBytesSucceeded(anyLong());

        FileTransferTracker localFileTransferTracker = new FileTransferTracker(shardId, localRemoteTranslogTransferTracker, false);

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

        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>(2);
        toUpload.add(transferFileSnapshot);
        localFileTransferTracker.recordBytesForFiles(toUpload);
        localRemoteTranslogTransferTracker.addUploadBytesStarted(2 * fileSize);
        localFileTransferTracker.onSuccess(transferFileSnapshot);
        assertEquals(localFileTransferTracker.allUploadedGeneration().size(), 1);
    }

    public void testUploaded() throws IOException {
        Path testFile = createTempFile();
        Path ckpFile = createTempFile();
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

        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>(2);
        toUpload.add(transferFileSnapshot);
        fileTransferTracker.recordBytesForFiles(toUpload);
        remoteTranslogTransferTracker.addUploadBytesStarted(2 * fileSize);
        fileTransferTracker.onSuccess(transferFileSnapshot);
        String fileName = String.valueOf(testFile.getFileName());
        String ckpFileName = String.valueOf(ckpFile.getFileName());
        assertTrue(fileTransferTracker.translogGenerationUploaded(generation));
        assertFalse(fileTransferTracker.translogGenerationUploaded(generation + 2));

        fileTransferTracker.deleteGenerations(Set.of(generation));
        assertTrue(fileTransferTracker.uploaded(fileName));
        assertTrue(fileTransferTracker.uploaded(ckpFileName));
    }

}
