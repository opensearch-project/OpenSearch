/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class FileTransferTrackerTests extends OpenSearchTestCase {

    protected final ShardId shardId = new ShardId("index", "_na_", 1);
    FileTransferTracker fileTransferTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testOnSuccess() throws IOException {
        fileTransferTracker = new FileTransferTracker(shardId);
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        try (
            FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
                testFile,
                randomNonNegativeLong()
            )
        ) {
            fileTransferTracker.onSuccess(transferFileSnapshot);
            // idempotent
            fileTransferTracker.onSuccess(transferFileSnapshot);
            assertEquals(fileTransferTracker.allUploaded().size(), 1);
            try {
                fileTransferTracker.onFailure(transferFileSnapshot, new IOException("random exception"));
                fail("failure after succcess invalid");
            } catch (IllegalStateException ex) {
                // all good
            }
        }
    }

    public void testOnFailure() throws IOException {
        fileTransferTracker = new FileTransferTracker(shardId);
        Path testFile = createTempFile();
        Path testFile2 = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        try (
            FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
                testFile,
                randomNonNegativeLong()
            );
            FileSnapshot.TransferFileSnapshot transferFileSnapshot2 = new FileSnapshot.TransferFileSnapshot(
                testFile2,
                randomNonNegativeLong()
            )
        ) {

            fileTransferTracker.onFailure(transferFileSnapshot, new IOException("random exception"));
            fileTransferTracker.onSuccess(transferFileSnapshot2);
            assertEquals(fileTransferTracker.allUploaded().size(), 1);

            fileTransferTracker.onSuccess(transferFileSnapshot);
            assertEquals(fileTransferTracker.allUploaded().size(), 2);
        }
    }

    public void testUploaded() throws IOException {
        fileTransferTracker = new FileTransferTracker(shardId);
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        try (
            FileSnapshot.TransferFileSnapshot transferFileSnapshot = new FileSnapshot.TransferFileSnapshot(
                testFile,
                randomNonNegativeLong()
            );

        ) {
            fileTransferTracker.onSuccess(transferFileSnapshot);
            String fileName = String.valueOf(testFile.getFileName());
            assertTrue(fileTransferTracker.uploaded(fileName));
            assertFalse(fileTransferTracker.uploaded("random-name"));

            fileTransferTracker.delete(List.of(fileName));
            assertFalse(fileTransferTracker.uploaded(fileName));
        }
    }

}
