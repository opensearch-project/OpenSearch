/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.blobstore.stream.StreamContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class RemoteTransferContainerTests extends OpenSearchTestCase {

    public void testSupplyStreamContextForPathDivisibleParts() throws IOException, InterruptedException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);

        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile,
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                true,
                WritePriority.HIGH
            )
        ) {
            testSupplyStreamContext(remoteTransferContainer, 16, 8);
        }
    }

    public void testSupplyStreamContextForDirectoryDivisibleParts() throws IOException, InterruptedException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);

        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                new NIOFSDirectory(testFile.getParent()),
                IOContext.DEFAULT,
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                true,
                WritePriority.HIGH
            )
        ) {
            testSupplyStreamContext(remoteTransferContainer, 16, 8);
        }
    }

    public void testSupplyStreamContextForPathNonDivisibleParts() throws IOException, InterruptedException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);

        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile,
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                true,
                WritePriority.HIGH
            )
        ) {
            testSupplyStreamContext(remoteTransferContainer, 10, 13);
        }
    }

    public void testSupplyStreamContextForDirectoryNonDivisibleParts() throws IOException, InterruptedException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(128), StandardOpenOption.APPEND);

        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                new NIOFSDirectory(testFile.getParent()),
                IOContext.DEFAULT,
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                true,
                WritePriority.HIGH
            )
        ) {
            testSupplyStreamContext(remoteTransferContainer, 10, 13);
        }
    }

    private void testSupplyStreamContext(RemoteTransferContainer remoteTransferContainer, long partSize, int expectedPartCount)
        throws InterruptedException {
        StreamContext streamContext = remoteTransferContainer.supplyStreamContext(partSize);
        assertEquals(expectedPartCount, streamContext.getNumberOfParts());
        Thread[] threads = new Thread[expectedPartCount];
        long totalContentLength = remoteTransferContainer.getContentLength();
        long lastPartSize = (totalContentLength % partSize == 0) ? partSize : (totalContentLength % partSize);
        logger.info("Part Size: {}, Last Part Size: {}, Number of Parts: {}", partSize, lastPartSize, streamContext.getNumberOfParts());
        for (int partIdx = 0; partIdx < expectedPartCount; partIdx++) {
            int finalPartIdx = partIdx;
            long expectedPartSize = (partIdx == expectedPartCount - 1) ? lastPartSize : partSize;
            threads[partIdx] = new Thread(
                () -> assertEquals(expectedPartSize, streamContext.getStreamProvider().provideStream(finalPartIdx).getContentLength())
            );
            threads[partIdx].start();
        }
        for (int i = 0; i < expectedPartCount; i++) {
            threads[i].join();
        }
    }
}
