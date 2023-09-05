/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import org.junit.Before;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeFileInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.blobstore.transfer.stream.ResettableCheckedInputStream;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

public class RemoteTransferContainerTests extends OpenSearchTestCase {

    private static final int TEST_FILE_SIZE_BYTES = 128;

    private Path testFile;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(TEST_FILE_SIZE_BYTES), StandardOpenOption.APPEND);
    }

    public void testSupplyStreamContextDivisibleParts() throws IOException, InterruptedException {
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                TEST_FILE_SIZE_BYTES,
                true,
                WritePriority.HIGH,
                new RemoteTransferContainer.OffsetRangeInputStreamSupplier() {
                    @Override
                    public OffsetRangeInputStream get(long size, long position) throws IOException {
                        return new OffsetRangeFileInputStream(testFile, size, position);
                    }
                },
                0,
                false
            )
        ) {
            testSupplyStreamContext(remoteTransferContainer, 16, 16, 8);
        }
    }

    public void testSupplyStreamContextNonDivisibleParts() throws IOException, InterruptedException {
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                TEST_FILE_SIZE_BYTES,
                true,
                WritePriority.HIGH,
                new RemoteTransferContainer.OffsetRangeInputStreamSupplier() {
                    @Override
                    public OffsetRangeInputStream get(long size, long position) throws IOException {
                        return new OffsetRangeFileInputStream(testFile, size, position);
                    }
                },
                0,
                false
            )
        ) {
            testSupplyStreamContext(remoteTransferContainer, 10, 8, 13);
        }
    }

    private void testSupplyStreamContext(
        RemoteTransferContainer remoteTransferContainer,
        long partSize,
        long lastPartSize,
        int expectedPartCount
    ) throws InterruptedException {
        StreamContext streamContext = remoteTransferContainer.supplyStreamContext(partSize);
        int partCount = streamContext.getNumberOfParts();
        assertEquals(expectedPartCount, partCount);
        Thread[] threads = new Thread[partCount];
        long totalContentLength = remoteTransferContainer.getContentLength();
        assert partSize * (partCount - 1) + lastPartSize == totalContentLength
            : "part sizes and last part size don't add up to total content length";
        logger.info("partSize: {}, lastPartSize: {}, partCount: {}", partSize, lastPartSize, streamContext.getNumberOfParts());
        for (int partIdx = 0; partIdx < partCount; partIdx++) {
            int finalPartIdx = partIdx;
            long expectedPartSize = (partIdx == partCount - 1) ? lastPartSize : partSize;
            threads[partIdx] = new Thread(() -> {
                try {
                    InputStreamContainer inputStreamContainer = streamContext.provideStream(finalPartIdx);
                    assertEquals(expectedPartSize, inputStreamContainer.getContentLength());
                } catch (IOException e) {
                    fail("IOException during stream creation");
                }
            });
            threads[partIdx].start();
        }
        for (int i = 0; i < partCount; i++) {
            threads[i].join();
        }
    }

    public void testSupplyStreamContextCalledTwice() throws IOException {
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                TEST_FILE_SIZE_BYTES,
                true,
                WritePriority.HIGH,
                new RemoteTransferContainer.OffsetRangeInputStreamSupplier() {
                    @Override
                    public OffsetRangeInputStream get(long size, long position) throws IOException {
                        return new OffsetRangeFileInputStream(testFile, size, position);
                    }
                },
                0,
                false
            )
        ) {
            remoteTransferContainer.supplyStreamContext(16);
            assertThrows(RuntimeException.class, () -> remoteTransferContainer.supplyStreamContext(16));
        }
    }

    public void testTypeOfProvidedStreamsAllCases() throws IOException {
        testTypeOfProvidedStreams(true);
        testTypeOfProvidedStreams(false);
    }

    public void testCreateWriteContextAllCases() throws IOException {
        testCreateWriteContext(true);
        testCreateWriteContext(false);
    }

    private void testCreateWriteContext(boolean doRemoteDataIntegrityCheck) throws IOException {
        String remoteFileName = testFile.getFileName().toString() + UUID.randomUUID();
        Long expectedChecksum = randomLong();
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile.getFileName().toString(),
                remoteFileName,
                TEST_FILE_SIZE_BYTES,
                true,
                WritePriority.HIGH,
                new RemoteTransferContainer.OffsetRangeInputStreamSupplier() {
                    @Override
                    public OffsetRangeInputStream get(long size, long position) throws IOException {
                        return new OffsetRangeFileInputStream(testFile, size, position);
                    }
                },
                expectedChecksum,
                doRemoteDataIntegrityCheck
            )
        ) {
            WriteContext writeContext = remoteTransferContainer.createWriteContext();
            assertEquals(remoteFileName, writeContext.getFileName());
            assertTrue(writeContext.isFailIfAlreadyExists());
            assertEquals(TEST_FILE_SIZE_BYTES, writeContext.getFileSize());
            assertEquals(WritePriority.HIGH, writeContext.getWritePriority());
            assertEquals(doRemoteDataIntegrityCheck, writeContext.doRemoteDataIntegrityCheck());
            if (doRemoteDataIntegrityCheck) {
                assertEquals(expectedChecksum, writeContext.getExpectedChecksum());
            } else {
                assertNull(writeContext.getExpectedChecksum());
            }
        }
    }

    private void testTypeOfProvidedStreams(boolean isRemoteDataIntegritySupported) throws IOException {
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                TEST_FILE_SIZE_BYTES,
                true,
                WritePriority.HIGH,
                new RemoteTransferContainer.OffsetRangeInputStreamSupplier() {
                    @Override
                    public OffsetRangeInputStream get(long size, long position) throws IOException {
                        return new OffsetRangeFileInputStream(testFile, size, position);
                    }
                },
                0,
                isRemoteDataIntegritySupported
            )
        ) {
            StreamContext streamContext = remoteTransferContainer.supplyStreamContext(16);
            InputStreamContainer inputStreamContainer = streamContext.provideStream(0);
            if (shouldOffsetInputStreamsBeChecked(isRemoteDataIntegritySupported)) {
                assertTrue(inputStreamContainer.getInputStream() instanceof ResettableCheckedInputStream);
            } else {
                assertTrue(inputStreamContainer.getInputStream() instanceof OffsetRangeInputStream);
            }
            assertThrows(RuntimeException.class, () -> remoteTransferContainer.supplyStreamContext(16));
        }
    }

    private boolean shouldOffsetInputStreamsBeChecked(boolean isRemoteDataIntegritySupported) {
        return !isRemoteDataIntegritySupported;
    }
}
