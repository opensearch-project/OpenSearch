/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeFileInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.blobstore.transfer.stream.RateLimitingOffsetRangeInputStream;
import org.opensearch.common.blobstore.transfer.stream.ResettableCheckedInputStream;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

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
                0L,
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
                0L,
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
        InputStream[] streams = new InputStream[partCount];
        long totalContentLength = remoteTransferContainer.getContentLength();
        assert partSize * (partCount - 1) + lastPartSize == totalContentLength
            : "part sizes and last part size don't add up to total content length";
        logger.info("partSize: {}, lastPartSize: {}, partCount: {}", partSize, lastPartSize, streamContext.getNumberOfParts());
        try {
            for (int partIdx = 0; partIdx < partCount; partIdx++) {
                int finalPartIdx = partIdx;
                long expectedPartSize = (partIdx == partCount - 1) ? lastPartSize : partSize;
                threads[partIdx] = new Thread(() -> {
                    try {
                        InputStreamContainer inputStreamContainer = streamContext.provideStream(finalPartIdx);
                        streams[finalPartIdx] = inputStreamContainer.getInputStream();
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
        } finally {
            Arrays.stream(streams).forEach(stream -> {
                try {
                    stream.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
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
                0L,
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
        InputStream inputStream = null;
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
                0L,
                isRemoteDataIntegritySupported
            )
        ) {
            StreamContext streamContext = remoteTransferContainer.supplyStreamContext(16);
            InputStreamContainer inputStreamContainer = streamContext.provideStream(0);
            inputStream = inputStreamContainer.getInputStream();
            if (shouldOffsetInputStreamsBeChecked(isRemoteDataIntegritySupported)) {
                assertTrue(inputStreamContainer.getInputStream() instanceof ResettableCheckedInputStream);
            } else {
                assertTrue(inputStreamContainer.getInputStream() instanceof OffsetRangeInputStream);
            }
            assertThrows(RuntimeException.class, () -> remoteTransferContainer.supplyStreamContext(16));
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    public void testCloseDuringOngoingReadOnStream() throws IOException, InterruptedException {
        Supplier<RateLimiter> rateLimiterSupplier = Mockito.mock(Supplier.class);
        Mockito.when(rateLimiterSupplier.get()).thenReturn(null);
        CountDownLatch readInvokedLatch = new CountDownLatch(1);
        AtomicBoolean readAfterClose = new AtomicBoolean();
        CountDownLatch streamClosed = new CountDownLatch(1);
        AtomicBoolean indexInputClosed = new AtomicBoolean();
        AtomicInteger closedCount = new AtomicInteger();
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                TEST_FILE_SIZE_BYTES,
                true,
                WritePriority.NORMAL,
                new RemoteTransferContainer.OffsetRangeInputStreamSupplier() {
                    @Override
                    public OffsetRangeInputStream get(long size, long position) throws IOException {
                        IndexInput indexInput = Mockito.mock(IndexInput.class);
                        Mockito.doAnswer(invocation -> {
                            indexInputClosed.set(true);
                            closedCount.incrementAndGet();
                            return null;
                        }).when(indexInput).close();
                        Mockito.when(indexInput.getFilePointer()).thenAnswer((Answer<Long>) invocation -> {
                            if (readAfterClose.get() == false) {
                                return 0L;
                            }
                            readInvokedLatch.countDown();
                            boolean closedSuccess = streamClosed.await(30, TimeUnit.SECONDS);
                            assertTrue(closedSuccess);
                            assertFalse(indexInputClosed.get());
                            return 0L;
                        });

                        OffsetRangeIndexInputStream offsetRangeIndexInputStream = new OffsetRangeIndexInputStream(
                            indexInput,
                            size,
                            position
                        );
                        return new RateLimitingOffsetRangeInputStream(offsetRangeIndexInputStream, rateLimiterSupplier, null);
                    }
                },
                0L,
                true
            )
        ) {
            StreamContext streamContext = remoteTransferContainer.supplyStreamContext(16);
            InputStreamContainer inputStreamContainer = streamContext.provideStream(0);
            assertTrue(inputStreamContainer.getInputStream() instanceof RateLimitingOffsetRangeInputStream);
            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    readAfterClose.set(true);
                    inputStreamContainer.getInputStream().readAllBytes();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            }).start();
            boolean successReadWait = readInvokedLatch.await(30, TimeUnit.SECONDS);
            assertTrue(successReadWait);
            // Closing stream here. Test Multiple invocations of close. Shouldn't throw any exception
            inputStreamContainer.getInputStream().close();
            inputStreamContainer.getInputStream().close();
            inputStreamContainer.getInputStream().close();
            streamClosed.countDown();
            boolean processed = latch.await(30, TimeUnit.SECONDS);
            assertTrue(processed);
            assertTrue(readAfterClose.get());
            assertTrue(indexInputClosed.get());

            // Test Multiple invocations of close. Close count should always be 1.
            inputStreamContainer.getInputStream().close();
            inputStreamContainer.getInputStream().close();
            inputStreamContainer.getInputStream().close();
            assertEquals(1, closedCount.get());

        }
    }

    public void testReadAccessWhenStreamClosed() throws IOException {
        Supplier<RateLimiter> rateLimiterSupplier = Mockito.mock(Supplier.class);
        Mockito.when(rateLimiterSupplier.get()).thenReturn(null);
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                testFile.getFileName().toString(),
                testFile.getFileName().toString(),
                TEST_FILE_SIZE_BYTES,
                true,
                WritePriority.NORMAL,
                new RemoteTransferContainer.OffsetRangeInputStreamSupplier() {
                    @Override
                    public OffsetRangeInputStream get(long size, long position) throws IOException {
                        IndexInput indexInput = Mockito.mock(IndexInput.class);
                        OffsetRangeIndexInputStream offsetRangeIndexInputStream = new OffsetRangeIndexInputStream(
                            indexInput,
                            size,
                            position
                        );
                        return new RateLimitingOffsetRangeInputStream(offsetRangeIndexInputStream, rateLimiterSupplier, null);
                    }
                },
                0L,
                true
            )
        ) {
            StreamContext streamContext = remoteTransferContainer.supplyStreamContext(16);
            InputStreamContainer inputStreamContainer = streamContext.provideStream(0);
            inputStreamContainer.getInputStream().close();
            assertThrows(AlreadyClosedException.class, () -> inputStreamContainer.getInputStream().readAllBytes());
        }
    }

    private boolean shouldOffsetInputStreamsBeChecked(boolean isRemoteDataIntegritySupported) {
        return !isRemoteDataIntegritySupported;
    }
}
