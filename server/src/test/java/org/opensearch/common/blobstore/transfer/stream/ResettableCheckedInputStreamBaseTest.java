/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer.stream;

import org.junit.After;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

public abstract class ResettableCheckedInputStreamBaseTest extends OpenSearchTestCase {

    private static final int TEST_FILE_SIZE_BYTES = 10;

    private final byte[] bytesToWrite = randomByteArrayOfLength(TEST_FILE_SIZE_BYTES);
    protected Path testFile;
    private ResettableCheckedInputStream[] resettableCheckedInputStreams;

    static class InputStreamContainer {
        private final InputStream inputStream;
        private final Supplier<Long> inputStreamPosSupplier;

        public InputStreamContainer(InputStream inputStream, Supplier<Long> inputStreamPosSupplier) {
            this.inputStream = inputStream;
            this.inputStreamPosSupplier = inputStreamPosSupplier;
        }

        public InputStream getInputStream() {
            return inputStream;
        }

        public Supplier<Long> getInputStreamPosSupplier() {
            return inputStreamPosSupplier;
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testFile = createTempFile();
        Files.write(testFile, bytesToWrite, StandardOpenOption.TRUNCATE_EXISTING);
    }

    protected abstract InputStreamContainer provideInputStreamContainer(int offset, long size) throws IOException;

    public void testReadSingleByte() throws IOException, InterruptedException {
        final int nParallelReads = 10;
        Thread[] threads = new Thread[nParallelReads];
        resettableCheckedInputStreams = new ResettableCheckedInputStream[nParallelReads];
        for (int readIdx = 0; readIdx < nParallelReads; readIdx++) {
            int offset = randomInt(TEST_FILE_SIZE_BYTES - 1);
            InputStreamContainer inputStreamContainer = provideInputStreamContainer(offset, 1);
            ResettableCheckedInputStream resettableCheckedInputStream = new ResettableCheckedInputStream(
                inputStreamContainer.getInputStream(),
                testFile.getFileName().toString(),
                inputStreamContainer.getInputStreamPosSupplier()
            );
            resettableCheckedInputStreams[readIdx] = resettableCheckedInputStream;
            threads[readIdx] = new Thread(() -> {
                try {
                    assertEquals(bytesToWrite[offset], resettableCheckedInputStream.read());
                } catch (IOException e) {
                    fail("Failure while reading single byte from offset stream");
                }
            });
            threads[readIdx].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testReadMultipleBytes() throws IOException, InterruptedException {
        final int nParallelReads = 10;
        Thread[] threads = new Thread[nParallelReads];
        resettableCheckedInputStreams = new ResettableCheckedInputStream[nParallelReads];
        for (int readIdx = 0; readIdx < nParallelReads; readIdx++) {
            int readByteCount = randomInt(TEST_FILE_SIZE_BYTES - 1) + 1;
            int offset = randomInt(TEST_FILE_SIZE_BYTES - readByteCount);
            InputStreamContainer inputStreamContainer = provideInputStreamContainer(offset, readByteCount);
            ResettableCheckedInputStream resettableCheckedInputStream = new ResettableCheckedInputStream(
                inputStreamContainer.getInputStream(),
                testFile.getFileName().toString(),
                inputStreamContainer.getInputStreamPosSupplier()
            );
            resettableCheckedInputStreams[readIdx] = resettableCheckedInputStream;
            threads[readIdx] = new Thread(() -> {
                try {
                    byte[] buffer = new byte[readByteCount];
                    int bytesRead = resettableCheckedInputStream.read(buffer, 0, readByteCount);
                    assertEquals(readByteCount, bytesRead);
                    for (int bufferIdx = 0; bufferIdx < readByteCount; bufferIdx++) {
                        assertEquals(bytesToWrite[offset + bufferIdx], buffer[bufferIdx]);
                    }
                } catch (IOException e) {
                    fail("Failure while reading bytes from offset stream");
                }
            });
            threads[readIdx].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testMarkAndReset() throws IOException, InterruptedException {
        final int nParallelReads = 100;
        Thread[] threads = new Thread[nParallelReads];
        resettableCheckedInputStreams = new ResettableCheckedInputStream[nParallelReads];
        for (int readIdx = 0; readIdx < nParallelReads; readIdx++) {
            int readByteCount = randomInt(TEST_FILE_SIZE_BYTES - 1) + 1;
            int offset = randomInt(TEST_FILE_SIZE_BYTES - readByteCount);
            InputStreamContainer inputStreamContainer = provideInputStreamContainer(offset, readByteCount);
            ResettableCheckedInputStream resettableCheckedInputStream = new ResettableCheckedInputStream(
                inputStreamContainer.getInputStream(),
                testFile.getFileName().toString(),
                inputStreamContainer.getInputStreamPosSupplier()
            );
            resettableCheckedInputStreams[readIdx] = resettableCheckedInputStream;
            int finalRunIdx = readIdx;
            threads[readIdx] = new Thread(() -> {
                try {
                    boolean streamMarked = false;
                    long streamMarkPosition = -1;
                    for (int byteIdx = 0; byteIdx < readByteCount - 1; byteIdx++) {
                        resettableCheckedInputStream.read();
                        if (!streamMarked && randomBoolean()) {
                            streamMarked = true;
                            streamMarkPosition = inputStreamContainer.getInputStreamPosSupplier().get();
                            resettableCheckedInputStream.mark(readByteCount);
                        }
                    }
                    if (!streamMarked) {
                        streamMarkPosition = inputStreamContainer.getInputStreamPosSupplier().get();
                        resettableCheckedInputStream.mark(readByteCount);
                    }
                    resettableCheckedInputStream.reset();
                    assertEquals(bytesToWrite[(int) streamMarkPosition], resettableCheckedInputStream.read());
                } catch (IOException e) {
                    fail("Failure while reading bytes from offset stream");
                }
            });
            threads[readIdx].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    public void testReadAfterSkip() throws IOException {
        InputStreamContainer inputStreamContainer = provideInputStreamContainer(0, TEST_FILE_SIZE_BYTES);
        ResettableCheckedInputStream resettableCheckedInputStream = new ResettableCheckedInputStream(
            inputStreamContainer.getInputStream(),
            testFile.getFileName().toString(),
            inputStreamContainer.getInputStreamPosSupplier()
        );
        resettableCheckedInputStreams = new ResettableCheckedInputStream[] { resettableCheckedInputStream };

        long skipBytes = randomLongBetween(1, TEST_FILE_SIZE_BYTES - 1);
        long actualBytesSkipped = resettableCheckedInputStream.skip(skipBytes);
        assertEquals(skipBytes, actualBytesSkipped);
        assertEquals(bytesToWrite[(int) skipBytes], resettableCheckedInputStream.read());
    }

    public void testReadLastByte() throws IOException {
        InputStreamContainer inputStreamContainer = provideInputStreamContainer(0, TEST_FILE_SIZE_BYTES);
        ResettableCheckedInputStream resettableCheckedInputStream = new ResettableCheckedInputStream(
            inputStreamContainer.getInputStream(),
            testFile.getFileName().toString(),
            inputStreamContainer.getInputStreamPosSupplier()
        );
        resettableCheckedInputStreams = new ResettableCheckedInputStream[] { resettableCheckedInputStream };

        long skipBytes = TEST_FILE_SIZE_BYTES;
        long actualBytesSkipped = resettableCheckedInputStream.skip(skipBytes);
        assertEquals(skipBytes, actualBytesSkipped);
        assertEquals(-1, resettableCheckedInputStream.read());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        for (ResettableCheckedInputStream resettableCheckedInputStream : resettableCheckedInputStreams) {
            if (resettableCheckedInputStream != null) {
                resettableCheckedInputStream.close();
            }
        }
        super.tearDown();
    }
}
