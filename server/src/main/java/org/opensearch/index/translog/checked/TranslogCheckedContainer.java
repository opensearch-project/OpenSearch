/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.checked;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.Channels;
import org.opensearch.common.util.concurrent.ReleasableLock;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class TranslogCheckedContainer {

    private final Checksum checksum;
    private final AtomicLong contentLength;
    private final ReleasableLock updateLock = new ReleasableLock(new ReentrantLock());
    private final String file;

    private final Logger logger = LogManager.getLogger(TranslogCheckedContainer.class);

    /**
     * Creates an empty NRTCheckedContainer.
     *
     * @param file Name of the file
     */
    public TranslogCheckedContainer(String file) {
        this.checksum = new CRC32();
        this.contentLength = new AtomicLong();
        this.file = file;
    }

    /**
     * Creates NRTCheckedContainer from provided channel.
     *
     * @param channel {@link FileChannel} to read from
     * @param offset  offset of channel from which bytes are to be read.
     * @param len     Length of bytes to be read.
     */
    public TranslogCheckedContainer(FileChannel channel, int offset, int len, String file) throws IOException {
        this.checksum = new CRC32();
        this.contentLength = new AtomicLong();
        this.file = file;

        byte[] bytes = Channels.readFromFileChannel(channel, offset, len);
        updateFromBytes(bytes, 0, bytes.length);
    }

    /**
     * Updates checksum from bytes array
     *
     * @param bytes  Input bytes to update checksum from
     * @param offset Position in bytesReference to buffer bytes from
     * @param len    Length of bytes to be buffered
     */
    public void updateFromBytes(byte[] bytes, int offset, int len) {
        try (ReleasableLock ignored = updateLock.acquire()) {
            checksum.update(bytes, offset, len);
            updateContentLength(len);
        }
    }


    /**
     * Resets and updates checksum from provided channel.
     *
     * @param channel {@link FileChannel} to read from
     * @param offset  offset of channel from which bytes are to be read.
     * @param len     Length of bytes to be read.
     */
    public void resetAndUpdate(FileChannel channel, int offset, int len) throws IOException {
        byte[] bytes = Channels.readFromFileChannel(channel, offset, len);
        try (ReleasableLock ignored = updateLock.acquire()) {
            String stage = System.getenv("STAGE");
            if ("alpha".equals(stage) || "beta".equals(stage)) {
                logger.info("Resetting checksum for file: " + file );
                CRC32 curCRC = new CRC32();
                curCRC.update(bytes, 0, bytes.length);
                logger.info("Previous checksum stored in container for file: " + file  + " " + checksum.getValue());
                logger.info("Actual checksum of the updated file: " + file + " " + curCRC.getValue());
            }
            checksum.reset();
            contentLength.set(0L);
            checksum.update(bytes, 0, bytes.length);
            if ("alpha".equals(stage) || "beta".equals(stage)) {
                logger.info("Updated checksum of the file: " + file + " " + checksum.getValue());
                printTrace(file);
            }
            updateContentLength(len);
        }
    }

    public void printTrace(String fileName) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int maxTraceIterations = 15;
        StringBuilder sb = new StringBuilder();
        sb.append("checkpoint_debug_trace ").append(fileName).append(" =>");
        for (int traceIdx=2; traceIdx<maxTraceIterations; traceIdx++) {
            if (traceIdx < stackTrace.length) {
                sb.append(stackTrace[traceIdx].getClassName())
                    .append(".").append(stackTrace[traceIdx].getMethodName())
                    .append(":").append(stackTrace[traceIdx].getLineNumber())
                    .append(":::");
            }
        }
        logger.info(sb.toString());
    }

    private void updateContentLength(long delta) {
        assert updateLock.isHeldByCurrentThread();
        contentLength.addAndGet(delta);
    }

    /**
     * @return checksum value of bytes which have been supplied to container so far.
     */
    public long getChecksum() {
        return checksum.getValue();
    }

    /**
     * @return Content length of bytes which have been supplied to container so far.
     */
    public long getContentLength() {
        return contentLength.get();
    }
}
