/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.util.concurrent.ReleasableLock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * TranslogCheckedContainer is used to store, update and retrieve checksums for translog files.
 *
 * @opensearch.internal
 */
public class TranslogCheckedContainer {

    private final Checksum checksum;
    private final AtomicLong contentLength;
    private final ReleasableLock updateLock = new ReleasableLock(new ReentrantLock());

    /**
     * Create TranslogCheckedContainer from provided bytes
     *
     * @param bytes The byte array to read from
     */
    public TranslogCheckedContainer(byte[] bytes) {
        this.checksum = new CRC32();
        this.contentLength = new AtomicLong();
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
