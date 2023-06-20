/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer.stream;

import com.jcraft.jzlib.CRC32;

import java.io.FilterInputStream;
import java.io.IOException;

/**
 * ResettableCheckedInputStream is a modified implementation of {@link java.util.zip.CheckedInputStream} that supports
 * mark and reset and modifies the file checksum during mark and reset calls.
 *
 * @opensearch.internal
 */
public class ResettableCheckedInputStream extends FilterInputStream {
    private final CRC32 checksum;
    private final CRC32 markedChecksum;
    private final long startPos;
    private final String file;
    private final OffsetRangeInputStream offsetRangeInputStream;

    /**
     * Creates a new ResettableCheckedInputStream object
     *
     * @param offsetRangeInputStream   The underlying input stream
     * @param file Name of the file
     */
    public ResettableCheckedInputStream(OffsetRangeInputStream offsetRangeInputStream, String file) throws IOException {
        super(offsetRangeInputStream);
        this.checksum = new CRC32();
        this.markedChecksum = new CRC32();
        this.offsetRangeInputStream = offsetRangeInputStream;
        this.startPos = offsetRangeInputStream.getFilePointer();
        this.file = file;
    }

    /**
     * Reads a byte. Will block if no input is available.
     * @return the byte read, or -1 if the end of the stream is reached.
     * @exception IOException if an I/O error has occurred
     */
    public int read() throws IOException {
        byte[] buffer = new byte[1];
        int len = read(buffer, 0, 1);
        if (len == -1) {
            return -1;
        }
        return buffer[0];
    }

    /**
     * Reads into an array of bytes. If <code>len</code> is not zero, the method
     * blocks until some input is available; otherwise, no
     * bytes are read and <code>0</code> is returned.
     * @param buf the buffer into which the data is read
     * @param off the start offset in the destination array <code>buf</code>
     * @param len the maximum number of bytes read
     * @return    the actual number of bytes read, or -1 if the end
     *            of the stream is reached.
     * @exception  NullPointerException If <code>buf</code> is <code>null</code>.
     * @exception  IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>buf.length - off</code>
     * @exception IOException if an I/O error has occurred
     */
    public int read(byte[] buf, int off, int len) throws IOException {
        len = in.read(buf, off, len);
        if (len != -1) {
            checksum.update(buf, off, len);
        }
        return len;
    }

    /**
     * Skips specified number of bytes of input.
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped
     * @exception IOException if an I/O error has occurred
     */
    public long skip(long n) throws IOException {
        byte[] buf = new byte[512];
        long total = 0;
        while (total < n) {
            long len = n - total;
            len = read(buf, 0, len < buf.length ? (int) len : buf.length);
            if (len == -1) {
                return total;
            }
            total += len;
        }
        return total;
    }

    /**
     * Returns the Checksum for this input stream.
     * @return the Checksum value
     */
    public long getChecksum() {
        return checksum.getValue();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        markedChecksum.reset(checksum.getValue());
        super.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        if (startPos == offsetRangeInputStream.getFilePointer()) {
            return;
        }
        checksum.reset(markedChecksum.getValue());
        super.reset();
    }
}
