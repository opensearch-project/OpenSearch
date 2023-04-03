/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer.stream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * ResettableCheckedInputStream is a modified implementation of {@link java.util.zip.CheckedInputStream} that supports
 * mark and reset and modifies the file checksum during mark and reset calls.
 */
public class ResettableCheckedInputStream extends FilterInputStream {
    private final long startPos;
    private final String file;
    private final int partNumber;
    private final int numberOfParts;
    private final Supplier<Long> posSupplier;

    /**
     * Creates an input stream using the specified Checksum.
     * @param in the input stream
     */
    public ResettableCheckedInputStream(InputStream in, String file, int partNumber, int numberOfParts, Supplier<Long> posSupplier) {
        super(in);
        this.startPos = posSupplier.get();
        this.file = file;
        this.partNumber = partNumber;
        this.numberOfParts = numberOfParts;
        this.posSupplier = posSupplier;
    }

    /**
     * Reads a byte. Will block if no input is available.
     * @return the byte read, or -1 if the end of the stream is reached.
     * @exception IOException if an I/O error has occurred
     */
    public int read() throws IOException {
        byte[] buffer = new byte[1];
        return read(buffer, 0, 1);
    }

    /**
     * Reads into an array of bytes. If <code>len</code> is not zero, the method
     * blocks until some input is available; otherwise, no
     * bytes are read and <code>0</code> is returned.
     * @param buf the buffer into which the data is read
     * @param off the start offset in the destination array <code>b</code>
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
        return in.read(buf, off, len);
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

    @Override
    public synchronized void mark(int readlimit) {
        super.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        if (startPos == posSupplier.get()) {
            return;
        }
        super.reset();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
