/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import java.io.IOException;
import java.io.InputStream;

/**
 * Trims content from a given source range to a target range.
 */
public class TrimmingStream extends InputStream {

    private final long sourceStart;
    private final long sourceEnd;
    private final long targetStart;
    private final long targetEnd;
    private final InputStream in;

    private long offsetFromStart = 0;

    public TrimmingStream(long sourceStart, long sourceEnd, long targetStart, long targetEnd, InputStream in) {
        if (sourceStart < 0
            || targetStart < 0
            || targetEnd < 0
            || targetStart > targetEnd
            || sourceStart > targetStart
            || sourceEnd < targetEnd) {
            throw new IllegalArgumentException("Invalid arguments to the bounded stream");
        }

        this.sourceStart = sourceStart;
        this.sourceEnd = sourceEnd;
        this.targetStart = targetStart;
        this.targetEnd = targetEnd;
        this.in = in;
    }

    private void skipBytesOutsideBounds() throws IOException {
        long relativeOffset = offsetFromStart + sourceStart;

        if (relativeOffset < targetStart) {
            skipBytes(relativeOffset, targetStart);
        }

        if (relativeOffset > targetEnd) {
            skipBytes(relativeOffset, sourceEnd + 1);
        }
    }

    private void skipBytes(long offset, long end) throws IOException {
        long bytesToSkip = end - offset;
        while (bytesToSkip > 0) {
            long skipped = skip(bytesToSkip);
            if (skipped <= 0) {
                // End of stream or unable to skip further
                break;
            }
            bytesToSkip -= skipped;
        }
    }

    @Override
    public int read() throws IOException {
        skipBytesOutsideBounds();
        if (offsetFromStart + sourceStart > targetEnd) {
            return -1;
        }
        int b = in.read();
        if (b != -1) {
            offsetFromStart++;
        }
        // This call is made again to ensure that source stream is fully consumed when it reaches end of target range.
        skipBytesOutsideBounds();
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        skipBytesOutsideBounds();
        if (offsetFromStart + sourceStart > targetEnd) {
            return -1;
        }
        len = (int) Math.min(len, targetEnd - offsetFromStart - sourceStart + 1);
        int bytesRead = in.read(b, off, len);
        if (bytesRead != -1) {
            offsetFromStart += bytesRead;
        }
        // This call is made again to ensure that source stream is fully consumed when it reaches end of target range.
        skipBytesOutsideBounds();
        return bytesRead;
    }

    /**
     * Skips specified number of bytes of input.
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped
     * @throws    IOException if an I/O error has occurred
     */
    public long skip(long n) throws IOException {
        byte[] buf = new byte[512];
        long total = 0;
        while (total < n) {
            long len = n - total;
            len = in.read(buf, 0, len < buf.length ? (int) len : buf.length);
            if (len == -1) {
                return total;
            }
            offsetFromStart += len;
            total += len;
        }
        return total;
    }
}
