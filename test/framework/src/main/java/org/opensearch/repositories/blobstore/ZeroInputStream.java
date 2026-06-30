/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A resettable InputStream that only serves zeros.
 **/
public class ZeroInputStream extends InputStream {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long length;
    private final AtomicLong reads;
    private volatile long mark;

    public ZeroInputStream(final long length) {
        this.length = length;
        this.reads = new AtomicLong(0);
        this.mark = -1;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        return (reads.incrementAndGet() <= length) ? 0 : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        if (len == 0) {
            return 0;
        }

        final int available = available();
        if (available == 0) {
            return -1;
        }

        final int toCopy = Math.min(len, available);
        Arrays.fill(b, off, off + toCopy, (byte) 0);
        reads.addAndGet(toCopy);
        return toCopy;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        mark = reads.get();
    }

    @Override
    public synchronized void reset() throws IOException {
        ensureOpen();
        reads.set(mark);
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        if (reads.get() >= length) {
            return 0;
        }
        try {
            return Math.toIntExact(length - reads.get());
        } catch (ArithmeticException e) {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException("Stream closed");
        }
    }
}
