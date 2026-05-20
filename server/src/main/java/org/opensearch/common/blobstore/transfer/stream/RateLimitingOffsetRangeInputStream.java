/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer.stream;

import org.apache.lucene.store.RateLimiter;
import org.opensearch.common.StreamLimiter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Rate Limits an {@link OffsetRangeInputStream}
 *
 * @opensearch.internal
 */
public class RateLimitingOffsetRangeInputStream extends OffsetRangeInputStream {

    private final StreamLimiter streamLimiter;

    private final OffsetRangeInputStream delegate;

    /**
     * The ctor for RateLimitingOffsetRangeInputStream
     * @param delegate the underlying {@link OffsetRangeInputStream}
     * @param rateLimiterSupplier the supplier for {@link RateLimiter}
     * @param listener the listener to be invoked on rate limits
     */
    public RateLimitingOffsetRangeInputStream(
        OffsetRangeInputStream delegate,
        Supplier<RateLimiter> rateLimiterSupplier,
        StreamLimiter.Listener listener
    ) {
        this.streamLimiter = new StreamLimiter(rateLimiterSupplier, listener);
        this.delegate = delegate;
    }

    public void setReadBlock(AtomicBoolean readBlock) {
        delegate.setReadBlock(readBlock);
    }

    @Override
    public int read() throws IOException {
        int b = delegate.read();
        streamLimiter.maybePause(1);
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = delegate.read(b, off, len);
        if (n > 0) {
            streamLimiter.maybePause(n);
        }
        return n;
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    @Override
    public long getFilePointer() throws IOException {
        return delegate.getFilePointer();
    }

    @Override
    public synchronized void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
