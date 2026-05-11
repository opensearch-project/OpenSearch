/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds an acquired reader open across query and fetch phases of a QTF query.
 * The reader is acquired once during the query phase and reused during the fetch phase.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Query phase: acquires reader, stores in context, marks in-use</li>
 *   <li>Query completes: marks not-in-use, starts keepAlive countdown</li>
 *   <li>Fetch phase: marks in-use, uses the same reader</li>
 *   <li>Fetch completes: marks not-in-use, context can be freed</li>
 *   <li>Reaper: closes expired contexts (not in-use AND keepAlive elapsed)</li>
 * </ol>
 */
public class ReaderContext implements Closeable {

    private final String queryId;
    private final GatedCloseable<Reader> gatedReader;
    private final AtomicBoolean inUse = new AtomicBoolean(false);
    private final AtomicLong lastAccessTime;
    private volatile long keepAliveMillis;
    private volatile boolean closed;

    public ReaderContext(String queryId, GatedCloseable<Reader> gatedReader, long keepAliveMillis) {
        this.queryId = queryId;
        this.gatedReader = gatedReader;
        this.keepAliveMillis = keepAliveMillis;
        this.lastAccessTime = new AtomicLong(System.currentTimeMillis());
    }

    public String getQueryId() {
        return queryId;
    }

    public Reader getReader() {
        return gatedReader.get();
    }

    /**
     * Mark the context as in-use. Returns true if successfully marked.
     */
    public boolean markInUse() {
        if (closed) return false;
        lastAccessTime.set(System.currentTimeMillis());
        return inUse.compareAndSet(false, true);
    }

    /**
     * Mark the context as no longer in-use. Updates last access time.
     */
    public void markDone() {
        lastAccessTime.set(System.currentTimeMillis());
        inUse.set(false);
    }

    /**
     * Check if this context has expired (not in-use AND keepAlive elapsed).
     */
    public boolean isExpired() {
        if (inUse.get()) {
            return false;
        }
        long elapsed = System.currentTimeMillis() - lastAccessTime.get();
        return elapsed > keepAliveMillis;
    }

    public void setKeepAliveMillis(long keepAliveMillis) {
        this.keepAliveMillis = keepAliveMillis;
    }

    long getLastAccessTime() {
        return lastAccessTime.get();
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        gatedReader.close();
    }
}
