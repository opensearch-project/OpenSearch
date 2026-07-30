/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds a reader open across the query and fetch phases of a QTF query, reused by both.
 *
 * <p>Use-tracking is a reference count (not a single-permit lock), mirroring core
 * {@link org.opensearch.search.internal.ReaderContext}. The {@link ReaderContextStore} holds one
 * reference; each active phase adds one via {@link #markInUse()} and removes it via
 * {@link #markDone()}. The reader is closed only when no references remain — every phase is done
 * and {@link #close()} has run. Because the count is additive, the query and fetch phases can run
 * at the same time without one blocking the other or closing the reader out from under it.
 */
public class ReaderContext implements Closeable {

    private static final Logger logger = LogManager.getLogger(ReaderContext.class);

    private final String queryId;
    private final ShardId shardId;
    private final GatedCloseable<Reader> gatedReader;
    private final AtomicLong lastAccessTime;
    private volatile long keepAliveMillis;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    // Use-count: starts at 1 (base ref), reader closes when it hits 0.
    private final AbstractRefCounted refCounted;

    public ReaderContext(String queryId, ShardId shardId, GatedCloseable<Reader> gatedReader, long keepAliveMillis) {
        this.queryId = queryId;
        this.shardId = shardId;
        this.gatedReader = gatedReader;
        this.keepAliveMillis = keepAliveMillis;
        this.lastAccessTime = new AtomicLong(System.currentTimeMillis());
        this.refCounted = new AbstractRefCounted("qtf_reader_context") {
            @Override
            protected void closeInternal() {
                doClose();
            }
        };
    }

    public String getQueryId() {
        return queryId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public Reader getReader() {
        return gatedReader.get();
    }

    /**
     * Marks the context as in use by a phase so it won't be reaped while that phase runs.
     * Returns {@code false} if the context is already closed (the caller must then treat it as gone).
     */
    public boolean markInUse() {
        if (closed.get()) {
            return false;
        }
        lastAccessTime.set(System.currentTimeMillis());
        return refCounted.tryIncRef();
    }

    /** Marks a phase done with the context. Call once per {@link #markInUse()}. */
    public void markDone() {
        lastAccessTime.set(System.currentTimeMillis());
        refCounted.decRef();
    }

    /** Expired = no phase in use (refCount back to the base ref) AND keepAlive elapsed. */
    public boolean isExpired() {
        if (refCounted.refCount() > 1) {
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

    /** Closes the reader once no phase is still using it. */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            refCounted.decRef();
        }
    }

    private void doClose() {
        try {
            gatedReader.close();
        } catch (IOException e) {
            logger.warn("[ReaderContext] Failed to close reader for query={} shard={}: {}", queryId, shardId, e);
        }
    }
}
