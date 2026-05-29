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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores reader contexts across query and fetch phases for QTF execution.
 * Each context is keyed by queryId and holds the acquired reader open
 * until the fetch phase completes or the keepAlive expires.
 *
 * <p>A reaper thread periodically scans for expired contexts and closes them.
 */
public class ReaderContextStore {

    private static final Logger logger = LogManager.getLogger(ReaderContextStore.class);
    private static final TimeValue REAPER_INTERVAL = TimeValue.timeValueSeconds(10);

    public static final Setting<TimeValue> READER_CONTEXT_KEEP_ALIVE = Setting.positiveTimeSetting(
        "analytics.qtf.reader_context.keep_alive",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Multi-shard QTF queries call {@link #createContext} once per shard with the same
     * {@code queryId}. Keying by {@code queryId} alone would collide and overwrite.
     */
    public record Key(String queryId, ShardId shardId) {
    }

    private final Map<Key, ReaderContext> activeContexts = new ConcurrentHashMap<>();
    private volatile long defaultKeepAliveMillis;

    public ReaderContextStore(ThreadPool threadPool) {
        this(threadPool, READER_CONTEXT_KEEP_ALIVE.getDefault(null).millis());
    }

    public ReaderContextStore(ThreadPool threadPool, long defaultKeepAliveMillis) {
        this.defaultKeepAliveMillis = defaultKeepAliveMillis;
        threadPool.scheduleWithFixedDelay(new Reaper(), REAPER_INTERVAL, ThreadPool.Names.SAME);
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.defaultKeepAliveMillis = keepAlive.millis();
    }

    /**
     * Create and store a new reader context for the given query/shard.
     * Acquires the reader and marks it in-use.
     */
    public ReaderContext createContext(String queryId, ShardId shardId, GatedCloseable<Reader> gatedReader) {
        ReaderContext ctx = new ReaderContext(queryId, shardId, gatedReader, defaultKeepAliveMillis);
        ctx.markInUse();
        activeContexts.put(new Key(queryId, shardId), ctx);
        return ctx;
    }

    /**
     * Get an existing context by queryId/shardId. Returns null if not found or expired.
     */
    public ReaderContext getContext(String queryId, ShardId shardId) {
        return activeContexts.get(new Key(queryId, shardId));
    }

    /**
     * Acquire a context for use (fetch phase). Marks it in-use.
     * Returns null if not found or already closed.
     */
    public ReaderContext acquireContext(String queryId, ShardId shardId) {
        ReaderContext ctx = activeContexts.get(new Key(queryId, shardId));
        if (ctx == null) return null;
        if (ctx.markInUse()) {
            return ctx;
        }
        return null;
    }

    /**
     * Release a context after use (query or fetch done). Marks it not-in-use.
     */
    public void releaseContext(String queryId, ShardId shardId) {
        ReaderContext ctx = activeContexts.get(new Key(queryId, shardId));
        if (ctx != null) {
            ctx.markDone();
        }
    }

    /**
     * Remove and close a context (fetch complete, no longer needed).
     */
    public void freeContext(String queryId, ShardId shardId) {
        ReaderContext ctx = activeContexts.remove(new Key(queryId, shardId));
        if (ctx != null) {
            try {
                ctx.close();
            } catch (Exception e) {
                logger.warn("[ReaderContextStore] Failed to close context for query={} shard={}: {}", queryId, shardId, e);
            }
        }
    }

    public int activeCount() {
        return activeContexts.size();
    }

    private class Reaper implements Runnable {
        @Override
        public void run() {
            for (ReaderContext ctx : activeContexts.values()) {
                if (ctx.isExpired()) {
                    logger.debug("[ReaderContextStore] Freeing expired context for query={} shard={}", ctx.getQueryId(), ctx.getShardId());
                    freeContext(ctx.getQueryId(), ctx.getShardId());
                }
            }
        }
    }
}
