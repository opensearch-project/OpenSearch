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

    private final Map<String, ReaderContext> activeContexts = new ConcurrentHashMap<>();
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
     * Create and store a new reader context for the given query.
     * Acquires the reader and marks it in-use.
     */
    public ReaderContext createContext(String queryId, GatedCloseable<Reader> gatedReader) {
        ReaderContext ctx = new ReaderContext(queryId, gatedReader, defaultKeepAliveMillis);
        ctx.markInUse();
        activeContexts.put(queryId, ctx);
        return ctx;
    }

    /**
     * Get an existing context by queryId. Returns null if not found or expired.
     */
    public ReaderContext getContext(String queryId) {
        return activeContexts.get(queryId);
    }

    /**
     * Acquire a context for use (fetch phase). Marks it in-use.
     * Returns null if not found or already closed.
     */
    public ReaderContext acquireContext(String queryId) {
        ReaderContext ctx = activeContexts.get(queryId);
        if (ctx == null) return null;
        if (ctx.markInUse()) {
            return ctx;
        }
        return null;
    }

    /**
     * Release a context after use (query or fetch done). Marks it not-in-use.
     */
    public void releaseContext(String queryId) {
        ReaderContext ctx = activeContexts.get(queryId);
        if (ctx != null) {
            ctx.markDone();
        }
    }

    /**
     * Remove and close a context (fetch complete, no longer needed).
     */
    public void freeContext(String queryId) {
        ReaderContext ctx = activeContexts.remove(queryId);
        if (ctx != null) {
            try {
                ctx.close();
            } catch (Exception e) {
                logger.warn("[ReaderContextStore] Failed to close context for query={}", queryId, e);
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
                    logger.debug("[ReaderContextStore] Freeing expired context for query={}", ctx.getQueryId());
                    freeContext(ctx.getQueryId());
                }
            }
        }
    }
}
