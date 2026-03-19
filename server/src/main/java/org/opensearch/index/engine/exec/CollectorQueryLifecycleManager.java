/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the lifecycle of {@link SegmentCollector} instances for a single query.
 * <p>
 * Provides a JNI-friendly primitives-only API: callers receive an {@code int} key
 * from {@link #registerCollector} and use it to invoke {@link #collectDocs} and
 * {@link #releaseCollector}. Java owns the collector state; the native (Rust) side
 * only holds lightweight int keys.
 * <p>
 * One manager is created per query and closed when the query finishes.
 * {@link #close()} acts as a safety net, releasing any collectors that were not
 * explicitly released by the caller.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CollectorQueryLifecycleManager implements Closeable {

    private final AtomicInteger nextKey = new AtomicInteger(1);
    private final Map<Integer, SegmentCollector> collectors = new ConcurrentHashMap<>();

    /**
     * Registers a collector and returns its int key.
     *
     * @param collector the segment collector to manage
     * @return a unique key that identifies this collector
     */
    public int registerCollector(SegmentCollector collector) {
        int key = nextKey.getAndIncrement();
        collectors.put(key, collector);
        return key;
    }

    /**
     * Collects matching document IDs for the collector identified by {@code key}.
     *
     * @param key    the collector key returned by {@link #registerCollector}
     * @param minDoc inclusive lower bound
     * @param maxDoc exclusive upper bound
     * @return packed {@code long[]} bitset of matching doc IDs, or empty array if key is invalid
     */
    public long[] collectDocs(int key, int minDoc, int maxDoc) {
        SegmentCollector collector = collectors.get(key);
        if (collector == null) {
            return new long[0];
        }
        return collector.collectDocs(minDoc, maxDoc);
    }

    /**
     * Releases the collector identified by {@code key}, closing it and
     * removing it from the registry.
     *
     * @param key the collector key returned by {@link #registerCollector}
     */
    public void releaseCollector(int key) {
        SegmentCollector collector = collectors.remove(key);
        if (collector != null) {
            collector.close();
        }
    }

    /**
     * Closes all remaining collectors. Acts as a safety net for any
     * collectors that were not explicitly released.
     */
    @Override
    public void close() {
        for (SegmentCollector collector : collectors.values()) {
            collector.close();
        }
        collectors.clear();
    }
}
