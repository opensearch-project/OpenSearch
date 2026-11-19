/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounds how many shard streams one search keeps open, which bounds the batches it buffers on the coordinator.
 * <p>
 * The search's per-node shard request limit decides which shards are submitted. This scheduler then opens up to
 * {@code maxOpen} of them, preferring the node with the fewest open streams of the search, so that the streams spread
 * across nodes. Launches always run on the executor, never on the caller's thread.
 */
final class ShardStreamScheduler {

    /** A shard stream waiting to open. */
    interface Launch {
        /** Opens the stream. The launch must call {@link ShardStreamScheduler#release} once the stream ends. */
        void run();

        /** Ends the shard without opening its stream, because the search stopped. */
        void abandon();
    }

    private final Executor executor;
    private final ReentrantLock lock = new ReentrantLock();
    private int maxOpen;
    private final Map<String, ArrayDeque<Launch>> queued = new LinkedHashMap<>();
    private final Map<String, Integer> openPerNode = new LinkedHashMap<>();
    private int open;
    private int waiting;
    private boolean closed;

    /**
     * Creates a scheduler for one search.
     *
     * @param maxOpen  how many streams may be open at once; values below one count as one
     * @param executor runs the launches
     */
    ShardStreamScheduler(int maxOpen, Executor executor) {
        this.maxOpen = Math.max(1, maxOpen);
        this.executor = executor;
    }

    /**
     * Queues a shard stream to open on {@code nodeId}, and opens queued streams while slots are free. Once the scheduler is
     * closed, abandons the stream instead.
     */
    void submit(String nodeId, Launch launch) {
        boolean abandon = false;
        lock.lock();
        try {
            if (closed) {
                abandon = true;
            } else {
                queued.computeIfAbsent(nodeId, k -> new ArrayDeque<>()).add(launch);
                waiting++;
            }
        } finally {
            lock.unlock();
        }
        if (abandon) {
            executor.execute(launch::abandon);
        } else {
            drain();
        }
    }

    /**
     * Lowers how many streams may be open at once to {@code maxOpen}, but not below one. A higher limit has no effect.
     */
    void limit(int maxOpen) {
        lock.lock();
        try {
            this.maxOpen = Math.max(1, Math.min(this.maxOpen, maxOpen));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Frees the slot of a stream that ended on {@code nodeId}, and opens queued streams while slots are free.
     */
    void release(String nodeId) {
        lock.lock();
        try {
            open--;
            openPerNode.merge(nodeId, -1, Integer::sum);
        } finally {
            lock.unlock();
        }
        drain();
    }

    /**
     * Abandons every queued stream, and every stream submitted later.
     */
    void close() {
        final List<Launch> abandoned = new ArrayList<>();
        lock.lock();
        try {
            closed = true;
            for (ArrayDeque<Launch> launches : queued.values()) {
                abandoned.addAll(launches);
                launches.clear();
            }
            waiting = 0;
        } finally {
            lock.unlock();
        }
        for (Launch launch : abandoned) {
            executor.execute(launch::abandon);
        }
    }

    int openStreams() {
        lock.lock();
        try {
            return open;
        } finally {
            lock.unlock();
        }
    }

    private void drain() {
        final List<Launch> launches = new ArrayList<>();
        lock.lock();
        try {
            while (closed == false && open < maxOpen && waiting > 0) {
                String target = null;
                int fewest = Integer.MAX_VALUE;
                for (Map.Entry<String, ArrayDeque<Launch>> entry : queued.entrySet()) {
                    if (entry.getValue().isEmpty()) {
                        continue;
                    }
                    final int nodeOpen = openPerNode.getOrDefault(entry.getKey(), 0);
                    if (nodeOpen < fewest) {
                        fewest = nodeOpen;
                        target = entry.getKey();
                    }
                }
                launches.add(queued.get(target).poll());
                waiting--;
                open++;
                openPerNode.merge(target, 1, Integer::sum);
            }
        } finally {
            lock.unlock();
        }
        for (Launch launch : launches) {
            executor.execute(launch::run);
        }
    }
}
