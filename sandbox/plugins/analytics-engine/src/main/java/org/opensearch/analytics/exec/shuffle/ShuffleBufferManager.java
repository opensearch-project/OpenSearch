/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ShuffleBufferAccess;
import org.opensearch.analytics.spi.ShuffleBufferRegistry;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-node registry of shuffle buffers for hash-shuffle joins. One {@link ShuffleBuffer} per
 * (queryId, targetStageId, partitionIndex).
 *
 * <p>Ported from OLAP's {@code ShuffleManager}
 * ({@code opensearch-olap/.../transport/ShuffleManager.java}). OLAP keys by (queryId, stageId)
 * because its dispatch assigns exactly one shuffle partition per node. analytics-engine supports
 * configurations where a single node hosts multiple partitions of the same stage (small clusters,
 * over-provisioned {@code plugins.analytics.shuffle_partitions}), so partitionIndex must be part
 * of the key — otherwise two partitions' left/right payloads and {@code isLast} markers would
 * land in the same buffer and the join would read cross-partition rows + see false completion.
 *
 * <p>Each {@link ShuffleBuffer} tracks per-side sender completion via {@code CountDownLatch}es
 * and enforces a per-partition byte cap — senders must retry with exponential backoff when
 * {@link ShuffleBuffer#tryAddData} returns {@code false}. The consumer calls
 * {@link ShuffleBuffer#awaitReady} before draining.
 *
 * @opensearch.internal
 */
public class ShuffleBufferManager implements ShuffleBufferRegistry {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleBufferManager.class);

    private final Map<String, ShuffleBuffer> buffers = new ConcurrentHashMap<>();
    private volatile long bufferMaxBytes = Long.MAX_VALUE;

    /**
     * Tombstones for queries that have been {@link #clearForQuery cleared} (terminated / aborted).
     * A producer RPC for a cancelled query can race in AFTER its buffers were cleared and would
     * otherwise re-create a buffer via {@link #getOrCreateBuffer} that no consumer will ever drain
     * or remove → a fresh leak. While a query is tombstoned, {@code getOrCreateBuffer} hands back a
     * NON-stored throwaway buffer so the late write goes to a collectible object instead of the map.
     *
     * <p>Bounded by FIFO eviction of the OLDEST tombstones (not a wholesale clear): under an abort
     * storm a wholesale clear could drop a tombstone whose post-cancel producer window is still open,
     * re-opening the recreate race for that query. Evicting oldest-first keeps the most-recent
     * {@link #ABORTED_TOMBSTONE_CAP} queries protected — recent aborts are exactly the ones whose late
     * producer RPCs may still be in flight; an evicted old tombstone's window closed long ago.
     *
     * <p>{@code aborted} (the membership set checked on the lock-free producer hot path) is a
     * concurrent set; the {@code abortedOrder} FIFO and its eviction are mutated ONLY under
     * {@link #abortedLock}. Without the lock, two concurrent {@code clearForQuery} calls could each
     * observe {@code size > cap} and both {@code poll()}, over-evicting and dropping an in-window
     * tombstone (codex review). Producers never take the lock — they only read {@code aborted}.
     */
    private final Set<String> aborted = ConcurrentHashMap.newKeySet();
    private final ArrayDeque<String> abortedOrder = new ArrayDeque<>();
    private final Object abortedLock = new Object();
    private static final int ABORTED_TOMBSTONE_CAP = 4096;

    @Override
    public ShuffleBufferAccess getOrCreate(String queryId, int targetStageId, int partitionIndex) {
        return getOrCreateBuffer(queryId, targetStageId, partitionIndex);
    }

    /** Configure the per-buffer cap for newly-created buffers. Existing buffers keep their cap. */
    public void setBufferMaxBytes(long bufferMaxBytes) {
        this.bufferMaxBytes = bufferMaxBytes;
    }

    public long getBufferMaxBytes() {
        return bufferMaxBytes;
    }

    public ShuffleBuffer getOrCreateBuffer(String queryId, int targetStageId, int partitionIndex) {
        // If the query was already cleared (cancelled/terminated), do NOT repopulate the map — a late
        // producer RPC racing after clearForQuery would otherwise leave an undrained buffer behind.
        // Hand back an unstored throwaway so the write lands on a collectible object.
        if (queryId != null && aborted.contains(queryId)) {
            return new ShuffleBuffer(bufferMaxBytes);
        }
        String k = key(queryId, targetStageId, partitionIndex);
        ShuffleBuffer buffer = buffers.computeIfAbsent(k, kk -> new ShuffleBuffer(bufferMaxBytes));
        // Double-check AFTER inserting: the tombstone-then-check above is not atomic with the
        // computeIfAbsent, so a producer that read aborted==false can still insert a key AFTER
        // clearForQuery's removal sweep already passed it. clearForQuery sets the tombstone BEFORE
        // sweeping, so any such late insert is guaranteed to observe the tombstone here — remove our
        // own entry and return a throwaway, closing the recreate race. (codex review: tombstone race.)
        if (queryId != null && aborted.contains(queryId)) {
            buffers.remove(k, buffer);
            return new ShuffleBuffer(bufferMaxBytes);
        }
        return buffer;
    }

    public ShuffleBuffer getBuffer(String queryId, int targetStageId, int partitionIndex) {
        return buffers.get(key(queryId, targetStageId, partitionIndex));
    }

    @Override
    public void removeBuffer(String queryId, int targetStageId, int partitionIndex) {
        buffers.remove(key(queryId, targetStageId, partitionIndex));
    }

    /**
     * Removes every buffer belonging to {@code queryId} (all stages, all partitions on this node).
     * Each {@link ShuffleBuffer} holds the query's shuffled payload as on-heap {@code byte[]} lists;
     * without this the entries live for the JVM's lifetime and accumulate across queries → on-heap
     * OOM. Invoked on EVERY terminal outcome of a query's data-node work — normal completion, error,
     * and task cancellation — so the on-heap payload becomes collectible as soon as the query ends.
     * Idempotent and safe to call when no buffers exist for the query (the common non-shuffle case).
     *
     * @return the number of buffers removed (for logging / test assertions)
     */
    @Override
    public int clearForQuery(String queryId) {
        if (queryId == null) {
            return 0;
        }
        // Tombstone the query FIRST so a producer RPC racing in after the clear (the abort/cancel
        // case) can't recreate a buffer via getOrCreateBuffer — it gets an unstored throwaway instead.
        // Set before the removal sweep so there's no window between sweep and tombstone where a write
        // could re-add a key (getOrCreateBuffer's post-insert recheck closes the rest of the race).
        // The add + FIFO eviction run under abortedLock: two concurrent clearForQuery calls must not
        // both observe size>cap and both poll() (over-evicting an in-window tombstone — codex review).
        // The producer hot path only reads `aborted` (lock-free); it never takes this lock.
        synchronized (abortedLock) {
            if (aborted.add(queryId)) {
                abortedOrder.addLast(queryId);
                // Evict OLDEST tombstones past the cap (FIFO), never a wholesale clear — a wholesale
                // clear could drop a still-in-window tombstone under an abort storm and re-open the race.
                while (abortedOrder.size() > ABORTED_TOMBSTONE_CAP) {
                    String oldest = abortedOrder.pollFirst();
                    if (oldest != null) {
                        aborted.remove(oldest);
                    }
                }
            }
        }
        String prefix = queryId + ":";
        int removed = 0;
        // Iterate the key set and remove matching entries. ConcurrentHashMap's iterator is weakly
        // consistent — safe to remove through it; concurrent producer/consumer activity for a still
        // -running query won't be cleared because only this query's terminal hook calls clearForQuery.
        for (var it = buffers.keySet().iterator(); it.hasNext();) {
            if (it.next().startsWith(prefix)) {
                it.remove();
                removed++;
            }
        }
        if (removed > 0) {
            LOGGER.debug("Cleared {} shuffle buffer(s) for query {}", removed, queryId);
        }
        return removed;
    }

    private static String key(String queryId, int targetStageId, int partitionIndex) {
        return queryId + ":" + targetStageId + ":" + partitionIndex;
    }

    /**
     * Per-partition shuffle buffer. Thread-safe — multiple senders may call {@link #tryAddData}
     * concurrently; one consumer thread drains via {@link #getLeftData} / {@link #getRightData}
     * after {@link #awaitReady} returns.
     */
    public static class ShuffleBuffer implements ShuffleBufferAccess {
        private final List<byte[]> leftData = Collections.synchronizedList(new ArrayList<>());
        private final List<byte[]> rightData = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger leftDoneCount = new AtomicInteger();
        private final AtomicInteger rightDoneCount = new AtomicInteger();
        private volatile int expectedLeftSenders = -1;
        private volatile int expectedRightSenders = -1;
        private final CountDownLatch leftReady = new CountDownLatch(1);
        private final CountDownLatch rightReady = new CountDownLatch(1);
        private final AtomicLong currentBytes = new AtomicLong();
        private final AtomicLong rejectedCount = new AtomicLong();
        private final long maxBytes;

        public ShuffleBuffer(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        public void setExpectedSenders(int leftSenders, int rightSenders) {
            // -1 means "leave unchanged"; allows per-side handlers to set their own count
            // without clobbering the other side's.
            if (leftSenders >= 0) {
                this.expectedLeftSenders = leftSenders;
                checkCompletion("left");
            }
            if (rightSenders >= 0) {
                this.expectedRightSenders = rightSenders;
                checkCompletion("right");
            }
        }

        /**
         * Attempts to add {@code data} to the named side. Returns {@code false} if the buffer's
         * byte cap would be exceeded — the sender must retry with exponential backoff.
         */
        public boolean tryAddData(String side, byte[] data) {
            int size = data == null ? 0 : data.length;
            long newTotal = currentBytes.addAndGet(size);
            if (newTotal > maxBytes) {
                currentBytes.addAndGet(-size);
                rejectedCount.incrementAndGet();
                return false;
            }
            if ("left".equals(side)) {
                leftData.add(data);
            } else {
                rightData.add(data);
            }
            return true;
        }

        public long getCurrentBytes() {
            return currentBytes.get();
        }

        public long getMaxBytes() {
            return maxBytes;
        }

        public long getRejectedCount() {
            return rejectedCount.get();
        }

        public void senderDone(String side) {
            if ("left".equals(side)) {
                leftDoneCount.incrementAndGet();
                checkCompletion("left");
            } else {
                rightDoneCount.incrementAndGet();
                checkCompletion("right");
            }
        }

        private void checkCompletion(String side) {
            if ("left".equals(side)) {
                if (expectedLeftSenders >= 0 && leftDoneCount.get() >= expectedLeftSenders) {
                    leftReady.countDown();
                }
            } else {
                if (expectedRightSenders >= 0 && rightDoneCount.get() >= expectedRightSenders) {
                    rightReady.countDown();
                }
            }
        }

        /** Wait for both sides' senders to complete. Returns {@code false} on timeout. */
        public boolean awaitReady(long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            long remaining = timeoutMillis;
            if (!leftReady.await(remaining, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Shuffle left side timed out: received {}/{} senders", leftDoneCount.get(), expectedLeftSenders);
                return false;
            }
            remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) return false;
            if (!rightReady.await(remaining, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Shuffle right side timed out: received {}/{} senders", rightDoneCount.get(), expectedRightSenders);
                return false;
            }
            return true;
        }

        public List<byte[]> getLeftData() {
            return leftData;
        }

        public List<byte[]> getRightData() {
            return rightData;
        }

        public int getExpectedLeftSenders() {
            return expectedLeftSenders;
        }

        public int getExpectedRightSenders() {
            return expectedRightSenders;
        }

        public int getLeftDoneCount() {
            return leftDoneCount.get();
        }

        public int getRightDoneCount() {
            return rightDoneCount.get();
        }
    }
}
