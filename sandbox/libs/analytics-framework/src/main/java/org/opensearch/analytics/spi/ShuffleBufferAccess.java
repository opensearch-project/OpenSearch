/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only handle to a node-local shuffle buffer slice — what a hash-shuffle worker handler
 * uses to drain accumulated bytes for one (queryId, stageId, partitionIndex) on one slot.
 *
 * <p>The implementing class lives in analytics-engine ({@code ShuffleBufferManager.ShuffleBuffer});
 * the SPI exposes only the consumer-side surface so backend handlers don't need a hard
 * dependency on the engine plugin's internals. Producers populate the buffer via the
 * {@code AnalyticsShuffleDataAction} transport path; consumers (this interface's caller)
 * await readiness, then drain.
 *
 * @opensearch.internal
 */
public interface ShuffleBufferAccess {

    /**
     * Sets the number of senders this buffer will receive on each slot, keyed by slot label (see
     * {@link ShuffleSlots}). The consumer-side handler calls this on the worker node before
     * {@link #awaitReady} so the buffer's per-slot completion latches know when to fire. Each value
     * should equal the number of producer tasks (one per source shard) that will ship into this
     * partition on that slot.
     *
     * <p>ALL of the consumer's slots must be declared in ONE call: {@link #awaitReady} waits for
     * every declared slot, and a slot first named by a later call would not be waited on by an
     * already-blocked consumer. Calling this repeatedly with the same values is idempotent (a
     * CountDownLatch only fires once); calling with different values from concurrent threads is
     * unsupported.
     */
    void setExpectedSenders(Map<String, Integer> expectedSendersBySlot);

    /**
     * Binary convenience form of {@link #setExpectedSenders(Map)} for a two-slot (hash-join)
     * consumer. A negative count means "leave this slot unchanged", which is how the single-slot
     * aggregate-shuffle path declares an unused right side.
     */
    default void setExpectedSenders(int expectedLeftSenders, int expectedRightSenders) {
        Map<String, Integer> bySlot = new LinkedHashMap<>();
        if (expectedLeftSenders >= 0) {
            bySlot.put(ShuffleSlots.LEFT, expectedLeftSenders);
        }
        if (expectedRightSenders >= 0) {
            bySlot.put(ShuffleSlots.RIGHT, expectedRightSenders);
        }
        setExpectedSenders(bySlot);
    }

    /**
     * Blocks until every declared slot's senders have all reported {@code isLast}, or
     * {@code timeoutMillis} elapses. Returns {@code true} on success, {@code false} on timeout.
     * Throws {@link InterruptedException} if the calling thread is interrupted (e.g. task
     * cancellation).
     */
    boolean awaitReady(long timeoutMillis) throws InterruptedException;

    /** Returns the accumulated Arrow IPC chunks for {@code slot}. Caller must not mutate.
     *  <p>EAGER: with spill enabled this reads the whole partition (spilled file + in-memory tail)
     *  back into heap at once. Prefer {@link #drain(String)} on the consumer hot path so a spilled
     *  partition never fully materializes. Retained for tests / small non-spill callers. */
    List<byte[]> getData(String slot);

    /**
     * LAZILY drains {@code slot}'s chunks in arrival order: spilled chunks are streamed
     * one-at-a-time from disk, then the in-memory tail. The consumer feeds each chunk to the native
     * sender and discards it before pulling the next, so a spilled partition is never fully resident
     * in heap (this is what lets an over-budget shuffle RUN rather than OOM during drain).
     *
     * <p>Call once per slot after {@link #awaitReady}. MUST be closed (try-with-resources) so the
     * spill-file handle is released even on partial iteration. The default wraps
     * {@link #getData(String)} for implementations that hold everything in memory anyway.
     */
    default CloseableIterator<byte[]> drain(String slot) {
        return wrap(getData(slot).iterator());
    }

    /**
     * {@link #drain(String)} with an explicit per-chunk liveness timeout.
     *
     * <p>Under pipelined shuffle the returned iterator is CONCURRENT with the producers: it blocks for
     * the next chunk rather than being created after the partition is complete, and terminates at the
     * stream's end-of-stream marker. {@code timeoutMillis} therefore bounds the wait for ONE chunk, not
     * for the whole partition, and a timeout means the producers have stalled — implementations must
     * fail rather than report a clean end-of-stream, or the consumer would silently under-deliver.
     */
    default CloseableIterator<byte[]> drain(String slot, long timeoutMillis) {
        return drain(slot);
    }

    /** {@link #getData(String)} for the {@link ShuffleSlots#LEFT} slot. */
    default List<byte[]> getLeftData() {
        return getData(ShuffleSlots.LEFT);
    }

    /** {@link #getData(String)} for the {@link ShuffleSlots#RIGHT} slot. */
    default List<byte[]> getRightData() {
        return getData(ShuffleSlots.RIGHT);
    }

    /** {@link #drain(String)} for the {@link ShuffleSlots#LEFT} slot. */
    default CloseableIterator<byte[]> drainLeft() {
        return drain(ShuffleSlots.LEFT);
    }

    /** {@link #drain(String)} for the {@link ShuffleSlots#RIGHT} slot. */
    default CloseableIterator<byte[]> drainRight() {
        return drain(ShuffleSlots.RIGHT);
    }

    /** Adapts a plain {@link Iterator} to a no-op-close {@link CloseableIterator} (in-memory case). */
    private static CloseableIterator<byte[]> wrap(Iterator<byte[]> it) {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public byte[] next() {
                return it.next();
            }

            @Override
            public void close() {
                // nothing to release — the backing list is heap-resident
            }
        };
    }
}
