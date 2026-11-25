/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Iterator;
import java.util.List;

/**
 * Read-only handle to a node-local shuffle buffer slice — what a hash-shuffle worker handler
 * uses to drain accumulated bytes for one (queryId, stageId, partitionIndex) on one side.
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
     * Sets the number of senders this buffer will receive on each side. The consumer-side
     * handler calls this on the worker node before {@link #awaitReady} so the buffer's
     * completion latches know when to fire. {@code expectedLeft} / {@code expectedRight} should
     * each equal the number of producer tasks (one per source shard) that will ship into this
     * partition. Calling this multiple times with the same values is idempotent (CountDownLatch
     * only fires once); calling with different values from concurrent threads is unsupported.
     */
    void setExpectedSenders(int expectedLeftSenders, int expectedRightSenders);

    /**
     * Blocks until both sides' senders have all reported {@code isLast}, or {@code timeoutMillis}
     * elapses. Returns {@code true} on success, {@code false} on timeout. Throws
     * {@link InterruptedException} if the calling thread is interrupted (e.g. task cancellation).
     */
    boolean awaitReady(long timeoutMillis) throws InterruptedException;

    /** Returns the accumulated Arrow IPC chunks for the {@code "left"} side. Caller must not mutate.
     *  <p>EAGER: with spill enabled this reads the whole partition (spilled file + in-memory tail)
     *  back into heap at once. Prefer {@link #drainLeft()} on the consumer hot path so a spilled
     *  partition never fully materializes. Retained for tests / small non-spill callers. */
    List<byte[]> getLeftData();

    /** Returns the accumulated Arrow IPC chunks for the {@code "right"} side. Caller must not mutate.
     *  <p>EAGER — see {@link #getLeftData()}. Prefer {@link #drainRight()} on the hot path. */
    List<byte[]> getRightData();

    /**
     * LAZILY drains the {@code "left"} side's chunks in arrival order: spilled chunks are streamed
     * one-at-a-time from disk, then the in-memory tail. The consumer feeds each chunk to the native
     * sender and discards it before pulling the next, so a spilled partition is never fully resident
     * in heap (this is what lets an over-budget shuffle RUN rather than OOM during drain).
     *
     * <p>Call once per side after {@link #awaitReady}. MUST be closed (try-with-resources) so the
     * spill-file handle is released even on partial iteration. The default wraps {@link #getLeftData()}
     * for implementations that hold everything in memory anyway.
     */
    default CloseableIterator<byte[]> drainLeft() {
        return wrap(getLeftData().iterator());
    }

    /** LAZILY drains the {@code "right"} side — see {@link #drainLeft()}. */
    default CloseableIterator<byte[]> drainRight() {
        return wrap(getRightData().iterator());
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
