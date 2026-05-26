/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

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
     * Blocks until both sides' senders have all reported {@code isLast}, or {@code timeoutMillis}
     * elapses. Returns {@code true} on success, {@code false} on timeout. Throws
     * {@link InterruptedException} if the calling thread is interrupted (e.g. task cancellation).
     */
    boolean awaitReady(long timeoutMillis) throws InterruptedException;

    /** Returns the accumulated Arrow IPC chunks for the {@code "left"} side. Caller must not mutate. */
    List<byte[]> getLeftData();

    /** Returns the accumulated Arrow IPC chunks for the {@code "right"} side. Caller must not mutate. */
    List<byte[]> getRightData();
}
