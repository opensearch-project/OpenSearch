/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;

/**
 * A poller for reading messages from an ingestion shard. This is used in the ingestion engine.
 */
public interface StreamPoller extends Closeable, ClusterStateListener {

    String BATCH_START = "batch_start";

    /**
     * Prefix for per-source-partition checkpoint keys in Lucene commit data, used by the
     * multi-partition checkpoint model. The full key is {@code BATCH_START_PREFIX + partitionId}
     * (e.g., {@code batch_start_p3} for source partition 3).
     */
    String BATCH_START_PREFIX = "batch_start_p";

    /**
     * Start the poller
     */
    void start();;

    /**
     * Pause the poller
     */
    void pause();

    /**
     * Resume the poller polling
     */
    void resume();

    /**
     * @return if the poller is paused
     */
    boolean isPaused();

    /**
     * Check if the poller is closed
     */
    boolean isClosed();

    /**
     * Get the pointer to the start of the current batch of messages.
     */
    IngestionShardPointer getBatchStartPointer();

    /**
     * Returns per-source-partition recovery pointers for the multi-partition checkpoint model.
     * The map key is the source partition ID and the value is the safe checkpoint for that
     * partition (the minimum pointer across all processor threads that have observed messages
     * from that partition).
     * <p>
     * The default implementation returns an empty map. Multi-partition pollers should override
     * to return per-partition checkpoints. An empty map signals to callers (e.g., the engine
     * during commit) that the legacy single-pointer checkpoint model should be used via
     * {@link #getBatchStartPointer()}.
     *
     * @return per-source-partition recovery pointers; empty in single-partition (legacy) mode
     */
    default Map<Integer, IngestionShardPointer> getBatchStartPointers() {
        return Collections.emptyMap();
    }

    PollingIngestStats getStats();

    IngestionErrorStrategy getErrorStrategy();

    State getState();

    /**
     * Update the error strategy for the poller.
     */
    void updateErrorStrategy(IngestionErrorStrategy errorStrategy);

    /**
     * Returns if write block is active for the poller.
     */
    boolean isWriteBlockEnabled();

    /**
     * Sets write block status for the poller.
     */
    void setWriteBlockEnabled(boolean isWriteBlockEnabled);

    IngestionShardConsumer getConsumer();

    /**
     * Requests the poller to reinitialize the consumer with updated ingestion source configuration.
     * This is called when ingestion source params are dynamically updated.
     * @param updatedIngestionSource the updated ingestion source with new configuration parameters
     */
    void requestConsumerReinitialization(IngestionSource updatedIngestionSource);

    /**
     * Updates the warmup configuration dynamically.
     * Called when index settings are changed at runtime.
     */
    void updateWarmupConfig(IngestionSource.WarmupConfig config);

    /**
     * @return true if the warmup phase is complete and the shard is ready to serve
     */
    boolean isWarmupComplete();

    /**
     * Block until warmup is complete or timeout occurs.
     * @param timeoutMs maximum time to wait in milliseconds
     * @return true if warmup completed, false if timeout
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    boolean awaitWarmupComplete(long timeoutMs) throws InterruptedException;

    /**
     * A state to indicate the current state of the poller
     */
    enum State {
        NONE,
        WARMING_UP,
        CLOSED,
        PAUSED,
        POLLING,
        PROCESSING,
    }

    /**
     *  A reset state to indicate how to reset the pointer
     */
    @PublicApi(since = "3.6.0")
    enum ResetState {
        EARLIEST,
        LATEST,
        RESET_BY_OFFSET,
        RESET_BY_TIMESTAMP,
        NONE,
    }
}
