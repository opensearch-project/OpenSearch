/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.opensearch.index.IngestionShardPointer;

/**
 * A poller for reading messages from an ingestion shard. This is used in the ingestion engine.
 */
public interface StreamPoller {

    String BATCH_START = "batch_start";

    void start();;

    void pause();

    void resume();

    void close();

    IngestionShardPointer getCurrentPointer();

    void resetPointer(IngestionShardPointer pointer);

    boolean isPaused();

    boolean isClosed();

    String getBatchStartPointer();

    /**
     * a state to indicate the current state of the poller
     */
    enum State {
        NONE,
        CLOSED,
        PAUSED,
        POLLING,
        PROCESSING,
    }

    /**
     *  a reset state to indicate how to reset the pointer
     */
    enum ResetState {
        EARLIEST,
        LATEST,
        NONE,
    }
}
