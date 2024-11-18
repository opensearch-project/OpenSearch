/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.opensearch.index.IngestionShardPointer;

public interface StreamPoller {

    void start();;

    void pause();

    void resume();

    void close();

    IngestionShardPointer getCurrentPointer();

    void resetPointer();

    boolean isPaused();

    boolean isClosed();

    enum State {
        NONE,
        CLOSED,
        PAUSED,
        POLLING,
        PROCESSING,
    }
}
