/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link TranslogManager} interface for interacting with the {@link org.opensearch.index.engine.Engine}
 *
 * @opensearch.internal
 */
public interface EngineTranslogManager extends TranslogManager, Closeable {

    long getLastSyncedGlobalCheckpoint();

    long getMaxSeqNo();

    void trimUnreferencedReaders() throws IOException;

    boolean shouldPeriodicallyFlush(long localCheckpointOfLastCommit, long flushThreshold);

    Exception getTragicExceptionIfClosed();

    TranslogDeletionPolicy getDeletionPolicy();

    String getTranslogUUID();

}
