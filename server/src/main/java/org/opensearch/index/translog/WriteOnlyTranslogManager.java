/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/***
 *
 */
public class WriteOnlyTranslogManager extends InternalTranslogManager {

    public WriteOnlyTranslogManager(
        EngineConfig engineConfig,
        ShardId shardId,
        ReleasableLock readLock,
        LocalCheckpointTracker tracker,
        String translogUUID,
        TranslogEventListener translogEventListener,
        Runnable ensureOpen,
        BiConsumer<String, Exception> failEngine,
        Function<AlreadyClosedException, Boolean> failOnTragicEvent
    ) throws IOException {
        super(engineConfig, shardId, readLock, tracker, translogUUID, translogEventListener, ensureOpen, failEngine, failOnTragicEvent);
    }

    @Override
    public int restoreLocalHistoryFromTranslog(long processedCheckpoint, TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return 0;
    }

    @Override
    public void recoverFromTranslog(
        TranslogRecoveryRunner translogRecoveryRunner,
        long localCheckpoint,
        long recoverUpToSeqNo,
        Runnable flush
    ) throws IOException {
        throw new UnsupportedOperationException("Read only replicas do not have an IndexWriter and cannot recover from a translog.");
    }

    @Override
    public void skipTranslogRecovery() {
        // Do nothing.
    }
}
