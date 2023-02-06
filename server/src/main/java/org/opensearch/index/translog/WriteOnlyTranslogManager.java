/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.index.engine.LifecycleAware;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.translog.listener.TranslogEventListener;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/***
 * The implementation of {@link TranslogManager} that only orchestrates writes to the underlying {@link Translog}
 *
 * @opensearch.internal
 */
public class WriteOnlyTranslogManager extends InternalTranslogManager {

    public WriteOnlyTranslogManager(
        TranslogConfig translogConfig,
        LongSupplier primaryTermSupplier,
        LongSupplier globalCheckpointSupplier,
        TranslogDeletionPolicy translogDeletionPolicy,
        ShardId shardId,
        ReleasableLock readLock,
        Supplier<LocalCheckpointTracker> localCheckpointTrackerSupplier,
        String translogUUID,
        TranslogEventListener translogEventListener,
        LifecycleAware engineLifecycleAware,
        TranslogFactory translogFactory,
        BooleanSupplier primaryModeSupplier
    ) throws IOException {
        super(
            translogConfig,
            primaryTermSupplier,
            globalCheckpointSupplier,
            translogDeletionPolicy,
            shardId,
            readLock,
            localCheckpointTrackerSupplier,
            translogUUID,
            translogEventListener,
            engineLifecycleAware,
            translogFactory,
            primaryModeSupplier
        );
    }

    @Override
    public int restoreLocalHistoryFromTranslog(long processedCheckpoint, TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return 0;
    }

    @Override
    public int recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long localCheckpoint, long recoverUpToSeqNo)
        throws IOException {
        throw new UnsupportedOperationException("Read only replicas do not have an IndexWriter and cannot recover from a translog.");
    }

    @Override
    public void skipTranslogRecovery() {
        // Do nothing.
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        throw new UnsupportedOperationException("Translog snapshot unsupported with no-op translogs");
    }
}
