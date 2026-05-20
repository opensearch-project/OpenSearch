/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * The no-op implementation of {@link TranslogManager} that doesn't perform any operation
 *
 * @opensearch.internal
 */
public class NoOpTranslogManager implements TranslogManager {

    private final Translog.Snapshot emptyTranslogSnapshot;
    private final ReleasableLock readLock;
    private final Runnable ensureOpen;
    private final ShardId shardId;
    private final TranslogStats translogStats;
    private final TranslogDeletionPolicy translogDeletionPolicy;
    private final String translogUUID;
    private final boolean skipRecoveryStep;

    public NoOpTranslogManager(
        ShardId shardId,
        ReleasableLock readLock,
        Runnable ensureOpen,
        TranslogStats translogStats,
        Translog.Snapshot emptyTranslogSnapshot
    ) throws IOException {
        this(shardId, readLock, ensureOpen, translogStats, emptyTranslogSnapshot, "", false);
    }

    public NoOpTranslogManager(
        ShardId shardId,
        ReleasableLock readLock,
        Runnable ensureOpen,
        TranslogStats translogStats,
        Translog.Snapshot emptyTranslogSnapshot,
        String translogUUID,
        boolean skipRecoveryStep
    ) throws IOException {
        this.emptyTranslogSnapshot = emptyTranslogSnapshot;
        this.readLock = readLock;
        this.shardId = shardId;
        this.ensureOpen = ensureOpen;
        this.translogStats = translogStats;
        this.translogDeletionPolicy = new DefaultTranslogDeletionPolicy(0, 0, 0);
        this.translogUUID = translogUUID;
        this.skipRecoveryStep = skipRecoveryStep;
    }

    @Override
    public void rollTranslogGeneration() throws TranslogException {}

    @Override
    public int recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long localCheckpoint, long recoverUpToSeqNo)
        throws IOException {
        // skip translog recovery attempt when skipRecoveryStep is true
        if (skipRecoveryStep) {
            return 0;
        }

        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen.run();
            try (Translog.Snapshot snapshot = emptyTranslogSnapshot) {
                translogRecoveryRunner.run(snapshot);
            } catch (final Exception e) {
                throw new TranslogException(shardId, "failed to recover from empty translog snapshot", e);
            }
        }
        return emptyTranslogSnapshot.totalOperations();
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) {
        return false;
    }

    @Override
    public void syncTranslog() throws IOException {}

    @Override
    public TranslogStats getTranslogStats() {
        return translogStats;
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return new Translog.Location(0, 0, 0);
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws TranslogException {}

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws TranslogException {}

    @Override
    public void ensureCanFlush() {}

    @Override
    public void setMinSeqNoToKeep(long seqNo) {}

    @Override
    public int restoreLocalHistoryFromTranslog(long processedCheckpoint, TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return 0;
    }

    @Override
    public void skipTranslogRecovery() {}

    @Override
    public Translog.Operation readOperation(Translog.Location location) throws IOException {
        return null;
    }

    @Override
    public Translog.Location add(Translog.Operation operation) throws IOException {
        return new Translog.Location(0, 0, 0);
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        throw new UnsupportedOperationException("Translog snapshot unsupported with no-op translogs");
    }

    @Override
    public void onDelete() {}

    @Override
    public Releasable drainSync() {
        return () -> {};
    }

    @Override
    public Translog.TranslogGeneration getTranslogGeneration() {
        return new Translog.TranslogGeneration(translogUUID, 0);
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return 0;
    }

    @Override
    public long getMaxSeqNo() {
        return SequenceNumbers.NO_OPS_PERFORMED;
    }

    @Override
    public void trimUnreferencedReaders() throws IOException {}

    @Override
    public boolean shouldPeriodicallyFlush(long localCheckpointOfLastCommit, long flushThreshold) {
        return false;
    }

    @Override
    public Exception getTragicExceptionIfClosed() {
        return null;
    }

    @Override
    public TranslogDeletionPolicy getDeletionPolicy() {
        return translogDeletionPolicy;
    }

    @Override
    public String getTranslogUUID() {
        return translogUUID;
    }

    @Override
    public void close() throws IOException {}
}
