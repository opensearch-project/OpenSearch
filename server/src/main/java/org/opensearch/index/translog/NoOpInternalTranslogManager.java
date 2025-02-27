/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.lease.Releasable;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.stream.Stream;

import static org.opensearch.index.translog.Translog.EMPTY_TRANSLOG_LOCATION;

/**
 * A no-op {@link TranslogManager} implementation.
 *
 * @opensearch.internal
 */
public class NoOpInternalTranslogManager implements TranslogManager {
    private final TranslogDeletionPolicy translogDeletionPolicy;
    private final String translogUUID;

    public NoOpInternalTranslogManager(EngineConfig engineConfig, String translogUUID) {
        this.translogUUID = translogUUID;
        this.translogDeletionPolicy = new DefaultTranslogDeletionPolicy(
            engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
            engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
            engineConfig.getIndexSettings().getTranslogRetentionTotalFiles()
        );
    }

    @Override
    public void rollTranslogGeneration() throws TranslogException, IOException {}

    @Override
    public int recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long localCheckpoint, long recoverUpToSeqNo)
        throws IOException {
        return 0;
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        return null;
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        return true;
    }

    @Override
    public void syncTranslog() throws IOException {}

    @Override
    public void trimUnreferencedTranslogFiles() throws TranslogException {}

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws TranslogException {}

    @Override
    public int restoreLocalHistoryFromTranslog(long processedCheckpoint, TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return 0;
    }

    @Override
    public void onDelete() {}

    @Override
    public Releasable drainSync() {
        return () -> {};
    }

    @Override
    public long getMaxSeqNo() {
        return SequenceNumbers.NO_OPS_PERFORMED;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return new TranslogStats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return EMPTY_TRANSLOG_LOCATION;
    }

    @Override
    public void ensureCanFlush() {}

    @Override
    public void setMinSeqNoToKeep(long seqNo) {}

    @Override
    public Translog.TranslogGeneration getTranslogGeneration() {
        return new Translog.TranslogGeneration(translogUUID, 0);
    }

    @Override
    public Translog.Operation readOperation(Translog.Location location) throws IOException {
        return null;
    }

    @Override
    public Translog.Location add(Translog.Operation operation) throws IOException {
        return new Translog.Location(0, 0, 0);
    }

    @Override
    public void skipTranslogRecovery() {}

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return 0;
    }

    @Override
    public void trimUnreferencedReaders() throws IOException {}

    @Override
    public String getTranslogUUID() {
        return translogUUID;
    }

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
    public void close() throws IOException {}
}
