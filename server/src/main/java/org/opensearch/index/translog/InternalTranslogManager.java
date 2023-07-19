/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.LifecycleAware;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.listener.TranslogEventListener;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The {@link TranslogManager} implementation capable of orchestrating all read/write {@link Translog} operations while
 * interfacing with the {@link org.opensearch.index.engine.InternalEngine}
 *
 * @opensearch.internal
 */
public class InternalTranslogManager implements TranslogManager, Closeable {

    private final ReleasableLock readLock;
    private final LifecycleAware engineLifeCycleAware;
    private final ShardId shardId;
    private final Translog translog;
    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);
    private final TranslogEventListener translogEventListener;
    private final Supplier<LocalCheckpointTracker> localCheckpointTrackerSupplier;
    private static final Logger logger = LogManager.getLogger(InternalTranslogManager.class);

    public InternalTranslogManager(
        TranslogConfig translogConfig,
        LongSupplier primaryTermSupplier,
        LongSupplier globalCheckpointSupplier,
        TranslogDeletionPolicy translogDeletionPolicy,
        ShardId shardId,
        ReleasableLock readLock,
        Supplier<LocalCheckpointTracker> localCheckpointTrackerSupplier,
        String translogUUID,
        TranslogEventListener translogEventListener,
        LifecycleAware engineLifeCycleAware,
        TranslogFactory translogFactory,
        BooleanSupplier primaryModeSupplier
    ) throws IOException {
        this.shardId = shardId;
        this.readLock = readLock;
        this.engineLifeCycleAware = engineLifeCycleAware;
        this.translogEventListener = translogEventListener;
        this.localCheckpointTrackerSupplier = localCheckpointTrackerSupplier;
        Translog translog = openTranslog(translogConfig, primaryTermSupplier, translogDeletionPolicy, globalCheckpointSupplier, seqNo -> {
            final LocalCheckpointTracker tracker = localCheckpointTrackerSupplier.get();
            assert tracker != null || getTranslog(true).isOpen() == false;
            if (tracker != null) {
                tracker.markSeqNoAsPersisted(seqNo);
            }
        }, translogUUID, translogFactory, primaryModeSupplier);
        assert translog.getGeneration() != null;
        this.translog = translog;
        assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
        // don't allow commits until we are done with recovering
        pendingTranslogRecovery.set(true);
    }

    /**
     * Rolls the translog generation and cleans unneeded.
     */
    @Override
    public void rollTranslogGeneration() throws TranslogException {
        try (ReleasableLock ignored = readLock.acquire()) {
            engineLifeCycleAware.ensureOpen();
            translog.rollGeneration();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            translogEventListener.onFailure("translog roll generation failed", e);
            throw e;
        } catch (Exception e) {
            try {
                translogEventListener.onFailure("translog roll generation failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new TranslogException(shardId, "failed to roll translog", e);
        }
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(long fromSeqNo, long toSeqNo, boolean requiredFullRange) throws IOException {
        return translog.newSnapshot(fromSeqNo, toSeqNo, requiredFullRange);
    }

    /**
     * Performs recovery from the transaction log up to {@code recoverUpToSeqNo} (inclusive).
     * This operation will close the engine if the recovery fails.
     *  @param translogRecoveryRunner the translog recovery runner
     * @param recoverUpToSeqNo       the upper bound, inclusive, of sequence number to be recovered
     * @return the total number of operations recovered
     */
    @Override
    public int recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long localCheckpoint, long recoverUpToSeqNo)
        throws IOException {
        int opsRecovered = 0;
        translogEventListener.onBeginTranslogRecovery();
        try (ReleasableLock ignored = readLock.acquire()) {
            engineLifeCycleAware.ensureOpen();
            if (pendingTranslogRecovery.get() == false) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                opsRecovered = recoverFromTranslogInternal(translogRecoveryRunner, localCheckpoint, recoverUpToSeqNo);
            } catch (Exception e) {
                try {
                    pendingTranslogRecovery.set(true); // just play safe and never allow commits on this see #ensureCanFlush
                    translogEventListener.onFailure("failed to recover from translog", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                throw e;
            }
        }
        return opsRecovered;
    }

    private int recoverFromTranslogInternal(TranslogRecoveryRunner translogRecoveryRunner, long localCheckpoint, long recoverUpToSeqNo) {
        final int opsRecovered;
        if (localCheckpoint < recoverUpToSeqNo) {
            try (Translog.Snapshot snapshot = translog.newSnapshot(localCheckpoint + 1, recoverUpToSeqNo)) {
                opsRecovered = translogRecoveryRunner.run(snapshot);
            } catch (Exception e) {
                throw new TranslogException(shardId, "failed to recover from translog", e);
            }
        } else {
            opsRecovered = 0;
        }
        // flush if we recovered something or if we have references to older translogs
        // note: if opsRecovered == 0 and we have older translogs it means they are corrupted or 0 length.
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false); // we are good - now we can commit
        logger.trace(
            () -> new ParameterizedMessage(
                "flushing post recovery from translog: ops recovered [{}], current translog generation [{}]",
                opsRecovered,
                translog.currentFileGeneration()
            )
        );
        translogEventListener.onAfterTranslogRecovery();
        return opsRecovered;
    }

    /**
     * Checks if the underlying storage sync is required.
     */
    @Override
    public boolean isTranslogSyncNeeded() {
        return getTranslog(true).syncNeeded();
    }

    /**
     * Ensures that all locations in the given stream have been written to the underlying storage.
     */
    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        final boolean synced = translog.ensureSynced(locations);
        if (synced) {
            translogEventListener.onAfterTranslogSync();
        }
        return synced;
    }

    /**
     * Syncs the translog and invokes the listener
     * @throws IOException the exception on sync failure
     */
    @Override
    public void syncTranslog() throws IOException {
        translog.sync();
        translogEventListener.onAfterTranslogSync();
    }

    @Override
    public TranslogStats getTranslogStats() {
        return getTranslog(true).stats();
    }

    /**
     * Returns the last location that the translog of this engine has written into.
     */
    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return getTranslog(true).getLastWriteLocation();
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.opensearch.index.translog.TranslogDeletionPolicy} for details
     */
    @Override
    public void trimUnreferencedTranslogFiles() throws TranslogException {
        try (ReleasableLock ignored = readLock.acquire()) {
            engineLifeCycleAware.ensureOpen();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            translogEventListener.onFailure("translog trimming unreferenced translog failed", e);
            throw e;
        } catch (Exception e) {
            try {
                translogEventListener.onFailure("translog trimming unreferenced translog failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new TranslogException(shardId, "failed to trim unreferenced translog translog", e);
        }
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation.
     * This test is based on the size of the current generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    @Override
    public boolean shouldRollTranslogGeneration() {
        return getTranslog(true).shouldRollGeneration();
    }

    /**
     * Trims translog for terms below <code>belowTerm</code> and seq# above <code>aboveSeqNo</code>
     * @see Translog#trimOperations(long, long)
     */
    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws TranslogException {
        try (ReleasableLock ignored = readLock.acquire()) {
            engineLifeCycleAware.ensureOpen();
            translog.trimOperations(belowTerm, aboveSeqNo);
        } catch (AlreadyClosedException e) {
            translogEventListener.onFailure("translog operations trimming failed", e);
            throw e;
        } catch (Exception e) {
            try {
                translogEventListener.onFailure("translog operations trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new TranslogException(shardId, "failed to trim translog operations", e);
        }
    }

    /**
     * This method replays translog to restore the Lucene index which might be reverted previously.
     * This ensures that all acknowledged writes are restored correctly when this engine is promoted.
     *
     * @return the number of translog operations have been recovered
     */
    @Override
    public int restoreLocalHistoryFromTranslog(long processedCheckpoint, TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            engineLifeCycleAware.ensureOpen();
            try (Translog.Snapshot snapshot = getTranslog(true).newSnapshot(processedCheckpoint + 1, Long.MAX_VALUE)) {
                return translogRecoveryRunner.run(snapshot);
            }
        }
    }

    /**
     * Ensures that the flushes can succeed if there are no pending translog recovery
     */
    @Override
    public void ensureCanFlush() {
        // translog recovery happens after the engine is fully constructed.
        // If we are in this stage we have to prevent flushes from this
        // engine otherwise we might loose documents if the flush succeeds
        // and the translog recovery fails when we "commit" the translog on flush.
        if (pendingTranslogRecovery.get()) {
            throw new IllegalStateException(shardId.toString() + " flushes are disabled - pending translog recovery");
        }
    }

    @Override
    public void setMinSeqNoToKeep(long seqNo) {
        translog.setMinSeqNoToKeep(seqNo);
    }

    public void onDelete() {
        translog.onDelete();
    }

    @Override
    public Translog.TranslogGeneration getTranslogGeneration() {
        return translog.getGeneration();
    }

    /**
     * Reads operations from the translog
     * @param location location of translog
     * @return the translog operation
     * @throws IOException throws an IO exception
     */
    @Override
    public Translog.Operation readOperation(Translog.Location location) throws IOException {
        return translog.readOperation(location);
    }

    /**
     * Adds an operation to the translog
     * @param operation operation to add to translog
     * @return the location in the translog
     * @throws IOException throws an IO exception
     */
    @Override
    public Translog.Location add(Translog.Operation operation) throws IOException {
        return translog.add(operation);
    }

    /**
     * Do not replay translog operations, but make the engine be ready.
     */
    @Override
    public void skipTranslogRecovery() {
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false); // we are good - now we can commit
    }

    // visible for testing
    // TODO refactor tests to remove public access to translog
    public Translog getTranslog() {
        return translog;
    }

    private Translog getTranslog(boolean ensureOpen) {
        if (ensureOpen) {
            this.engineLifeCycleAware.ensureOpen();
        }
        return translog;
    }

    protected Translog openTranslog(
        TranslogConfig translogConfig,
        LongSupplier primaryTermSupplier,
        TranslogDeletionPolicy translogDeletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        String translogUUID,
        TranslogFactory translogFactory,
        BooleanSupplier primaryModeSupplier
    ) throws IOException {
        return translogFactory.newTranslog(
            translogConfig,
            translogUUID,
            translogDeletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            primaryModeSupplier
        );
    }

    /**
     * Retrieves last synced global checkpoint
     * @return last synced global checkpoint
     */
    public long getLastSyncedGlobalCheckpoint() {
        return translog.getLastSyncedGlobalCheckpoint();
    }

    /**
     * Retrieves the max seq no
     * @return max seq no
     */
    public long getMaxSeqNo() {
        return translog.getMaxSeqNo();
    }

    /**
     * Trims unreferenced translog generations by asking {@link TranslogDeletionPolicy} for the minimum
     * required generation
     */
    public void trimUnreferencedReaders() throws IOException {
        translog.trimUnreferencedReaders();
    }

    /**
     * Retrieves the translog deletion policy
     * @return TranslogDeletionPolicy
     */
    public TranslogDeletionPolicy getDeletionPolicy() {
        return translog.getDeletionPolicy();
    }

    /**
     * Retrieves the underlying translog tragic exception
     * @return the tragic exception
     */
    public Exception getTragicExceptionIfClosed() {
        return translog.isOpen() == false ? translog.getTragicException() : null;
    }

    /**
     * Retrieves the translog unique identifier
     * @return the uuid of the translog
     */
    public String getTranslogUUID() {
        return translog.getTranslogUUID();
    }

    /**
     *
     * @param localCheckpointOfLastCommit local checkpoint reference of last commit to translog
     * @param flushThreshold threshold to flush to translog
     * @return if the translog should be flushed
     */
    public boolean shouldPeriodicallyFlush(long localCheckpointOfLastCommit, long flushThreshold) {
        final long translogGenerationOfLastCommit = translog.getMinGenerationForSeqNo(
            localCheckpointOfLastCommit + 1
        ).translogFileGeneration;
        if (translog.sizeInBytesByMinGen(translogGenerationOfLastCommit) < flushThreshold) {
            return false;
        }
        /*
         * We flush to reduce the size of uncommitted translog but strictly speaking the uncommitted size won't always be
         * below the flush-threshold after a flush. To avoid getting into an endless loop of flushing, we only enable the
         * periodically flush condition if this condition is disabled after a flush. The condition will change if the new
         * commit points to the later generation the last commit's(eg. gen-of-last-commit < gen-of-new-commit)[1].
         *
         * When the local checkpoint equals to max_seqno, and translog-gen of the last commit equals to translog-gen of
         * the new commit, we know that the last generation must contain operations because its size is above the flush
         * threshold and the flush-threshold is guaranteed to be higher than an empty translog by the setting validation.
         * This guarantees that the new commit will point to the newly rolled generation. In fact, this scenario only
         * happens when the generation-threshold is close to or above the flush-threshold; otherwise we have rolled
         * generations as the generation-threshold was reached, then the first condition (eg. [1]) is already satisfied.
         *
         * This method is to maintain translog only, thus IndexWriter#hasUncommittedChanges condition is not considered.
         */
        final long translogGenerationOfNewCommit = translog.getMinGenerationForSeqNo(
            localCheckpointTrackerSupplier.get().getProcessedCheckpoint() + 1
        ).translogFileGeneration;
        return translogGenerationOfLastCommit < translogGenerationOfNewCommit
            || localCheckpointTrackerSupplier.get().getProcessedCheckpoint() == localCheckpointTrackerSupplier.get().getMaxSeqNo();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(translog);
    }
}
