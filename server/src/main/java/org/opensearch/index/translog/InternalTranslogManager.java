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
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public class InternalTranslogManager extends TranslogManager {

    private final ReleasableLock readLock;
    private final Runnable ensureOpen;
    private final ShardId shardId;
    private Translog translog;
    private final BiConsumer<String, Exception> failEngine;
    private final Function<AlreadyClosedException, Boolean> failOnTragicEvent;
    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);
    private final TranslogManager.TranslogEventListener translogEventListener;
    private static final Logger logger = LogManager.getLogger(TranslogManager.class);

    public InternalTranslogManager(EngineConfig engineConfig, ShardId shardId, ReleasableLock readLock, LocalCheckpointTracker tracker,
                           String translogUUID, TranslogManager.TranslogEventListener translogEventListener, Runnable ensureOpen,
                           BiConsumer<String, Exception> failEngine, Function<AlreadyClosedException, Boolean> failOnTragicEvent) throws IOException {
        this.shardId = shardId;
        this.readLock = readLock;
        this.ensureOpen = ensureOpen;
        this.failEngine = failEngine;
        this.failOnTragicEvent = failOnTragicEvent;
        this.translogEventListener = translogEventListener;
        final TranslogDeletionPolicy translogDeletionPolicy;
        TranslogDeletionPolicy customTranslogDeletionPolicy = null;
        if (engineConfig.getCustomTranslogDeletionPolicyFactory() != null) {
            customTranslogDeletionPolicy = engineConfig.getCustomTranslogDeletionPolicyFactory()
                .create(engineConfig.getIndexSettings(), engineConfig.retentionLeasesSupplier());
        }
        if (customTranslogDeletionPolicy != null) {
            translogDeletionPolicy = customTranslogDeletionPolicy;
        } else {
            translogDeletionPolicy = new DefaultTranslogDeletionPolicy(
                engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
                engineConfig.getIndexSettings().getTranslogRetentionTotalFiles()
            );
        }
        Translog translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier(), seqNo -> {
            assert tracker != null || getTranslog(true).isOpen() == false;
            if (tracker != null) {
                tracker.markSeqNoAsPersisted(seqNo);
            }
        }, translogUUID);
        assert translog.getGeneration() != null;
        this.translog = translog;
        assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
        // don't allow commits until we are done with recovering
        pendingTranslogRecovery.set(true);
    }

    /**
     * Rolls the translog generation and cleans unneeded.
     */
    public void rollTranslogGeneration() throws TranslogException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen.run();
            translog.rollGeneration();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent.apply(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine.accept("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new TranslogException(shardId, "failed to roll translog", e);
        }
    }

    /**
     * Performs recovery from the transaction log up to {@code recoverUpToSeqNo} (inclusive).
     * This operation will close the engine if the recovery fails.
     *
     * @param translogRecoveryRunner the translog recovery runner
     * @param recoverUpToSeqNo       the upper bound, inclusive, of sequence number to be recovered
     */
    public void recoverFromTranslog(Engine engine, TranslogRecoveryRunner translogRecoveryRunner,
                                    long localCheckpoint, long recoverUpToSeqNo, Runnable flush) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen.run();
            if (pendingTranslogRecovery.get() == false) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                recoverFromTranslogInternal(engine, translogRecoveryRunner, localCheckpoint, recoverUpToSeqNo, flush);
            } catch (Exception e) {
                try {
                    pendingTranslogRecovery.set(true); // just play safe and never allow commits on this see #ensureCanFlush
                    failEngine.accept("failed to recover from translog", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                throw e;
            }
        }
    }

    private void recoverFromTranslogInternal(Engine engine, TranslogRecoveryRunner translogRecoveryRunner,
                                             long localCheckpoint, long recoverUpToSeqNo, Runnable flush) throws IOException {
        final int opsRecovered;
        if (localCheckpoint < recoverUpToSeqNo) {
            try (Translog.Snapshot snapshot = translog.newSnapshot(localCheckpoint + 1, recoverUpToSeqNo)) {
                opsRecovered = translogRecoveryRunner.run(engine, snapshot);
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
        flush.run();
        translog.trimUnreferencedReaders();
    }


    /**
     * Checks if the underlying storage sync is required.
     */
    public boolean isTranslogSyncNeeded() {
        return getTranslog(true).syncNeeded();
    }

    /**
     * Ensures that all locations in the given stream have been written to the underlying storage.
     */
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        final boolean synced = translog.ensureSynced(locations);
        if (synced) {
            translogEventListener.onTranslogSync();
        }
        return synced;
    }

    public void syncTranslog() throws IOException {
        translog.sync();
        translogEventListener.onTranslogSync();
    }

    public TranslogStats getTranslogStats() {
        return getTranslog(true).stats();
    }

    /**
     * Returns the last location that the translog of this engine has written into.
     */
    public Translog.Location getTranslogLastWriteLocation() {
        return getTranslog(true).getLastWriteLocation();
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.opensearch.index.translog.TranslogDeletionPolicy} for details
     */
    public void trimUnreferencedTranslogFiles() throws TranslogException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen.run();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent.apply(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine.accept("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new TranslogException(shardId, "failed to trim translog", e);
        }
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation.
     * This test is based on the size of the current generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    public boolean shouldRollTranslogGeneration() {
        return getTranslog(true).shouldRollGeneration();
    }

    /**
     * Trims translog for terms below <code>belowTerm</code> and seq# above <code>aboveSeqNo</code>
     * @see Translog#trimOperations(long, long)
     */
    public void trimOperationsFromTranslog(ShardId shardId, long belowTerm, long aboveSeqNo) throws TranslogException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen.run();
            translog.trimOperations(belowTerm, aboveSeqNo);
        } catch (AlreadyClosedException e) {
            failOnTragicEvent.apply(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine.accept("translog operations trimming failed", e);
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
    public int restoreLocalHistoryFromTranslog(Engine engine, long processedCheckpoint, TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen.run();
            try (Translog.Snapshot snapshot = getTranslog(true).newSnapshot(processedCheckpoint + 1, Long.MAX_VALUE)) {
                return translogRecoveryRunner.run(engine, snapshot);
            }
        }
    }

    public void ensureCanFlush(ShardId shardId) {
        // translog recovery happens after the engine is fully constructed.
        // If we are in this stage we have to prevent flushes from this
        // engine otherwise we might loose documents if the flush succeeds
        // and the translog recovery fails when we "commit" the translog on flush.
        if (pendingTranslogRecovery.get()) {
            throw new IllegalStateException(shardId.toString() + " flushes are disabled - pending translog recovery");
        }
    }

    /**
     * Do not replay translog operations, but make the engine be ready.
     */
    public void skipTranslogRecovery() {
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false); // we are good - now we can commit
    }

    private Translog openTranslog(
        EngineConfig engineConfig,
        TranslogDeletionPolicy translogDeletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        String translogUUID
    ) throws IOException {

        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
        return new Translog(
            translogConfig,
            translogUUID,
            translogDeletionPolicy,
            globalCheckpointSupplier,
            engineConfig.getPrimaryTermSupplier(),
            persistedSequenceNumberConsumer
        );
    }

    public Translog getTranslog(boolean ensureOpen) {
        if (ensureOpen) {
            this.ensureOpen.run();
        }
        return translog;
    }
}
