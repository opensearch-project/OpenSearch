/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogStats;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * Abstract Engine implementation that provides common Translog functionality.
 * This class creates a closeable {@link Translog}, it is up to subclasses to close the translog reference.
 *
 * @opensearch.internal
 */
public abstract class TranslogAwareEngine extends Engine {

    protected final Translog translog;
    protected final TranslogDeletionPolicy translogDeletionPolicy;

    protected TranslogAwareEngine(EngineConfig engineConfig) {
        super(engineConfig);
        store.incRef();
        TranslogDeletionPolicy customTranslogDeletionPolicy = null;
        if (engineConfig.getCustomTranslogDeletionPolicyFactory() != null) {
            customTranslogDeletionPolicy = engineConfig.getCustomTranslogDeletionPolicyFactory()
                .create(engineConfig.getIndexSettings(), engineConfig.retentionLeasesSupplier());
        }
        translogDeletionPolicy = Objects.requireNonNullElseGet(customTranslogDeletionPolicy, () -> new DefaultTranslogDeletionPolicy(
            engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
            engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
            engineConfig.getIndexSettings().getTranslogRetentionTotalFiles()
        ));
        try {
            store.trimUnsafeCommits(engineConfig.getTranslogConfig().getTranslogPath());
            translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier(), seqNo -> {
                final LocalCheckpointTracker tracker = getLocalCheckpointTracker();
                assert tracker != null || getTranslog().isOpen() == false;
                if (tracker != null) {
                    tracker.markSeqNoAsPersisted(seqNo);
                }
            });
            assert translog.getGeneration() != null;
        } catch (IOException | TranslogCorruptedException e) {
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        } finally {
            store.decRef();
        }
    }

    @Override
    public final int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = getLocalCheckpointTracker().getProcessedCheckpoint();
            try (Translog.Snapshot snapshot = getTranslog().newSnapshot(localCheckpoint + 1, Long.MAX_VALUE)) {
                return translogRecoveryRunner.run(this, snapshot);
            }
        }
    }

    @Override
    public final boolean isTranslogSyncNeeded() {
        return getTranslog().syncNeeded();
    }

    @Override
    public final void syncTranslog() throws IOException {
        translog.sync();
        deleteUnusedFiles();
        translog.trimUnreferencedReaders();
    }

    @Override
    public final boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        boolean synced = translog.ensureSynced(locations);
        if (synced) {
            deleteUnusedFiles();
            translog.trimUnreferencedReaders();
        }
        return synced;
    }

    @Override
    public final void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimOperations(belowTerm, aboveSeqNo);
        } catch (AlreadyClosedException e) {
            maybeFailEngine("trimOperationsFromTranslog", e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog operations trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog operations", e);
        }
    }

    @Override
    public final TranslogStats getTranslogStats() {
        return getTranslog().stats();
    }

    @Override
    public final Translog.Location getTranslogLastWriteLocation() {
        return getTranslog().getLastWriteLocation();
    }

    @Override
    public final long getLastSyncedGlobalCheckpoint() {
        return getTranslog().getLastSyncedGlobalCheckpoint();
    }

    @Override
    public final void rollTranslogGeneration() throws EngineException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            translog.rollGeneration();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            maybeFailEngine("rollTranslogGeneration", e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to roll translog", e);
        }
    }

    @Override
    public final boolean shouldRollTranslogGeneration() {
        return getTranslog().shouldRollGeneration();
    }

    @Override
    public final void trimUnreferencedTranslogFiles() throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            maybeFailEngine("trimUnreferencedTranslogFiles", e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog", e);
        }
    }

    protected final void refreshTranslogDeletionPolicy(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize) {
        final TranslogDeletionPolicy translogDeletionPolicy = translog.getDeletionPolicy();
        translogDeletionPolicy.setRetentionAgeInMillis(translogRetentionAge.millis());
        translogDeletionPolicy.setRetentionSizeInBytes(translogRetentionSize.getBytes());
    }

    protected final Translog getTranslog() {
        ensureOpen();
        return translog;
    }

    protected final void addIndexOperationToTranslog(Index index, IndexResult indexResult) throws IOException {
        Translog.Location location = null;
        if (indexResult.getResultType() == Result.Type.SUCCESS) {
            location = translog.add(new Translog.Index(index, indexResult));
        } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            // if we have document failure, record it as a no-op in the translog and Lucene with the generated seq_no
            final NoOp noOp = new NoOp(
                indexResult.getSeqNo(),
                index.primaryTerm(),
                index.origin(),
                index.startTime(),
                indexResult.getFailure().toString()
            );
            location = noOp(noOp).getTranslogLocation();
        }
        indexResult.setTranslogLocation(location);
    }

    protected final void addDeleteOperationToTranslog(Delete delete, DeleteResult deleteResult) throws IOException {
        if (deleteResult.getResultType() == Result.Type.SUCCESS) {
            final Translog.Location location = translog.add(new Translog.Delete(delete, deleteResult));
            deleteResult.setTranslogLocation(location);
        }
    }

    private Translog openTranslog(
        EngineConfig engineConfig,
        TranslogDeletionPolicy translogDeletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongConsumer persistedSequenceNumberConsumer
    ) throws IOException {
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        store.incRef();
        try {
            final Map<String, String> userData = store.readLastCommittedSegmentsInfo().getUserData();
            final String translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));
            // We expect that this shard already exists, so it must already have an existing translog else something is badly wrong!
            return new Translog(
                translogConfig,
                translogUUID,
                translogDeletionPolicy,
                globalCheckpointSupplier,
                engineConfig.getPrimaryTermSupplier(),
                persistedSequenceNumberConsumer
            );
        } finally {
            store.decRef();
        }
    }

    /**
     * Fetch a reference to this Engine's {@link LocalCheckpointTracker}
     *
     * @return {@link LocalCheckpointTracker}
     */
    protected abstract LocalCheckpointTracker getLocalCheckpointTracker();

    /**
     * Allow implementations to delete unused index files after a translog sync.
     *
     * @throws IOException - When there is an IO error deleting unused files from the index.
     */
    protected abstract void deleteUnusedFiles() throws IOException;
}
