/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.index.translog.listener.TranslogEventListener;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Shared translog-only helpers used by both {@link NRTReplicationEngine} and
 * {@link DataFormatAwareNRTReplicationEngine}. Both engines receive operations with
 * pre-assigned seq-nos and only append them to the translog; the three write methods below
 * are byte-identical copies of the historical {@link NRTReplicationEngine} implementation.
 * Also exposes a factory for the shared {@link TranslogEventListener} and the
 * {@link TranslogDeletionPolicy} selection logic so both engines construct their translog
 * manager identically.
 *
 * <p>Package-private; stateless.
 */
final class NRTReplicaTranslogOps {

    private NRTReplicaTranslogOps() {}

    static Engine.IndexResult index(
        WriteOnlyTranslogManager translogManager,
        LocalCheckpointTracker localCheckpointTracker,
        Engine.Index index
    ) throws IOException {
        Engine.IndexResult indexResult = new Engine.IndexResult(index.version(), index.primaryTerm(), index.seqNo(), false);
        final Translog.Location location = translogManager.add(new Translog.Index(index, indexResult));
        indexResult.setTranslogLocation(location);
        indexResult.setTook(System.nanoTime() - index.startTime());
        indexResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(index.seqNo());
        return indexResult;
    }

    static Engine.DeleteResult delete(
        WriteOnlyTranslogManager translogManager,
        LocalCheckpointTracker localCheckpointTracker,
        Engine.Delete delete
    ) throws IOException {
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true);
        final Translog.Location location = translogManager.add(new Translog.Delete(delete, deleteResult));
        deleteResult.setTranslogLocation(location);
        deleteResult.setTook(System.nanoTime() - delete.startTime());
        deleteResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(delete.seqNo());
        return deleteResult;
    }

    static Engine.NoOpResult noOp(WriteOnlyTranslogManager translogManager, LocalCheckpointTracker localCheckpointTracker, Engine.NoOp noOp)
        throws IOException {
        Engine.NoOpResult noOpResult = new Engine.NoOpResult(noOp.primaryTerm(), noOp.seqNo());
        final Translog.Location location = translogManager.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
        noOpResult.setTranslogLocation(location);
        noOpResult.setTook(System.nanoTime() - noOp.startTime());
        noOpResult.freeze();
        localCheckpointTracker.advanceMaxSeqNo(noOp.seqNo());
        return noOpResult;
    }

    /**
     * Creates the {@link TranslogEventListener} used by both replica engines. The listener
     * forwards translog failures to the engine's {@code failEngine} hook and trims unreferenced
     * readers on sync. The {@code translogManagerSupplier} lazily returns the manager reference
     * so the listener can be constructed before the manager field is assigned.
     */
    static TranslogEventListener createTranslogEventListener(
        BiConsumer<String, Exception> failEngine,
        Supplier<? extends TranslogManager> translogManagerSupplier,
        ShardId shardId
    ) {
        return new TranslogEventListener() {
            @Override
            public void onFailure(String reason, Exception ex) {
                failEngine.accept(reason, ex);
            }

            @Override
            public void onAfterTranslogSync() {
                try {
                    translogManagerSupplier.get().trimUnreferencedReaders();
                } catch (IOException ex) {
                    throw new TranslogException(shardId, "failed to trim unreferenced translog readers", ex);
                }
            }
        };
    }

    /**
     * Selects a translog deletion policy: the engine-config-provided custom factory if present,
     * otherwise {@link DefaultTranslogDeletionPolicy}. Mirrors {@code Engine.getTranslogDeletionPolicy}.
     */
    static TranslogDeletionPolicy getTranslogDeletionPolicy(EngineConfig engineConfig) {
        TranslogDeletionPolicy customTranslogDeletionPolicy = null;
        if (engineConfig.getCustomTranslogDeletionPolicyFactory() != null) {
            customTranslogDeletionPolicy = engineConfig.getCustomTranslogDeletionPolicyFactory()
                .create(engineConfig.getIndexSettings(), engineConfig.retentionLeasesSupplier());
        }
        return Objects.requireNonNullElseGet(
            customTranslogDeletionPolicy,
            () -> new DefaultTranslogDeletionPolicy(
                engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
                engineConfig.getIndexSettings().getTranslogRetentionTotalFiles()
            )
        );
    }
}
