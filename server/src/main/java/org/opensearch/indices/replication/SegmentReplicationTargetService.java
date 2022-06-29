/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.breaker.CircuitBreakingException;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.recovery.DelayRecoveryException;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.recovery.RecoveryListener;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationCollection.ReplicationRef;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.SendRequestTransportException;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.PREPARE_SHARD;

/**
 * Service class that orchestrates replication events on replicas.
 *
 * @opensearch.internal
 */
public class SegmentReplicationTargetService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SegmentReplicationTargetService.class);

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;

    private final ReplicationCollection<SegmentReplicationTarget> onGoingReplications;

    private final SegmentReplicationSourceFactory sourceFactory;
    private final TransportService transportService;

    /**
     * The internal actions
     *
     * @opensearch.internal
     */
    public static class Actions {
        public static final String FILE_CHUNK = "internal:index/shard/replication/file_chunk";
    }

    public SegmentReplicationTargetService(
        final ThreadPool threadPool,
        final RecoverySettings recoverySettings,
        final TransportService transportService,
        final SegmentReplicationSourceFactory sourceFactory
    ) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.onGoingReplications = new ReplicationCollection<>(logger, threadPool);
        this.sourceFactory = sourceFactory;
        this.transportService = transportService;

        transportService.registerRequestHandler(
            Actions.FILE_CHUNK,
            ThreadPool.Names.GENERIC,
            FileChunkRequest::new,
            new FileChunkTransportRequestHandler()
        );
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingReplications.cancelForShard(shardId, "shard closed");
        }
    }

    /**
     * Invoked when a new checkpoint is received from a primary shard.
     * It checks if a new checkpoint should be processed or not and starts replication if needed.
     *
     * @param receivedCheckpoint received checkpoint that is checked for processing
     * @param replicaShard       replica shard on which checkpoint is received
     */
    public synchronized void onNewCheckpoint(final ReplicationCheckpoint receivedCheckpoint, final IndexShard replicaShard) {
        if (onGoingReplications.isShardReplicating(replicaShard.shardId())) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "Ignoring new replication checkpoint - shard is currently replicating to checkpoint {}",
                    replicaShard.getLatestReplicationCheckpoint()
                )
            );
            return;
        }
        if (replicaShard.shouldProcessCheckpoint(receivedCheckpoint)) {
            startReplication(receivedCheckpoint, replicaShard, new SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {}

                @Override
                public void onReplicationFailure(SegmentReplicationState state, OpenSearchException e, boolean sendShardFailure) {
                    if (sendShardFailure == true) {
                        logger.error("replication failure", e);
                        replicaShard.failShard("replication failure", e);
                    }
                }
            });

        }
    }

    public void recoverShard(IndexShard indexShard, DiscoveryNode sourceNode, RecoveryListener recoveryListener) throws IOException {
        indexShard.prepareForIndexRecovery();
        final Store store = indexShard.store();
        StepListener<PrepareShardResponse> listener = new StepListener<>();
        final boolean isEmptyReplica = store.directory().listAll().length == 0;
        if (isEmptyReplica) {
            final TimeValue initialDelay = TimeValue.timeValueMillis(200);
            final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
            final RetryableAction retryableAction = new RetryableAction(logger, threadPool, initialDelay, timeout, listener) {
                @Override
                public void tryAction(ActionListener listener) {
                    transportService.sendRequest(
                        sourceNode,
                        PREPARE_SHARD,
                        new PrepareShardRequest(indexShard.shardId()),
                        TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
                        new ActionListenerResponseHandler<>(listener, PrepareShardResponse::new)
                    );
                }

                @Override
                public boolean shouldRetry(Exception e) {
                    if (e instanceof ConnectTransportException) {
                        return true;
                    } else if (e instanceof SendRequestTransportException) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(e);
                        return cause instanceof ConnectTransportException;
                    } else if (e instanceof RemoteTransportException) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(e);
                        return cause instanceof CircuitBreakingException
                            || cause instanceof OpenSearchRejectedExecutionException
                            || cause instanceof DelayRecoveryException;
                    }
                    return false;
                }
            };
            retryableAction.run();
        } else {
            listener.onResponse(null);
        }
        listener.whenComplete(r -> {
            // if the shard has no files, create an empty store. This will create a Segments_N file, but it will be overwritten
            // on the first copy event.
            if (isEmptyReplica) {
                store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
                final String uuid = r.getUuid();
                Translog.createEmptyTranslog(
                    indexShard.shardPath().resolveTranslog(),
                    indexShard.shardId(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    indexShard.getPendingPrimaryTerm(),
                    uuid,
                    FileChannel::open
                );
                store.associateIndexWithNewTranslog(uuid);
            }
            // start the shard.
            indexShard.persistRetentionLeases();
            indexShard.maybeCheckIndex();
            indexShard.recoveryState().setStage(RecoveryState.Stage.TRANSLOG);
            indexShard.openEngineAndSkipTranslogRecovery();
            if (isEmptyReplica) {
                // wipe the index, this ensures any segments_N created in the steps above is wiped.
                Lucene.cleanLuceneIndex(indexShard.store().directory());
            }
            startReplication(indexShard.getLatestReplicationCheckpoint(), indexShard, new SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {
                    indexShard.recoveryState().getIndex().setFileDetailsComplete();
                    indexShard.finalizeRecovery();
                    indexShard.recoverTranslogFromLuceneChangesSnapshot();
                    indexShard.postRecovery("Shard setup complete.");
                    recoveryListener.onDone(indexShard.recoveryState());
                }

                @Override
                public void onReplicationFailure(SegmentReplicationState state, OpenSearchException e, boolean sendShardFailure) {
                    recoveryListener.onFailure(indexShard.recoveryState(), e, sendShardFailure);
                }
            });
        }, e -> { recoveryListener.onFailure(indexShard.recoveryState(), new OpenSearchException(e), true); });
    }

    public void startReplication(
        final ReplicationCheckpoint checkpoint,
        final IndexShard indexShard,
        final SegmentReplicationListener listener
    ) {
        startReplication(new SegmentReplicationTarget(checkpoint, indexShard, sourceFactory.get(indexShard), listener));
    }

    public void startReplication(final SegmentReplicationTarget target) {
        final long replicationId = onGoingReplications.start(target, recoverySettings.activityTimeout());
        logger.trace(() -> new ParameterizedMessage("Starting replication {}", replicationId));
        threadPool.generic().execute(new ReplicationRunner(replicationId));
    }

    /**
     * Listener that runs on changes in Replication state
     *
     * @opensearch.internal
     */
    public interface SegmentReplicationListener extends ReplicationListener {

        @Override
        default void onDone(ReplicationState state) {
            onReplicationDone((SegmentReplicationState) state);
        }

        @Override
        default void onFailure(ReplicationState state, OpenSearchException e, boolean sendShardFailure) {
            onReplicationFailure((SegmentReplicationState) state, e, sendShardFailure);
        }

        void onReplicationDone(SegmentReplicationState state);

        void onReplicationFailure(SegmentReplicationState state, OpenSearchException e, boolean sendShardFailure);
    }

    /**
     * Runnable implementation to trigger a replication event.
     */
    private class ReplicationRunner implements Runnable {

        final long replicationId;

        public ReplicationRunner(long replicationId) {
            this.replicationId = replicationId;
        }

        @Override
        public void run() {
            start(replicationId);
        }
    }

    private void start(final long replicationId) {
        try (ReplicationRef<SegmentReplicationTarget> replicationRef = onGoingReplications.get(replicationId)) {
            replicationRef.get().startReplication(new ActionListener<>() {
                @Override
                public void onResponse(Void o) {
                    onGoingReplications.markAsDone(replicationId);
                }

                @Override
                public void onFailure(Exception e) {
                    onGoingReplications.fail(replicationId, new OpenSearchException("Segment Replication failed", e), true);
                }
            });
        }
    }

    private class FileChunkTransportRequestHandler implements TransportRequestHandler<FileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final FileChunkRequest request, TransportChannel channel, Task task) throws Exception {
            try (ReplicationRef<SegmentReplicationTarget> ref = onGoingReplications.getSafe(request.recoveryId(), request.shardId())) {
                final SegmentReplicationTarget target = ref.get();
                final ActionListener<Void> listener = target.createOrFinishListener(channel, Actions.FILE_CHUNK, request);
                target.handleFileChunk(request, target, bytesSinceLastPause, recoverySettings.rateLimiter(), listener);
            }
        }
    }
}
