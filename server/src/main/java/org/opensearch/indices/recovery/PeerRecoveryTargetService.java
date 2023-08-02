/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRunnable;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.RecoveryEngineException;
import org.opensearch.index.mapper.MapperException;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationCollection.ReplicationRef;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 * <p>
 * Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
 *
 * @opensearch.internal
 */
public class PeerRecoveryTargetService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(PeerRecoveryTargetService.class);

    /**
     * The internal actions
     *
     * @opensearch.internal
     */
    public static class Actions {
        public static final String FILES_INFO = "internal:index/shard/recovery/filesInfo";
        public static final String FILE_CHUNK = "internal:index/shard/recovery/file_chunk";
        public static final String CLEAN_FILES = "internal:index/shard/recovery/clean_files";
        public static final String TRANSLOG_OPS = "internal:index/shard/recovery/translog_ops";
        public static final String PREPARE_TRANSLOG = "internal:index/shard/recovery/prepare_translog";
        public static final String FINALIZE = "internal:index/shard/recovery/finalize";
        public static final String HANDOFF_PRIMARY_CONTEXT = "internal:index/shard/recovery/handoff_primary_context";
    }

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    private final ReplicationCollection<RecoveryTarget> onGoingRecoveries;

    public PeerRecoveryTargetService(
        ThreadPool threadPool,
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService
    ) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
        this.onGoingRecoveries = new ReplicationCollection<>(logger, threadPool);

        transportService.registerRequestHandler(
            Actions.FILES_INFO,
            ThreadPool.Names.GENERIC,
            RecoveryFilesInfoRequest::new,
            new FilesInfoRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.FILE_CHUNK,
            ThreadPool.Names.GENERIC,
            FileChunkRequest::new,
            new FileChunkTransportRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.CLEAN_FILES,
            ThreadPool.Names.GENERIC,
            RecoveryCleanFilesRequest::new,
            new CleanFilesRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.PREPARE_TRANSLOG,
            ThreadPool.Names.GENERIC,
            RecoveryPrepareForTranslogOperationsRequest::new,
            new PrepareForTranslogOperationsRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.TRANSLOG_OPS,
            ThreadPool.Names.GENERIC,
            RecoveryTranslogOperationsRequest::new,
            new TranslogOperationsRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.FINALIZE,
            ThreadPool.Names.GENERIC,
            RecoveryFinalizeRecoveryRequest::new,
            new FinalizeRecoveryRequestHandler()
        );
        transportService.registerRequestHandler(
            Actions.HANDOFF_PRIMARY_CONTEXT,
            ThreadPool.Names.GENERIC,
            RecoveryHandoffPrimaryContextRequest::new,
            new HandoffPrimaryContextRequestHandler()
        );
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingRecoveries.cancelForShard(shardId, "shard closed");
        }
    }

    public void startRecovery(final IndexShard indexShard, final DiscoveryNode sourceNode, final RecoveryListener listener) {
        // create a new recovery status, and process...
        final long recoveryId = onGoingRecoveries.start(
            new RecoveryTarget(indexShard, sourceNode, listener),
            recoverySettings.activityTimeout()
        );
        // we fork off quickly here and go async but this is called from the cluster state applier thread too and that can cause
        // assertions to trip if we executed it on the same thread hence we fork off to the generic threadpool.
        threadPool.generic().execute(new RecoveryRunner(recoveryId));
    }

    protected void retryRecovery(final long recoveryId, final Throwable reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace(() -> new ParameterizedMessage("will retry recovery with id [{}] in [{}]", recoveryId, retryAfter), reason);
        retryRecovery(recoveryId, retryAfter, activityTimeout);
    }

    protected void retryRecovery(final long recoveryId, final String reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace("will retry recovery with id [{}] in [{}] (reason [{}])", recoveryId, retryAfter, reason);
        retryRecovery(recoveryId, retryAfter, activityTimeout);
    }

    private void retryRecovery(final long recoveryId, final TimeValue retryAfter, final TimeValue activityTimeout) {
        RecoveryTarget newTarget = onGoingRecoveries.reset(recoveryId, activityTimeout);
        if (newTarget != null) {
            threadPool.scheduleUnlessShuttingDown(retryAfter, ThreadPool.Names.GENERIC, new RecoveryRunner(newTarget.getId()));
        }
    }

    protected void reestablishRecovery(final StartRecoveryRequest request, final String reason, TimeValue retryAfter) {
        final long recoveryId = request.recoveryId();
        logger.trace("will try to reestablish recovery with id [{}] in [{}] (reason [{}])", recoveryId, retryAfter, reason);
        threadPool.scheduleUnlessShuttingDown(retryAfter, ThreadPool.Names.GENERIC, new RecoveryRunner(recoveryId, request));
    }

    /**
     * Initiates recovery of the replica. TODO - Need to revisit it with PRRL and later. @see
     * <a href="https://github.com/opensearch-project/OpenSearch/issues/4502">github issue</a> on it.
     * @param recoveryId recovery id
     * @param preExistingRequest start recovery request
     */
    private void doRecovery(final long recoveryId, final StartRecoveryRequest preExistingRequest) {
        final String actionName;
        final TransportRequest requestToSend;
        final StartRecoveryRequest startRequest;
        final ReplicationTimer timer;
        try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.get(recoveryId)) {
            if (recoveryRef == null) {
                logger.trace("not running recovery with id [{}] - can not find it (probably finished)", recoveryId);
                return;
            }
            final RecoveryTarget recoveryTarget = recoveryRef.get();
            timer = recoveryTarget.state().getTimer();
            if (preExistingRequest == null) {
                try {
                    final IndexShard indexShard = recoveryTarget.indexShard();
                    indexShard.preRecovery();
                    assert recoveryTarget.sourceNode() != null : "can not do a recovery without a source node";
                    logger.trace("{} preparing shard for peer recovery", recoveryTarget.shardId());
                    indexShard.prepareForIndexRecovery();
                    final boolean hasRemoteSegmentStore = indexShard.indexSettings().isRemoteStoreEnabled();
                    if (hasRemoteSegmentStore) {
                        indexShard.syncSegmentsFromRemoteSegmentStore(false, false);
                    }
                    final boolean hasRemoteTranslog = recoveryTarget.state().getPrimary() == false && indexShard.isRemoteTranslogEnabled();
                    final boolean hasNoTranslog = indexShard.indexSettings().isRemoteSnapshot();
                    final boolean verifyTranslog = (hasRemoteTranslog || hasNoTranslog || hasRemoteSegmentStore) == false;
                    final long startingSeqNo = indexShard.recoverLocallyAndFetchStartSeqNo(!hasRemoteTranslog);
                    assert startingSeqNo == UNASSIGNED_SEQ_NO || recoveryTarget.state().getStage() == RecoveryState.Stage.TRANSLOG
                        : "unexpected recovery stage [" + recoveryTarget.state().getStage() + "] starting seqno [ " + startingSeqNo + "]";
                    startRequest = getStartRecoveryRequest(
                        logger,
                        clusterService.localNode(),
                        recoveryTarget,
                        startingSeqNo,
                        verifyTranslog
                    );
                    requestToSend = startRequest;
                    actionName = PeerRecoverySourceService.Actions.START_RECOVERY;
                } catch (final Exception e) {
                    // this will be logged as warning later on...
                    logger.trace("unexpected error while preparing shard for peer recovery, failing recovery", e);
                    onGoingRecoveries.fail(
                        recoveryId,
                        new RecoveryFailedException(recoveryTarget.state(), "failed to prepare shard for recovery", e),
                        true
                    );
                    return;
                }
                logger.trace("{} starting recovery from {}", startRequest.shardId(), startRequest.sourceNode());
            } else {
                startRequest = preExistingRequest;
                requestToSend = new ReestablishRecoveryRequest(recoveryId, startRequest.shardId(), startRequest.targetAllocationId());
                actionName = PeerRecoverySourceService.Actions.REESTABLISH_RECOVERY;
                logger.trace("{} reestablishing recovery from {}", startRequest.shardId(), startRequest.sourceNode());
            }
        }
        transportService.sendRequest(
            startRequest.sourceNode(),
            actionName,
            requestToSend,
            new RecoveryResponseHandler(startRequest, timer)
        );
    }

    public static StartRecoveryRequest getStartRecoveryRequest(
        Logger logger,
        DiscoveryNode localNode,
        RecoveryTarget recoveryTarget,
        long startingSeqNo
    ) {
        return getStartRecoveryRequest(logger, localNode, recoveryTarget, startingSeqNo, true);
    }

    /**
     * Prepare the start recovery request.
     *
     * @param logger           the logger
     * @param localNode        the local node of the recovery target
     * @param recoveryTarget   the target of the recovery
     * @param startingSeqNo    a sequence number that an operation-based peer recovery can start with.
     *                         This is the first operation after the local checkpoint of the safe commit if exists.
     * @param verifyTranslog should the recovery request validate translog consistency with snapshot store metadata.
     * @return a start recovery request
     */
    public static StartRecoveryRequest getStartRecoveryRequest(
        Logger logger,
        DiscoveryNode localNode,
        RecoveryTarget recoveryTarget,
        long startingSeqNo,
        boolean verifyTranslog
    ) {
        final StartRecoveryRequest request;
        logger.trace("{} collecting local files for [{}]", recoveryTarget.shardId(), recoveryTarget.sourceNode());

        Store.MetadataSnapshot metadataSnapshot;
        try {
            metadataSnapshot = recoveryTarget.indexShard().snapshotStoreMetadata();
            if (verifyTranslog) {
                // Make sure that the current translog is consistent with the Lucene index; otherwise, we have to throw away the Lucene
                // index.
                try {
                    final String expectedTranslogUUID = metadataSnapshot.getCommitUserData().get(Translog.TRANSLOG_UUID_KEY);
                    final long globalCheckpoint = Translog.readGlobalCheckpoint(recoveryTarget.translogLocation(), expectedTranslogUUID);
                    assert globalCheckpoint + 1 >= startingSeqNo : "invalid startingSeqNo " + startingSeqNo + " >= " + globalCheckpoint;
                } catch (IOException | TranslogCorruptedException e) {
                    logger.warn(
                        new ParameterizedMessage(
                            "error while reading global checkpoint from translog, "
                                + "resetting the starting sequence number from {} to unassigned and recovering as if there are none",
                            startingSeqNo
                        ),
                        e
                    );
                    metadataSnapshot = Store.MetadataSnapshot.EMPTY;
                    startingSeqNo = UNASSIGNED_SEQ_NO;
                }
            }
        } catch (final org.apache.lucene.index.IndexNotFoundException e) {
            // happens on an empty folder. no need to log
            assert startingSeqNo == UNASSIGNED_SEQ_NO : startingSeqNo;
            logger.trace("{} shard folder empty, recovering all files", recoveryTarget);
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        } catch (final IOException e) {
            if (startingSeqNo != UNASSIGNED_SEQ_NO) {
                logger.warn(
                    new ParameterizedMessage(
                        "error while listing local files, resetting the starting sequence number from {} "
                            + "to unassigned and recovering as if there are none",
                        startingSeqNo
                    ),
                    e
                );
                startingSeqNo = UNASSIGNED_SEQ_NO;
            } else {
                logger.warn("error while listing local files, recovering as if there are none", e);
            }
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        }
        logger.trace("{} local file count [{}]", recoveryTarget.shardId(), metadataSnapshot.size());
        request = new StartRecoveryRequest(
            recoveryTarget.shardId(),
            recoveryTarget.indexShard().routingEntry().allocationId().getId(),
            recoveryTarget.sourceNode(),
            localNode,
            metadataSnapshot,
            recoveryTarget.state().getPrimary(),
            recoveryTarget.getId(),
            startingSeqNo
        );
        return request;
    }

    class PrepareForTranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryPrepareForTranslogOperationsRequest> {

        @Override
        public void messageReceived(RecoveryPrepareForTranslogOperationsRequest request, TransportChannel channel, Task task) {
            try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.getSafe(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.get();
                final ActionListener<Void> listener = recoveryTarget.createOrFinishListener(channel, Actions.PREPARE_TRANSLOG, request);
                if (listener == null) {
                    return;
                }

                recoveryTarget.prepareForTranslogOperations(request.totalTranslogOps(), listener);
            }
        }
    }

    class FinalizeRecoveryRequestHandler implements TransportRequestHandler<RecoveryFinalizeRecoveryRequest> {

        @Override
        public void messageReceived(RecoveryFinalizeRecoveryRequest request, TransportChannel channel, Task task) throws Exception {
            try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.getSafe(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.get();
                final ActionListener<Void> listener = recoveryTarget.createOrFinishListener(channel, Actions.FINALIZE, request);
                if (listener == null) {
                    return;
                }

                recoveryTarget.finalizeRecovery(request.globalCheckpoint(), request.trimAboveSeqNo(), listener);
            }
        }
    }

    class HandoffPrimaryContextRequestHandler implements TransportRequestHandler<RecoveryHandoffPrimaryContextRequest> {

        @Override
        public void messageReceived(final RecoveryHandoffPrimaryContextRequest request, final TransportChannel channel, Task task)
            throws Exception {
            try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.getSafe(request.recoveryId(), request.shardId())) {
                recoveryRef.get().handoffPrimaryContext(request.primaryContext());
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

    }

    class TranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryTranslogOperationsRequest> {

        @Override
        public void messageReceived(final RecoveryTranslogOperationsRequest request, final TransportChannel channel, Task task)
            throws IOException {
            try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.getSafe(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.get();
                final ActionListener<Void> listener = recoveryTarget.createOrFinishListener(
                    channel,
                    Actions.TRANSLOG_OPS,
                    request,
                    nullVal -> new RecoveryTranslogOperationsResponse(recoveryTarget.indexShard().getLocalCheckpoint())
                );
                if (listener == null) {
                    return;
                }

                performTranslogOps(request, listener, recoveryRef);
            }
        }

        private void performTranslogOps(
            final RecoveryTranslogOperationsRequest request,
            final ActionListener<Void> listener,
            final ReplicationRef<RecoveryTarget> recoveryRef
        ) {
            final RecoveryTarget recoveryTarget = recoveryRef.get();

            final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
            final Consumer<Exception> retryOnMappingException = exception -> {
                // in very rare cases a translog replay from primary is processed before a mapping update on this node
                // which causes local mapping changes since the mapping (clusterstate) might not have arrived on this node.
                logger.debug("delaying recovery due to missing mapping changes", exception);
                // we do not need to use a timeout here since the entire recovery mechanism has an inactivity protection (it will be
                // canceled)
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        threadPool.generic().execute(ActionRunnable.wrap(listener, l -> {
                            try (
                                ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.getSafe(
                                    request.recoveryId(),
                                    request.shardId()
                                )
                            ) {
                                performTranslogOps(request, listener, recoveryRef);
                            }
                        }));
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new OpenSearchException("cluster service was closed while waiting for mapping updates"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        // note that we do not use a timeout (see comment above)
                        listener.onFailure(
                            new OpenSearchTimeoutException("timed out waiting for mapping updates " + "(timeout [" + timeout + "])")
                        );
                    }
                });
            };
            final IndexMetadata indexMetadata = clusterService.state().metadata().index(request.shardId().getIndex());
            final long mappingVersionOnTarget = indexMetadata != null ? indexMetadata.getMappingVersion() : 0L;
            recoveryTarget.indexTranslogOperations(
                request.operations(),
                request.totalTranslogOps(),
                request.maxSeenAutoIdTimestampOnPrimary(),
                request.maxSeqNoOfUpdatesOrDeletesOnPrimary(),
                request.retentionLeases(),
                request.mappingVersionOnPrimary(),
                ActionListener.wrap(checkpoint -> listener.onResponse(null), e -> {
                    // do not retry if the mapping on replica is at least as recent as the mapping
                    // that the primary used to index the operations in the request.
                    if (mappingVersionOnTarget < request.mappingVersionOnPrimary() && e instanceof MapperException) {
                        retryOnMappingException.accept(e);
                    } else {
                        listener.onFailure(e);
                    }
                })
            );
        }
    }

    class FilesInfoRequestHandler implements TransportRequestHandler<RecoveryFilesInfoRequest> {

        @Override
        public void messageReceived(RecoveryFilesInfoRequest request, TransportChannel channel, Task task) throws Exception {
            try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.getSafe(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.get();
                final ActionListener<Void> listener = recoveryTarget.createOrFinishListener(channel, Actions.FILES_INFO, request);
                if (listener == null) {
                    return;
                }

                recoveryTarget.receiveFileInfo(
                    request.phase1FileNames,
                    request.phase1FileSizes,
                    request.phase1ExistingFileNames,
                    request.phase1ExistingFileSizes,
                    request.totalTranslogOps,
                    listener
                );
            }
        }
    }

    class CleanFilesRequestHandler implements TransportRequestHandler<RecoveryCleanFilesRequest> {

        @Override
        public void messageReceived(RecoveryCleanFilesRequest request, TransportChannel channel, Task task) throws Exception {
            try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.getSafe(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.get();
                final ActionListener<Void> listener = recoveryTarget.createOrFinishListener(channel, Actions.CLEAN_FILES, request);
                if (listener == null) {
                    return;
                }

                recoveryTarget.cleanFiles(
                    request.totalTranslogOps(),
                    request.getGlobalCheckpoint(),
                    request.sourceMetaSnapshot(),
                    listener
                );
            }
        }
    }

    class FileChunkTransportRequestHandler implements TransportRequestHandler<FileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final FileChunkRequest request, TransportChannel channel, Task task) throws Exception {
            try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.getSafe(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.get();
                final ActionListener<Void> listener = recoveryTarget.createOrFinishListener(channel, Actions.FILE_CHUNK, request);
                recoveryTarget.handleFileChunk(request, recoveryTarget, bytesSinceLastPause, recoverySettings.rateLimiter(), listener);
            }
        }
    }

    class RecoveryRunner extends AbstractRunnable {

        final long recoveryId;
        private final StartRecoveryRequest startRecoveryRequest;

        RecoveryRunner(long recoveryId) {
            this(recoveryId, null);
        }

        RecoveryRunner(long recoveryId, StartRecoveryRequest startRecoveryRequest) {
            this.recoveryId = recoveryId;
            this.startRecoveryRequest = startRecoveryRequest;
        }

        @Override
        public void onFailure(Exception e) {
            try (ReplicationRef<RecoveryTarget> recoveryRef = onGoingRecoveries.get(recoveryId)) {
                if (recoveryRef != null) {
                    logger.error(() -> new ParameterizedMessage("unexpected error during recovery [{}], failing shard", recoveryId), e);
                    onGoingRecoveries.fail(
                        recoveryId,
                        new RecoveryFailedException(recoveryRef.get().state(), "unexpected error", e),
                        true // be safe
                    );
                } else {
                    logger.debug(
                        () -> new ParameterizedMessage("unexpected error during recovery, but recovery id [{}] is finished", recoveryId),
                        e
                    );
                }
            }
        }

        @Override
        public void doRun() {
            doRecovery(recoveryId, startRecoveryRequest);
        }
    }

    private class RecoveryResponseHandler implements TransportResponseHandler<RecoveryResponse> {

        private final long recoveryId;
        private final StartRecoveryRequest request;
        private final ReplicationTimer timer;

        private RecoveryResponseHandler(final StartRecoveryRequest request, final ReplicationTimer timer) {
            this.recoveryId = request.recoveryId();
            this.request = request;
            this.timer = timer;
        }

        @Override
        public void handleResponse(RecoveryResponse recoveryResponse) {
            final TimeValue recoveryTime = new TimeValue(timer.time());
            // do this through ongoing recoveries to remove it from the collection
            onGoingRecoveries.markAsDone(recoveryId);
            if (logger.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append('[')
                    .append(request.shardId().getIndex().getName())
                    .append(']')
                    .append('[')
                    .append(request.shardId().id())
                    .append("] ");
                sb.append("recovery completed from ").append(request.sourceNode()).append(", took[").append(recoveryTime).append("]\n");
                sb.append("   phase1: recovered_files [")
                    .append(recoveryResponse.phase1FileNames.size())
                    .append("]")
                    .append(" with total_size of [")
                    .append(new ByteSizeValue(recoveryResponse.phase1TotalSize))
                    .append("]")
                    .append(", took [")
                    .append(timeValueMillis(recoveryResponse.phase1Time))
                    .append("], throttling_wait [")
                    .append(timeValueMillis(recoveryResponse.phase1ThrottlingWaitTime))
                    .append(']')
                    .append("\n");
                sb.append("         : reusing_files   [")
                    .append(recoveryResponse.phase1ExistingFileNames.size())
                    .append("] with total_size of [")
                    .append(new ByteSizeValue(recoveryResponse.phase1ExistingTotalSize))
                    .append("]\n");
                sb.append("   phase2: start took [").append(timeValueMillis(recoveryResponse.startTime)).append("]\n");
                sb.append("         : recovered [")
                    .append(recoveryResponse.phase2Operations)
                    .append("]")
                    .append(" transaction log operations")
                    .append(", took [")
                    .append(timeValueMillis(recoveryResponse.phase2Time))
                    .append("]")
                    .append("\n");
                logger.trace("{}", sb);
            } else {
                logger.debug("{} recovery done from [{}], took [{}]", request.shardId(), request.sourceNode(), recoveryTime);
            }
        }

        @Override
        public void handleException(TransportException e) {
            onException(e);
        }

        private void onException(Exception e) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    () -> new ParameterizedMessage(
                        "[{}][{}] Got exception on recovery",
                        request.shardId().getIndex().getName(),
                        request.shardId().id()
                    ),
                    e
                );
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof CancellableThreads.ExecutionCancelledException) {
                // this can also come from the source wrapped in a RemoteTransportException
                onGoingRecoveries.fail(recoveryId, new RecoveryFailedException(request, "source has canceled the recovery", cause), false);
                return;
            }
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }
            // do it twice, in case we have double transport exception
            cause = ExceptionsHelper.unwrapCause(cause);
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }

            // here, we would add checks against exception that need to be retried (and not removeAndClean in this case)

            if (cause instanceof IllegalIndexShardStateException
                || cause instanceof IndexNotFoundException
                || cause instanceof ShardNotFoundException) {
                // if the target is not ready yet, retry
                retryRecovery(
                    recoveryId,
                    "remote shard not ready",
                    recoverySettings.retryDelayStateSync(),
                    recoverySettings.activityTimeout()
                );
                return;
            }

            // PeerRecoveryNotFound is returned when the source node cannot find the recovery requested by
            // the REESTABLISH_RECOVERY request. In this case, we delay and then attempt to restart.
            if (cause instanceof DelayRecoveryException || cause instanceof PeerRecoveryNotFound) {
                retryRecovery(recoveryId, cause, recoverySettings.retryDelayStateSync(), recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof ConnectTransportException) {
                logger.info(
                    "recovery of {} from [{}] interrupted by network disconnect, will retry in [{}]; cause: [{}]",
                    request.shardId(),
                    request.sourceNode(),
                    recoverySettings.retryDelayNetwork(),
                    cause.getMessage()
                );
                reestablishRecovery(request, cause.getMessage(), recoverySettings.retryDelayNetwork());
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                onGoingRecoveries.fail(recoveryId, new RecoveryFailedException(request, "source shard is closed", cause), false);
                return;
            }

            onGoingRecoveries.fail(recoveryId, new RecoveryFailedException(request, e), true);
        }

        @Override
        public String executor() {
            // we do some heavy work like refreshes in the response so fork off to the generic threadpool
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public RecoveryResponse read(StreamInput in) throws IOException {
            return new RecoveryResponse(in);
        }
    }
}
