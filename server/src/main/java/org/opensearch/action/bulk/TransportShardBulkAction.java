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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.MessageSupplier;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.action.support.replication.ReplicationOperation;
import org.opensearch.action.support.replication.ReplicationTask;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.action.support.replication.TransportWriteAction;
import org.opensearch.action.update.UpdateHelper;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.transport.NoNodeAvailableException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.MapperException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.remote.RemoteRefreshSegmentPressureService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Performs shard-level bulk (index, delete or update) operations
 *
 * @opensearch.internal
 */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private static final Logger logger = LogManager.getLogger(TransportShardBulkAction.class);
    private static final Function<IndexShard, String> EXECUTOR_NAME_FUNCTION = shard -> {
        if (shard.indexSettings().getIndexMetadata().isSystem()) {
            return Names.SYSTEM_WRITE;
        } else {
            return Names.WRITE;
        }
    };

    private final UpdateHelper updateHelper;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final SegmentReplicationPressureService segmentReplicationPressureService;
    private final RemoteRefreshSegmentPressureService remoteRefreshSegmentPressureService;

    /**
     * This action is used for performing primary term validation. With remote translog enabled, the translogs would
     * be durably persisted in remote store. Without remote translog, current transport replication calls does primary
     * term validation as well as logically replicate the data. With remote translog, the primary would make calls to
     * replicas to perform primary term validation. This make sures an isolated primary fails to ack after primary
     * term validation in presence of a new primary.
     */
    private final String transportPrimaryTermValidationAction;

    @Inject
    public TransportShardBulkAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        MappingUpdatedAction mappingUpdatedAction,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        IndexingPressureService indexingPressureService,
        SegmentReplicationPressureService segmentReplicationPressureService,
        RemoteRefreshSegmentPressureService remoteRefreshSegmentPressureService,
        SystemIndices systemIndices
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            BulkShardRequest::new,
            BulkShardRequest::new,
            EXECUTOR_NAME_FUNCTION,
            false,
            indexingPressureService,
            systemIndices
        );
        this.updateHelper = updateHelper;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.segmentReplicationPressureService = segmentReplicationPressureService;
        this.remoteRefreshSegmentPressureService = remoteRefreshSegmentPressureService;

        this.transportPrimaryTermValidationAction = ACTION_NAME + "[validate_primary_term]";

        transportService.registerRequestHandler(
            transportPrimaryTermValidationAction,
            executor,
            true,
            true,
            PrimaryTermValidationRequest::new,
            this::handlePrimaryTermValidationRequest
        );
    }

    protected void handlePrimaryTermValidationRequest(
        final PrimaryTermValidationRequest request,
        final TransportChannel channel,
        final Task task
    ) {
        ActionListener<ReplicaResponse> listener = new ChannelActionListener<>(channel, transportPrimaryTermValidationAction, request);
        final ShardId shardId = request.getShardId();
        assert shardId != null : "request shardId must be set";
        IndexShard replica = getIndexShard(shardId);
        try {
            new PrimaryTermValidationReplicaAction(listener, replica, (ReplicationTask) task, request).run();
        } catch (RuntimeException e) {
            listener.onFailure(e);
        }
    }

    /**
     * This action is the primary term validation action which is used for doing primary term validation with replicas.
     * This is only applicable for TransportShardBulkAction because all writes (delete/update/single write/bulk)
     * ultimately boils down to TransportShardBulkAction and isolated primary could continue to acknowledge if it is not
     * aware that the primary has changed. This helps achieve the same. More details in java doc of
     * {@link TransportShardBulkAction#transportPrimaryTermValidationAction}.
     *
     * @opensearch.internal
     */
    private static final class PrimaryTermValidationReplicaAction extends AbstractRunnable implements ActionListener<Releasable> {

        private final ActionListener<ReplicaResponse> onCompletionListener;
        private final IndexShard replica;
        private final ReplicationTask task;
        private final PrimaryTermValidationRequest request;

        public PrimaryTermValidationReplicaAction(
            ActionListener<ReplicaResponse> onCompletionListener,
            IndexShard replica,
            ReplicationTask task,
            PrimaryTermValidationRequest request
        ) {
            this.onCompletionListener = onCompletionListener;
            this.replica = replica;
            this.task = task;
            this.request = request;
        }

        @Override
        public void onResponse(Releasable releasable) {
            setPhase(task, "finished");
            onCompletionListener.onResponse(new ReplicaResponse(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED));
        }

        @Override
        public void onFailure(Exception e) {
            setPhase(task, "failed");
            onCompletionListener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            setPhase(task, "primary-term-validation");
            final String actualAllocationId = this.replica.routingEntry().allocationId().getId();
            if (actualAllocationId.equals(request.getTargetAllocationID()) == false) {
                throw new ShardNotFoundException(
                    this.replica.shardId(),
                    "expected allocation id [{}] but found [{}]",
                    request.getTargetAllocationID(),
                    actualAllocationId
                );
            }
            // Check operation primary term against the incoming primary term
            // If the request primary term is low, then trigger lister failure
            if (request.getPrimaryTerm() < replica.getOperationPrimaryTerm()) {
                final String message = String.format(
                    Locale.ROOT,
                    "%s operation primary term [%d] is too old (current [%d])",
                    request.getShardId(),
                    request.getPrimaryTerm(),
                    replica.getOperationPrimaryTerm()
                );
                onFailure(new IllegalStateException(message));
            } else {
                onResponse(null);
            }
        }
    }

    /**
     * Primary term validation request sent to a specific allocation id
     *
     * @opensearch.internal
     */
    protected static final class PrimaryTermValidationRequest extends TransportRequest {

        /**
         * {@link AllocationId#getId()} of the shard this request is sent to
         **/
        private final String targetAllocationID;
        private final long primaryTerm;
        private final ShardId shardId;

        public PrimaryTermValidationRequest(String targetAllocationID, long primaryTerm, ShardId shardId) {
            this.targetAllocationID = Objects.requireNonNull(targetAllocationID);
            this.primaryTerm = primaryTerm;
            this.shardId = Objects.requireNonNull(shardId);
        }

        public PrimaryTermValidationRequest(StreamInput in) throws IOException {
            super(in);
            targetAllocationID = in.readString();
            primaryTerm = in.readVLong();
            shardId = new ShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(targetAllocationID);
            out.writeVLong(primaryTerm);
            shardId.writeTo(out);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new ReplicationTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        public String getTargetAllocationID() {
            return targetAllocationID;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public String getDescription() {
            return toString();
        }

        @Override
        public String toString() {
            return "PrimaryTermValidationRequest ["
                + shardId
                + "] for targetAllocationID ["
                + targetAllocationID
                + "] with primaryTerm ["
                + primaryTerm
                + "]";
        }
    }

    @Override
    protected ReplicationOperation.Replicas<BulkShardRequest> primaryTermValidationReplicasProxy() {
        return new PrimaryTermValidationProxy();
    }

    /**
     * This {@link org.opensearch.action.support.replication.TransportReplicationAction.ReplicasProxy} implementation is
     * used for primary term validation and is only relevant for TransportShardBulkAction replication action.
     *
     * @opensearch.internal
     */
    private final class PrimaryTermValidationProxy extends WriteActionReplicasProxy {

        @Override
        public void performOn(
            ShardRouting replica,
            BulkShardRequest request,
            long primaryTerm,
            long globalCheckpoint,
            long maxSeqNoOfUpdatesOrDeletes,
            ActionListener<ReplicationOperation.ReplicaResponse> listener
        ) {
            String nodeId = replica.currentNodeId();
            final DiscoveryNode node = clusterService.state().nodes().get(nodeId);
            if (node == null) {
                listener.onFailure(new NoNodeAvailableException("unknown node [" + nodeId + "]"));
                return;
            }
            final PrimaryTermValidationRequest validationRequest = new PrimaryTermValidationRequest(
                replica.allocationId().getId(),
                primaryTerm,
                replica.shardId()
            );
            final ActionListenerResponseHandler<ReplicaResponse> handler = new ActionListenerResponseHandler<>(
                listener,
                ReplicaResponse::new
            );
            transportService.sendRequest(node, transportPrimaryTermValidationAction, validationRequest, transportOptions, handler);
        }
    }

    @Override
    protected TransportRequestOptions transportOptions(Settings settings) {
        return BulkAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected BulkShardResponse newResponseInstance(StreamInput in) throws IOException {
        return new BulkShardResponse(in);
    }

    @Override
    protected void dispatchedShardOperationOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener
    ) {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        performOnPrimary(request, primary, updateHelper, threadPool::absoluteTimeInMillis, (update, shardId, mappingListener) -> {
            assert update != null;
            assert shardId != null;
            mappingUpdatedAction.updateMappingOnClusterManager(shardId.getIndex(), update, mappingListener);
        }, mappingUpdateListener -> observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                mappingUpdateListener.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                mappingUpdateListener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                mappingUpdateListener.onFailure(new MapperException("timed out while waiting for a dynamic mapping update"));
            }
        }), listener, threadPool, executor(primary));
    }

    @Override
    protected long primaryOperationSize(BulkShardRequest request) {
        return request.ramBytesUsed();
    }

    @Override
    public ReplicationMode getReplicationMode(IndexShard indexShard) {
        if (indexShard.isRemoteTranslogEnabled()) {
            return ReplicationMode.PRIMARY_TERM_VALIDATION;
        }
        return super.getReplicationMode(indexShard);
    }

    public static void performOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        Consumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener,
        ThreadPool threadPool,
        String executorName
    ) {
        new ActionRunnable<PrimaryResult<BulkShardRequest, BulkShardResponse>>(listener) {

            private final Executor executor = threadPool.executor(executorName);

            private final BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(request, primary);

            @Override
            protected void doRun() throws Exception {
                while (context.hasMoreOperationsToExecute()) {
                    if (executeBulkItemRequest(
                        context,
                        updateHelper,
                        nowInMillisSupplier,
                        mappingUpdater,
                        waitForMappingUpdate,
                        ActionListener.wrap(v -> executor.execute(this), this::onRejection)
                    ) == false) {
                        // We are waiting for a mapping update on another thread, that will invoke this action again once its done
                        // so we just break out here.
                        return;
                    }
                    assert context.isInitial(); // either completed and moved to next or reset
                }
                // We're done, there's no more operations to execute so we resolve the wrapped listener
                finishRequest();
            }

            @Override
            public void onRejection(Exception e) {
                // We must finish the outstanding request. Finishing the outstanding request can include
                // refreshing and fsyncing. Therefore, we must force execution on the WRITE thread.
                executor.execute(new ActionRunnable<PrimaryResult<BulkShardRequest, BulkShardResponse>>(listener) {

                    @Override
                    protected void doRun() {
                        // Fail all operations after a bulk rejection hit an action that waited for a mapping update and finish the request
                        while (context.hasMoreOperationsToExecute()) {
                            context.setRequestToExecute(context.getCurrent());
                            final DocWriteRequest<?> docWriteRequest = context.getRequestToExecute();
                            onComplete(
                                exceptionToResult(
                                    e,
                                    primary,
                                    docWriteRequest.opType() == DocWriteRequest.OpType.DELETE,
                                    docWriteRequest.version()
                                ),
                                context,
                                null
                            );
                        }
                        finishRequest();
                    }

                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
            }

            private void finishRequest() {
                ActionListener.completeWith(
                    listener,
                    () -> new WritePrimaryResult<>(
                        context.getBulkShardRequest(),
                        context.buildShardResponse(),
                        context.getLocationToSync(),
                        null,
                        context.getPrimary(),
                        logger
                    )
                );
            }
        }.run();
    }

    @Override
    protected Releasable checkPrimaryLimits(BulkShardRequest request, boolean rerouteWasLocal, boolean localRerouteInitiatedByNodeClient) {
        if (force(request) == false) {
            if (segmentReplicationPressureService.isSegmentReplicationBackpressureEnabled()) {
                segmentReplicationPressureService.isSegrepLimitBreached(request.shardId());
            }
            // TODO - While removing remote store flag, this can be encapsulated to single class with common interface for backpressure
            // service
            if (FeatureFlags.isEnabled(FeatureFlags.REMOTE_STORE)
                && remoteRefreshSegmentPressureService.isSegmentsUploadBackpressureEnabled()) {
                remoteRefreshSegmentPressureService.validateSegmentsUploadLag(request.shardId());
            }
        }
        return super.checkPrimaryLimits(request, rerouteWasLocal, localRerouteInitiatedByNodeClient);
    }

    /**
     * Executes bulk item requests and handles request execution exceptions.
     * @return {@code true} if request completed on this thread and the listener was invoked, {@code false} if the request triggered
     *                      a mapping update that will finish and invoke the listener on a different thread
     */
    static boolean executeBulkItemRequest(
        BulkPrimaryExecutionContext context,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        Consumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<Void> itemDoneListener
    ) throws Exception {
        final DocWriteRequest.OpType opType = context.getCurrent().opType();

        final UpdateHelper.Result updateResult;
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            try {
                updateResult = updateHelper.prepare(updateRequest, context.getPrimary(), nowInMillisSupplier);
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                final Engine.Result result = new Engine.IndexResult(failure, updateRequest.version());
                context.setRequestToExecute(updateRequest);
                context.markOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
                return true;
            }
            // execute translated update request
            switch (updateResult.getResponseResult()) {
                case CREATED:
                case UPDATED:
                    IndexRequest indexRequest = updateResult.action();
                    IndexMetadata metadata = context.getPrimary().indexSettings().getIndexMetadata();
                    MappingMetadata mappingMd = metadata.mapping();
                    indexRequest.process(metadata.getCreationVersion(), mappingMd, updateRequest.concreteIndex());
                    context.setRequestToExecute(indexRequest);
                    break;
                case DELETED:
                    context.setRequestToExecute(updateResult.action());
                    break;
                case NOOP:
                    context.markOperationAsNoOp(updateResult.action());
                    context.markAsCompleted(context.getExecutionResult());
                    return true;
                default:
                    throw new IllegalStateException("Illegal update operation " + updateResult.getResponseResult());
            }
        } else {
            context.setRequestToExecute(context.getCurrent());
            updateResult = null;
        }

        assert context.getRequestToExecute() != null; // also checks that we're in TRANSLATED state

        final IndexShard primary = context.getPrimary();
        final long version = context.getRequestToExecute().version();
        final boolean isDelete = context.getRequestToExecute().opType() == DocWriteRequest.OpType.DELETE;
        final Engine.Result result;
        if (isDelete) {
            final DeleteRequest request = context.getRequestToExecute();
            result = primary.applyDeleteOperationOnPrimary(
                version,
                request.id(),
                request.versionType(),
                request.ifSeqNo(),
                request.ifPrimaryTerm()
            );
        } else {
            final IndexRequest request = context.getRequestToExecute();
            result = primary.applyIndexOperationOnPrimary(
                version,
                request.versionType(),
                new SourceToParse(request.index(), request.id(), request.source(), request.getContentType(), request.routing()),
                request.ifSeqNo(),
                request.ifPrimaryTerm(),
                request.getAutoGeneratedTimestamp(),
                request.isRetry()
            );
        }
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {

            try {
                primary.mapperService()
                    .merge(
                        MapperService.SINGLE_MAPPING_NAME,
                        new CompressedXContent(result.getRequiredMappingUpdate(), ToXContent.EMPTY_PARAMS),
                        MapperService.MergeReason.MAPPING_UPDATE_PREFLIGHT
                    );
            } catch (Exception e) {
                logger.info(() -> new ParameterizedMessage("{} mapping update rejected by primary", primary.shardId()), e);
                onComplete(exceptionToResult(e, primary, isDelete, version), context, updateResult);
                return true;
            }

            mappingUpdater.updateMappings(result.getRequiredMappingUpdate(), primary.shardId(), new ActionListener<Void>() {
                @Override
                public void onResponse(Void v) {
                    context.markAsRequiringMappingUpdate();
                    waitForMappingUpdate.accept(ActionListener.runAfter(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void v) {
                            assert context.requiresWaitingForMappingUpdate();
                            context.resetForExecutionForRetry();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            context.failOnMappingUpdate(e);
                        }
                    }, () -> itemDoneListener.onResponse(null)));
                }

                @Override
                public void onFailure(Exception e) {
                    onComplete(exceptionToResult(e, primary, isDelete, version), context, updateResult);
                    // Requesting mapping update failed, so we don't have to wait for a cluster state update
                    assert context.isInitial();
                    itemDoneListener.onResponse(null);
                }
            });
            return false;
        } else {
            onComplete(result, context, updateResult);
        }
        return true;
    }

    private static Engine.Result exceptionToResult(Exception e, IndexShard primary, boolean isDelete, long version) {
        return isDelete ? primary.getFailedDeleteResult(e, version) : primary.getFailedIndexResult(e, version);
    }

    private static void onComplete(Engine.Result r, BulkPrimaryExecutionContext context, UpdateHelper.Result updateResult) {
        context.markOperationAsExecuted(r);
        final DocWriteRequest<?> docWriteRequest = context.getCurrent();
        final DocWriteRequest.OpType opType = docWriteRequest.opType();
        final boolean isUpdate = opType == DocWriteRequest.OpType.UPDATE;
        final BulkItemResponse executionResult = context.getExecutionResult();
        final boolean isFailed = executionResult.isFailed();
        if (isUpdate
            && isFailed
            && isConflictException(executionResult.getFailure().getCause())
            && context.getRetryCounter() < ((UpdateRequest) docWriteRequest).retryOnConflict()) {
            context.resetForExecutionForRetry();
            return;
        }
        final BulkItemResponse response;
        if (isUpdate) {
            response = processUpdateResponse((UpdateRequest) docWriteRequest, context.getConcreteIndex(), executionResult, updateResult);
        } else {
            if (isFailed) {
                final Exception failure = executionResult.getFailure().getCause();
                final MessageSupplier messageSupplier = () -> new ParameterizedMessage(
                    "{} failed to execute bulk item ({}) {}",
                    context.getPrimary().shardId(),
                    opType.getLowercase(),
                    docWriteRequest
                );
                if (TransportShardBulkAction.isConflictException(failure)) {
                    logger.trace(messageSupplier, failure);
                } else {
                    logger.debug(messageSupplier, failure);
                }
            }
            response = executionResult;
        }
        context.markAsCompleted(response);
        assert context.isInitial();
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    /**
     * Creates a new bulk item result from the given requests and result of performing the update operation on the shard.
     */
    static BulkItemResponse processUpdateResponse(
        final UpdateRequest updateRequest,
        final String concreteIndex,
        BulkItemResponse operationResponse,
        final UpdateHelper.Result translate
    ) {
        final BulkItemResponse response;
        if (operationResponse.isFailed()) {
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, operationResponse.getFailure());
        } else {
            final DocWriteResponse.Result translatedResult = translate.getResponseResult();
            final UpdateResponse updateResponse;
            if (translatedResult == DocWriteResponse.Result.CREATED || translatedResult == DocWriteResponse.Result.UPDATED) {
                final IndexRequest updateIndexRequest = translate.action();
                final IndexResponse indexResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(
                    indexResponse.getShardInfo(),
                    indexResponse.getShardId(),
                    indexResponse.getId(),
                    indexResponse.getSeqNo(),
                    indexResponse.getPrimaryTerm(),
                    indexResponse.getVersion(),
                    indexResponse.getResult()
                );

                if (updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) {
                    final BytesReference indexSourceAsBytes = updateIndexRequest.source();
                    final Tuple<? extends MediaType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(
                        indexSourceAsBytes,
                        true,
                        updateIndexRequest.getContentType()
                    );
                    updateResponse.setGetResult(
                        UpdateHelper.extractGetResult(
                            updateRequest,
                            concreteIndex,
                            indexResponse.getSeqNo(),
                            indexResponse.getPrimaryTerm(),
                            indexResponse.getVersion(),
                            sourceAndContent.v2(),
                            sourceAndContent.v1(),
                            indexSourceAsBytes
                        )
                    );
                }
            } else if (translatedResult == DocWriteResponse.Result.DELETED) {
                final DeleteResponse deleteResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(
                    deleteResponse.getShardInfo(),
                    deleteResponse.getShardId(),
                    deleteResponse.getId(),
                    deleteResponse.getSeqNo(),
                    deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(),
                    deleteResponse.getResult()
                );

                final GetResult getResult = UpdateHelper.extractGetResult(
                    updateRequest,
                    concreteIndex,
                    deleteResponse.getSeqNo(),
                    deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(),
                    translate.updatedSourceAsMap(),
                    translate.updateSourceContentType(),
                    null
                );

                updateResponse.setGetResult(getResult);
            } else {
                throw new IllegalArgumentException("unknown operation type: " + translatedResult);
            }
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, updateResponse);
        }
        return response;
    }

    @Override
    protected void dispatchedShardOperationOnReplica(BulkShardRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            final Translog.Location location = performOnReplica(request, replica);
            return new WriteReplicaResult<>(request, location, null, replica, logger);
        });
    }

    @Override
    protected long replicaOperationSize(BulkShardRequest request) {
        return request.ramBytesUsed();
    }

    public static Translog.Location performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            final BulkItemRequest item = request.items()[i];
            final BulkItemResponse response = item.getPrimaryResponse();
            final Engine.Result operationResult;
            if (item.getPrimaryResponse().isFailed()) {
                if (response.getFailure().getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    continue; // ignore replication as we didn't generate a sequence number for this request.
                }

                final long primaryTerm;
                if (response.getFailure().getTerm() == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                    // primary is on older version, just take the current primary term
                    primaryTerm = replica.getOperationPrimaryTerm();
                } else {
                    primaryTerm = response.getFailure().getTerm();
                }
                operationResult = replica.markSeqNoAsNoop(
                    response.getFailure().getSeqNo(),
                    primaryTerm,
                    response.getFailure().getMessage()
                );
            } else {
                if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                    continue; // ignore replication as it's a noop
                }
                assert response.getResponse().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
                operationResult = performOpOnReplica(response.getResponse(), item.request(), replica);
            }
            assert operationResult != null : "operation result must never be null when primary response has no failure";
            location = syncOperationResultOrThrow(operationResult, location);
        }
        return location;
    }

    private static Engine.Result performOpOnReplica(
        DocWriteResponse primaryResponse,
        DocWriteRequest<?> docWriteRequest,
        IndexShard replica
    ) throws Exception {
        final Engine.Result result;
        switch (docWriteRequest.opType()) {
            case CREATE:
            case INDEX:
                final IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                final ShardId shardId = replica.shardId();
                final SourceToParse sourceToParse = new SourceToParse(
                    shardId.getIndexName(),
                    indexRequest.id(),
                    indexRequest.source(),
                    indexRequest.getContentType(),
                    indexRequest.routing()
                );
                result = replica.applyIndexOperationOnReplica(
                    primaryResponse.getId(),
                    primaryResponse.getSeqNo(),
                    primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(),
                    indexRequest.getAutoGeneratedTimestamp(),
                    indexRequest.isRetry(),
                    sourceToParse
                );
                break;
            case DELETE:
                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                result = replica.applyDeleteOperationOnReplica(
                    primaryResponse.getSeqNo(),
                    primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(),
                    deleteRequest.id()
                );
                break;
            default:
                assert false : "Unexpected request operation type on replica: " + docWriteRequest + ";primary result: " + primaryResponse;
                throw new IllegalStateException("Unexpected request operation type on replica: " + docWriteRequest.opType().getLowercase());
        }
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            // Even though the primary waits on all nodes to ack the mapping changes to the cluster-manager
            // (see MappingUpdatedAction.updateMappingOnClusterManager) we still need to protect against missing mappings
            // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
            // mapping update. Assume that that update is first applied on the primary, and only later on the replica
            // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
            // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
            // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
            // applied the new mapping, so there is no other option than to wait.
            throw new TransportReplicationAction.RetryOnReplicaException(
                replica.shardId(),
                "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate()
            );
        }
        return result;
    }
}
