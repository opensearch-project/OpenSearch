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

package org.opensearch.action.support.replication;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.TransportAction;
import org.opensearch.action.support.TransportActions;
import org.opensearch.action.support.replication.ReplicationOperation.Replicas;
import org.opensearch.client.transport.NoNodeAvailableException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.Assertions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.ReplicationGroup;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.shard.ShardNotInPrimaryModeException;
import org.opensearch.indices.IndexClosedException;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.NodeClosedException;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for requests that should be executed on a primary copy followed by replica copies.
 * Subclasses can resolve the target shard and provide implementation for primary and replica operations.
 * <p>
 * The action samples cluster state on the receiving node to reroute to node with primary copy and on the
 * primary node to validate request before primary operation followed by sampling state again for resolving
 * nodes with replica copies to perform replication.
 *
 * @opensearch.internal
 */
public abstract class TransportReplicationAction<
    Request extends ReplicationRequest<Request>,
    ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
    Response extends ReplicationResponse> extends TransportAction<Request, Response> {

    /**
     * The timeout for retrying replication requests.
     */
    public static final Setting<TimeValue> REPLICATION_RETRY_TIMEOUT = Setting.timeSetting(
        "indices.replication.retry_timeout",
        TimeValue.timeValueSeconds(60),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The maximum bound for the first retry backoff for failed replication operations. The backoff bound
     * will increase exponential if failures continue.
     */
    public static final Setting<TimeValue> REPLICATION_INITIAL_RETRY_BACKOFF_BOUND = Setting.timeSetting(
        "indices.replication.initial_retry_backoff_bound",
        TimeValue.timeValueMillis(50),
        TimeValue.timeValueMillis(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Making primary and replica actions suffixes as constant
     */
    public static final String PRIMARY_ACTION_SUFFIX = "[p]";
    public static final String REPLICA_ACTION_SUFFIX = "[r]";

    protected final ThreadPool threadPool;
    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final ShardStateAction shardStateAction;
    protected final IndicesService indicesService;
    protected final TransportRequestOptions transportOptions;
    protected final String executor;
    protected final boolean forceExecutionOnPrimary;

    // package private for testing
    protected final String transportReplicaAction;
    protected final String transportPrimaryAction;

    private final boolean syncGlobalCheckpointAfterOperation;
    private volatile TimeValue initialRetryBackoffBound;
    private volatile TimeValue retryTimeout;

    protected TransportReplicationAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Writeable.Reader<ReplicaRequest> replicaRequestReader,
        String executor
    ) {
        this(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            requestReader,
            replicaRequestReader,
            executor,
            false,
            false
        );
    }

    protected TransportReplicationAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Writeable.Reader<ReplicaRequest> replicaRequestReader,
        String executor,
        boolean syncGlobalCheckpointAfterOperation,
        boolean forceExecutionOnPrimary
    ) {
        this(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            requestReader,
            replicaRequestReader,
            executor,
            syncGlobalCheckpointAfterOperation,
            forceExecutionOnPrimary,
            null
        );
    }

    protected TransportReplicationAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Writeable.Reader<ReplicaRequest> replicaRequestReader,
        String executor,
        boolean syncGlobalCheckpointAfterOperation,
        boolean forceExecutionOnPrimary,
        AdmissionControlActionType admissionControlActionType
    ) {
        super(actionName, actionFilters, transportService.getTaskManager());
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;
        this.executor = executor;

        this.transportPrimaryAction = actionName + PRIMARY_ACTION_SUFFIX;
        this.transportReplicaAction = actionName + REPLICA_ACTION_SUFFIX;

        this.initialRetryBackoffBound = REPLICATION_INITIAL_RETRY_BACKOFF_BOUND.get(settings);
        this.retryTimeout = REPLICATION_RETRY_TIMEOUT.get(settings);
        this.forceExecutionOnPrimary = forceExecutionOnPrimary;

        transportService.registerRequestHandler(actionName, ThreadPool.Names.SAME, requestReader, this::handleOperationRequest);

        // This method will register Primary Request Handler Based on AdmissionControlActionType
        registerPrimaryRequestHandler(requestReader, admissionControlActionType);

        // we must never reject on because of thread pool capacity on replicas
        transportService.registerRequestHandler(
            transportReplicaAction,
            executor,
            true,
            true,
            in -> new ConcreteReplicaRequest<>(replicaRequestReader, in),
            this::handleReplicaRequest
        );

        this.transportOptions = transportOptions(settings);

        this.syncGlobalCheckpointAfterOperation = syncGlobalCheckpointAfterOperation;

        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(REPLICATION_INITIAL_RETRY_BACKOFF_BOUND, (v) -> initialRetryBackoffBound = v);
        clusterSettings.addSettingsUpdateConsumer(REPLICATION_RETRY_TIMEOUT, (v) -> retryTimeout = v);
    }

    /**
     *  This method will register handler as based on admissionControlActionType and AdmissionControlHandler will be
     *  invoked for registered action
     * @param requestReader instance of the request reader
     * @param admissionControlActionType type of AdmissionControlActionType
     */
    private void registerPrimaryRequestHandler(
        Writeable.Reader<Request> requestReader,
        AdmissionControlActionType admissionControlActionType
    ) {
        if (admissionControlActionType != null) {
            transportService.registerRequestHandler(
                transportPrimaryAction,
                executor,
                forceExecutionOnPrimary,
                true,
                admissionControlActionType,
                in -> new ConcreteShardRequest<>(requestReader, in),
                this::handlePrimaryRequest
            );
        } else {
            transportService.registerRequestHandler(
                transportPrimaryAction,
                executor,
                forceExecutionOnPrimary,
                true,
                in -> new ConcreteShardRequest<>(requestReader, in),
                this::handlePrimaryRequest
            );
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        assert request.shardId() != null : "request shardId must be set";
        runReroutePhase(task, request, listener, true);
    }

    private void runReroutePhase(Task task, Request request, ActionListener<Response> listener, boolean initiatedByNodeClient) {
        try {
            new ReroutePhase((ReplicationTask) task, request, listener, initiatedByNodeClient).run();
        } catch (RuntimeException e) {
            listener.onFailure(e);
        }
    }

    protected Replicas<ReplicaRequest> newReplicasProxy() {
        return new ReplicasProxy();
    }

    /**
     * This returns a ReplicaProxy that is used for primary term validation. The default behavior is that the control
     * must not reach inside the performOn method for ReplicationActions. However, the implementations of the underlying
     * class can provide primary term validation proxy that can allow performOn method to make calls to replica.
     *
     * @return Primary term validation replicas proxy.
     */
    protected Replicas<ReplicaRequest> primaryTermValidationReplicasProxy() {
        return new ReplicasProxy() {
            @Override
            public void performOn(
                ShardRouting replica,
                ReplicaRequest request,
                long primaryTerm,
                long globalCheckpoint,
                long maxSeqNoOfUpdatesOrDeletes,
                ActionListener<ReplicationOperation.ReplicaResponse> listener
            ) {
                throw new UnsupportedOperationException("Primary term validation is not available for " + actionName);
            }
        };
    }

    /**
     * This method is used for defining the {@link ReplicationMode} override per {@link TransportReplicationAction}.
     *
     * @param indexShard index shard used to determining the policy.
     * @return the overridden replication mode.
     */
    public ReplicationMode getReplicationMode(IndexShard indexShard) {
        if (indexShard.indexSettings().isAssignedOnRemoteNode()) {
            return ReplicationMode.NO_REPLICATION;
        }
        return ReplicationMode.FULL_REPLICATION;
    }

    protected abstract Response newResponseInstance(StreamInput in) throws IOException;

    /**
     * Resolves derived values in the request. For example, the target shard id of the incoming request, if not set at request construction.
     * Additional processing or validation of the request should be done here.
     *
     * @param indexMetadata index metadata of the concrete index this request is going to operate on
     * @param request       the request to resolve
     */
    protected void resolveRequest(final IndexMetadata indexMetadata, final Request request) {
        if (request.waitForActiveShards() == ActiveShardCount.DEFAULT) {
            // if the wait for active shard count has not been set in the request,
            // resolve it from the index settings
            request.waitForActiveShards(indexMetadata.getWaitForActiveShards());
        }
    }

    /**
     * Primary operation on node with primary copy.
     *
     * @param shardRequest the request to the primary shard
     * @param primary      the primary shard to perform the operation on
     */
    protected abstract void shardOperationOnPrimary(
        Request shardRequest,
        IndexShard primary,
        ActionListener<PrimaryResult<ReplicaRequest, Response>> listener
    );

    /**
     * Execute the specified replica operation. This is done under a permit from
     * {@link IndexShard#acquireReplicaOperationPermit(long, long, long, ActionListener, String, Object)}.
     *
     * @param shardRequest the request to the replica shard
     * @param replica      the replica shard to perform the operation on
     */
    protected abstract void shardOperationOnReplica(
        ReplicaRequest shardRequest,
        IndexShard replica,
        ActionListener<ReplicaResult> listener
    );

    /**
     * Cluster level block to check before request execution. Returning null means that no blocks need to be checked.
     */
    @Nullable
    protected ClusterBlockLevel globalBlockLevel() {
        return null;
    }

    /**
     * Index level block to check before request execution. Returning null means that no blocks need to be checked.
     */
    @Nullable
    public ClusterBlockLevel indexBlockLevel() {
        return null;
    }

    protected TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.EMPTY;
    }

    private ClusterBlockException blockExceptions(final ClusterState state, final String indexName) {
        ClusterBlockLevel globalBlockLevel = globalBlockLevel();
        if (globalBlockLevel != null) {
            ClusterBlockException blockException = state.blocks().globalBlockedException(globalBlockLevel);
            if (blockException != null) {
                return blockException;
            }
        }
        ClusterBlockLevel indexBlockLevel = indexBlockLevel();
        if (indexBlockLevel != null) {
            ClusterBlockException blockException = state.blocks().indexBlockedException(indexBlockLevel, indexName);
            if (blockException != null) {
                return blockException;
            }
        }
        return null;
    }

    protected boolean retryPrimaryException(final Throwable e) {
        return e.getClass() == ReplicationOperation.RetryOnPrimaryException.class
            || TransportActions.isShardNotAvailableException(e)
            || isRetryableClusterBlockException(e);
    }

    boolean isRetryableClusterBlockException(final Throwable e) {
        if (e instanceof ClusterBlockException) {
            return ((ClusterBlockException) e).retryable();
        }
        return false;
    }

    private void handleOperationRequest(final Request request, final TransportChannel channel, Task task) {
        Releasable releasable = checkOperationLimits(request);
        ActionListener<Response> listener = ActionListener.runBefore(
            new ChannelActionListener<>(channel, actionName, request),
            releasable::close
        );
        runReroutePhase(task, request, listener, false);
    }

    protected Releasable checkOperationLimits(final Request request) {
        return () -> {};
    }

    protected void handlePrimaryRequest(final ConcreteShardRequest<Request> request, final TransportChannel channel, final Task task) {
        Releasable releasable = checkPrimaryLimits(
            request.getRequest(),
            request.sentFromLocalReroute(),
            request.localRerouteInitiatedByNodeClient()
        );
        ActionListener<Response> listener = ActionListener.runBefore(
            new ChannelActionListener<>(channel, transportPrimaryAction, request),
            releasable::close
        );

        try {
            new AsyncPrimaryAction(request, listener, (ReplicationTask) task).run();
        } catch (RuntimeException e) {
            listener.onFailure(e);
        }
    }

    protected Releasable checkPrimaryLimits(final Request request, boolean rerouteWasLocal, boolean localRerouteInitiatedByNodeClient) {
        return () -> {};
    }

    /**
     * Asynchronous primary action
     *
     * @opensearch.internal
     */
    class AsyncPrimaryAction extends AbstractRunnable {
        private final ActionListener<Response> onCompletionListener;
        private final ReplicationTask replicationTask;
        private final ConcreteShardRequest<Request> primaryRequest;

        AsyncPrimaryAction(
            ConcreteShardRequest<Request> primaryRequest,
            ActionListener<Response> onCompletionListener,
            ReplicationTask replicationTask
        ) {
            this.primaryRequest = primaryRequest;
            this.onCompletionListener = onCompletionListener;
            this.replicationTask = replicationTask;
        }

        @Override
        protected void doRun() throws Exception {
            final ShardId shardId = primaryRequest.getRequest().shardId();
            final IndexShard indexShard = getIndexShard(shardId);
            final ShardRouting shardRouting = indexShard.routingEntry();
            // we may end up here if the cluster state used to route the primary is so stale that the underlying
            // index shard was replaced with a replica. For example - in a two node cluster, if the primary fails
            // the replica will take over and a replica will be assigned to the first node.
            if (shardRouting.primary() == false) {
                throw new ReplicationOperation.RetryOnPrimaryException(shardId, "actual shard is not a primary " + shardRouting);
            }
            final String actualAllocationId = shardRouting.allocationId().getId();
            if (actualAllocationId.equals(primaryRequest.getTargetAllocationID()) == false) {
                throw new ShardNotFoundException(
                    shardId,
                    "expected allocation id [{}] but found [{}]",
                    primaryRequest.getTargetAllocationID(),
                    actualAllocationId
                );
            }
            final long actualTerm = indexShard.getPendingPrimaryTerm();
            if (actualTerm != primaryRequest.getPrimaryTerm()) {
                throw new ShardNotFoundException(
                    shardId,
                    "expected allocation id [{}] with term [{}] but found [{}]",
                    primaryRequest.getTargetAllocationID(),
                    primaryRequest.getPrimaryTerm(),
                    actualTerm
                );
            }

            acquirePrimaryOperationPermit(
                indexShard,
                primaryRequest.getRequest(),
                ActionListener.wrap(releasable -> runWithPrimaryShardReference(new PrimaryShardReference(indexShard, releasable)), e -> {
                    if (e instanceof ShardNotInPrimaryModeException) {
                        onFailure(new ReplicationOperation.RetryOnPrimaryException(shardId, "shard is not in primary mode", e));
                    } else {
                        onFailure(e);
                    }
                })
            );
        }

        void runWithPrimaryShardReference(final PrimaryShardReference primaryShardReference) {
            try {
                final ClusterState clusterState = clusterService.state();
                final IndexMetadata indexMetadata = clusterState.metadata().getIndexSafe(primaryShardReference.routingEntry().index());

                final ClusterBlockException blockException = blockExceptions(clusterState, indexMetadata.getIndex().getName());
                if (blockException != null) {
                    logger.trace("cluster is blocked, action failed on primary", blockException);
                    throw blockException;
                }

                if (primaryShardReference.isRelocated()) {
                    primaryShardReference.close(); // release shard operation lock as soon as possible
                    setPhase(replicationTask, "primary_delegation");
                    // delegate primary phase to relocation target
                    // it is safe to execute primary phase on relocation target as there are no more in-flight operations where primary
                    // phase is executed on local shard and all subsequent operations are executed on relocation target as primary phase.
                    final ShardRouting primary = primaryShardReference.routingEntry();
                    assert primary.relocating() : "indexShard is marked as relocated but routing isn't" + primary;
                    final Writeable.Reader<Response> reader = TransportReplicationAction.this::newResponseInstance;
                    DiscoveryNode relocatingNode = clusterState.nodes().get(primary.relocatingNodeId());
                    transportService.sendRequest(
                        relocatingNode,
                        transportPrimaryAction,
                        new ConcreteShardRequest<>(
                            primaryRequest.getRequest(),
                            primary.allocationId().getRelocationId(),
                            primaryRequest.getPrimaryTerm()
                        ),
                        transportOptions,
                        new ActionListenerResponseHandler<Response>(onCompletionListener, reader) {
                            @Override
                            public void handleResponse(Response response) {
                                setPhase(replicationTask, "finished");
                                super.handleResponse(response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                setPhase(replicationTask, "finished");
                                super.handleException(exp);
                            }
                        }
                    );
                } else {
                    setPhase(replicationTask, "primary");

                    final ActionListener<Response> responseListener = ActionListener.wrap(response -> {
                        adaptResponse(response, primaryShardReference.indexShard);

                        if (syncGlobalCheckpointAfterOperation) {
                            try {
                                primaryShardReference.indexShard.maybeSyncGlobalCheckpoint("post-operation");
                            } catch (final Exception e) {
                                // only log non-closed exceptions
                                if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                                    // intentionally swallow, a missed global checkpoint sync should not fail this operation
                                    logger.info(
                                        new ParameterizedMessage(
                                            "{} failed to execute post-operation global checkpoint sync",
                                            primaryShardReference.indexShard.shardId()
                                        ),
                                        e
                                    );
                                }
                            }
                        }

                        primaryShardReference.close(); // release shard operation lock before responding to caller
                        setPhase(replicationTask, "finished");
                        onCompletionListener.onResponse(response);
                    }, e -> handleException(primaryShardReference, e));

                    final Replicas<ReplicaRequest> replicasProxy = newReplicasProxy();
                    final IndexShard indexShard = primaryShardReference.indexShard;
                    final Replicas<ReplicaRequest> termValidationProxy = primaryTermValidationReplicasProxy();

                    new ReplicationOperation<>(
                        primaryRequest.getRequest(),
                        primaryShardReference,
                        ActionListener.map(responseListener, result -> result.finalResponseIfSuccessful),
                        replicasProxy,
                        logger,
                        threadPool,
                        actionName,
                        primaryRequest.getPrimaryTerm(),
                        initialRetryBackoffBound,
                        retryTimeout,
                        indexShard.indexSettings().isAssignedOnRemoteNode()
                            ? new ReplicationModeAwareProxy<>(
                                getReplicationMode(indexShard),
                                clusterState.getNodes(),
                                replicasProxy,
                                termValidationProxy,
                                indexShard.isRemoteTranslogEnabled()
                            )
                            : new FanoutReplicationProxy<>(replicasProxy)
                    ).execute();
                }
            } catch (Exception e) {
                handleException(primaryShardReference, e);
            }
        }

        private void handleException(PrimaryShardReference primaryShardReference, Exception e) {
            Releasables.closeWhileHandlingException(primaryShardReference); // release shard operation lock before responding to caller
            onFailure(e);
        }

        @Override
        public void onFailure(Exception e) {
            setPhase(replicationTask, "finished");
            onCompletionListener.onFailure(e);
        }

    }

    // allows subclasses to adapt the response
    protected void adaptResponse(Response response, IndexShard indexShard) {

    }

    /**
     * The Primary Result
     *
     * @opensearch.internal
     */
    public static class PrimaryResult<ReplicaRequest extends ReplicationRequest<ReplicaRequest>, Response extends ReplicationResponse>
        implements
            ReplicationOperation.PrimaryResult<ReplicaRequest> {
        protected final ReplicaRequest replicaRequest;
        public final Response finalResponseIfSuccessful;
        public final Exception finalFailure;

        /**
         * Result of executing a primary operation
         * expects <code>finalResponseIfSuccessful</code> or <code>finalFailure</code> to be not-null
         */
        public PrimaryResult(ReplicaRequest replicaRequest, Response finalResponseIfSuccessful, Exception finalFailure) {
            assert finalFailure != null ^ finalResponseIfSuccessful != null : "either a response or a failure has to be not null, "
                + "found ["
                + finalFailure
                + "] failure and ["
                + finalResponseIfSuccessful
                + "] response";
            this.replicaRequest = replicaRequest;
            this.finalResponseIfSuccessful = finalResponseIfSuccessful;
            this.finalFailure = finalFailure;
        }

        public PrimaryResult(ReplicaRequest replicaRequest, Response replicationResponse) {
            this(replicaRequest, replicationResponse, null);
        }

        @Override
        public ReplicaRequest replicaRequest() {
            return replicaRequest;
        }

        @Override
        public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
            if (finalResponseIfSuccessful != null) {
                finalResponseIfSuccessful.setShardInfo(shardInfo);
            }
        }

        @Override
        public void runPostReplicationActions(ActionListener<Void> listener) {
            if (finalFailure != null) {
                listener.onFailure(finalFailure);
            } else {
                listener.onResponse(null);
            }
        }
    }

    /**
     * The replica result
     *
     * @opensearch.internal
     */
    public static class ReplicaResult {
        final Exception finalFailure;

        public ReplicaResult(Exception finalFailure) {
            this.finalFailure = finalFailure;
        }

        public ReplicaResult() {
            this(null);
        }

        public void runPostReplicaActions(ActionListener<Void> listener) {
            if (finalFailure != null) {
                listener.onFailure(finalFailure);
            } else {
                listener.onResponse(null);
            }
        }
    }

    protected void handleReplicaRequest(
        final ConcreteReplicaRequest<ReplicaRequest> replicaRequest,
        final TransportChannel channel,
        final Task task
    ) {
        Releasable releasable = checkReplicaLimits(replicaRequest.getRequest());
        ActionListener<ReplicaResponse> listener = ActionListener.runBefore(
            new ChannelActionListener<>(channel, transportReplicaAction, replicaRequest),
            releasable::close
        );

        try {
            new AsyncReplicaAction(replicaRequest, listener, (ReplicationTask) task).run();
        } catch (RuntimeException e) {
            listener.onFailure(e);
        }
    }

    protected Releasable checkReplicaLimits(final ReplicaRequest request) {
        return () -> {};
    }

    /**
     * Thrown if there are any errors retrying on the replica
     *
     * @opensearch.internal
     */
    public static class RetryOnReplicaException extends OpenSearchException {

        public RetryOnReplicaException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public RetryOnReplicaException(StreamInput in) throws IOException {
            super(in);
        }
    }

    /**
     * Asynchronous replica action
     *
     * @opensearch.internal
     */
    private final class AsyncReplicaAction extends AbstractRunnable implements ActionListener<Releasable> {
        private final ActionListener<ReplicaResponse> onCompletionListener;
        private final IndexShard replica;
        /**
         * The task on the node with the replica shard.
         */
        private final ReplicationTask task;
        // important: we pass null as a timeout as failing a replica is
        // something we want to avoid at all costs
        private final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
        private final ConcreteReplicaRequest<ReplicaRequest> replicaRequest;

        AsyncReplicaAction(
            ConcreteReplicaRequest<ReplicaRequest> replicaRequest,
            ActionListener<ReplicaResponse> onCompletionListener,
            ReplicationTask task
        ) {
            this.replicaRequest = replicaRequest;
            this.onCompletionListener = onCompletionListener;
            this.task = task;
            final ShardId shardId = replicaRequest.getRequest().shardId();
            assert shardId != null : "request shardId must be set";
            this.replica = getIndexShard(shardId);
        }

        @Override
        public void onResponse(Releasable releasable) {
            assert replica.getActiveOperationsCount() != 0 : "must perform shard operation under a permit";
            try {
                shardOperationOnReplica(
                    replicaRequest.getRequest(),
                    replica,
                    ActionListener.wrap((replicaResult) -> replicaResult.runPostReplicaActions(ActionListener.wrap(r -> {
                        final ReplicaResponse response = new ReplicaResponse(
                            replica.getLocalCheckpoint(),
                            replica.getLastSyncedGlobalCheckpoint()
                        );
                        releasable.close(); // release shard operation lock before responding to caller
                        if (logger.isTraceEnabled()) {
                            logger.trace(
                                "action [{}] completed on shard [{}] for request [{}]",
                                transportReplicaAction,
                                replicaRequest.getRequest().shardId(),
                                replicaRequest.getRequest()
                            );
                        }
                        setPhase(task, "finished");
                        onCompletionListener.onResponse(response);
                    }, e -> {
                        Releasables.closeWhileHandlingException(releasable); // release shard operation lock before responding to caller
                        responseWithFailure(e);
                    })), e -> {
                        Releasables.closeWhileHandlingException(releasable); // release shard operation lock before responding to caller
                        AsyncReplicaAction.this.onFailure(e);
                    })
                );
                // TODO: Evaluate if we still need to catch this exception
            } catch (Exception e) {
                Releasables.closeWhileHandlingException(releasable); // release shard operation lock before responding to caller
                AsyncReplicaAction.this.onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof RetryOnReplicaException) {
                logger.trace(
                    () -> new ParameterizedMessage(
                        "Retrying operation on replica, action [{}], request [{}]",
                        transportReplicaAction,
                        replicaRequest.getRequest()
                    ),
                    e
                );
                replicaRequest.getRequest().onRetry();
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        // Forking a thread on local node via transport service so that custom transport service have an
                        // opportunity to execute custom logic before the replica operation begins
                        transportService.sendRequest(
                            clusterService.localNode(),
                            transportReplicaAction,
                            replicaRequest,
                            new ActionListenerResponseHandler<>(onCompletionListener, ReplicaResponse::new)
                        );
                    }

                    @Override
                    public void onClusterServiceClose() {
                        responseWithFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        throw new AssertionError("Cannot happen: there is not timeout");
                    }
                });
            } else {
                responseWithFailure(e);
            }
        }

        protected void responseWithFailure(Exception e) {
            setPhase(task, "finished");
            onCompletionListener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            setPhase(task, "replica");
            final String actualAllocationId = this.replica.routingEntry().allocationId().getId();
            if (actualAllocationId.equals(replicaRequest.getTargetAllocationID()) == false) {
                throw new ShardNotFoundException(
                    this.replica.shardId(),
                    "expected allocation id [{}] but found [{}]",
                    replicaRequest.getTargetAllocationID(),
                    actualAllocationId
                );
            }
            acquireReplicaOperationPermit(
                replica,
                replicaRequest.getRequest(),
                this,
                replicaRequest.getPrimaryTerm(),
                replicaRequest.getGlobalCheckpoint(),
                replicaRequest.getMaxSeqNoOfUpdatesOrDeletes()
            );
        }
    }

    protected IndexShard getIndexShard(final ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getShard(shardId.id());
    }

    /**
     * Responsible for routing and retrying failed operations on the primary.
     * The actual primary operation is done in {@link ReplicationOperation} on the
     * node with primary copy.
     * <p>
     * Resolves index and shard id for the request before routing it to target node
     *
     * @opensearch.internal
     */
    final class ReroutePhase extends AbstractRunnable {
        private final ActionListener<Response> listener;
        private final Request request;
        private final boolean initiatedByNodeClient;
        private final ReplicationTask task;
        private final ClusterStateObserver observer;
        private final AtomicBoolean finished = new AtomicBoolean();

        ReroutePhase(ReplicationTask task, Request request, ActionListener<Response> listener) {
            this(task, request, listener, false);
        }

        ReroutePhase(ReplicationTask task, Request request, ActionListener<Response> listener, boolean initiatedByNodeClient) {
            this.request = request;
            this.initiatedByNodeClient = initiatedByNodeClient;
            if (task != null) {
                this.request.setParentTask(clusterService.localNode().getId(), task.getId());
            }
            this.listener = listener;
            this.task = task;
            this.observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        }

        @Override
        public void onFailure(Exception e) {
            finishWithUnexpectedFailure(e);
        }

        @Override
        protected void doRun() {
            setPhase(task, "routing");
            final ClusterState state = observer.setAndGetObservedState();
            final ClusterBlockException blockException = blockExceptions(state, request.shardId().getIndexName());
            if (blockException != null) {
                if (blockException.retryable()) {
                    logger.trace("cluster is blocked, scheduling a retry", blockException);
                    retry(blockException);
                } else {
                    finishAsFailed(blockException);
                }
            } else {
                final IndexMetadata indexMetadata = state.metadata().index(request.shardId().getIndex());
                if (indexMetadata == null) {
                    // ensure that the cluster state on the node is at least as high as the node that decided that the index was there
                    if (state.version() < request.routedBasedOnClusterVersion()) {
                        logger.trace(
                            "failed to find index [{}] for request [{}] despite sender thinking it would be here. "
                                + "Local cluster state version [{}]] is older than on sending node (version [{}]), scheduling a retry...",
                            request.shardId().getIndex(),
                            request,
                            state.version(),
                            request.routedBasedOnClusterVersion()
                        );
                        retry(
                            new IndexNotFoundException(
                                "failed to find index as current cluster state with version ["
                                    + state.version()
                                    + "] is stale (expected at least ["
                                    + request.routedBasedOnClusterVersion()
                                    + "]",
                                request.shardId().getIndexName()
                            )
                        );
                        return;
                    } else {
                        finishAsFailed(new IndexNotFoundException(request.shardId().getIndex()));
                        return;
                    }
                }

                if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    finishAsFailed(new IndexClosedException(indexMetadata.getIndex()));
                    return;
                }

                if (request.waitForActiveShards() == ActiveShardCount.DEFAULT) {
                    // if the wait for active shard count has not been set in the request,
                    // resolve it from the index settings
                    request.waitForActiveShards(indexMetadata.getWaitForActiveShards());
                }
                assert request.waitForActiveShards() != ActiveShardCount.DEFAULT
                    : "request waitForActiveShards must be set in resolveRequest";

                final ShardRouting primary = state.getRoutingTable().shardRoutingTable(request.shardId()).primaryShard();
                if (primary == null || primary.active() == false) {
                    logger.trace(
                        "primary shard [{}] is not yet active, scheduling a retry: action [{}], request [{}], "
                            + "cluster state version [{}]",
                        request.shardId(),
                        actionName,
                        request,
                        state.version()
                    );
                    retryBecauseUnavailable(request.shardId(), "primary shard is not active");
                    return;
                }
                if (state.nodes().nodeExists(primary.currentNodeId()) == false) {
                    logger.trace(
                        "primary shard [{}] is assigned to an unknown node [{}], scheduling a retry: action [{}], request [{}], "
                            + "cluster state version [{}]",
                        request.shardId(),
                        primary.currentNodeId(),
                        actionName,
                        request,
                        state.version()
                    );
                    retryBecauseUnavailable(request.shardId(), "primary shard isn't assigned to a known node.");
                    return;
                }
                final DiscoveryNode node = state.nodes().get(primary.currentNodeId());
                if (primary.currentNodeId().equals(state.nodes().getLocalNodeId())) {
                    performLocalAction(state, primary, node, indexMetadata);
                } else {
                    performRemoteAction(state, primary, node);
                }
            }
        }

        private void performLocalAction(ClusterState state, ShardRouting primary, DiscoveryNode node, IndexMetadata indexMetadata) {
            setPhase(task, "waiting_on_primary");
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "send action [{}] to local primary [{}] for request [{}] with cluster state version [{}] to [{}] ",
                    transportPrimaryAction,
                    request.shardId(),
                    request,
                    state.version(),
                    primary.currentNodeId()
                );
            }
            performAction(
                node,
                transportPrimaryAction,
                true,
                new ConcreteShardRequest<>(
                    request,
                    primary.allocationId().getId(),
                    indexMetadata.primaryTerm(primary.id()),
                    true,
                    initiatedByNodeClient
                )
            );
        }

        private void performRemoteAction(ClusterState state, ShardRouting primary, DiscoveryNode node) {
            if (state.version() < request.routedBasedOnClusterVersion()) {
                logger.trace(
                    "failed to find primary [{}] for request [{}] despite sender thinking it would be here. Local cluster state "
                        + "version [{}]] is older than on sending node (version [{}]), scheduling a retry...",
                    request.shardId(),
                    request,
                    state.version(),
                    request.routedBasedOnClusterVersion()
                );
                retryBecauseUnavailable(
                    request.shardId(),
                    "failed to find primary as current cluster state with version ["
                        + state.version()
                        + "] is stale (expected at least ["
                        + request.routedBasedOnClusterVersion()
                        + "]"
                );
                return;
            } else {
                // chasing the node with the active primary for a second hop requires that we are at least up-to-date with the current
                // cluster state version this prevents redirect loops between two nodes when a primary was relocated and the relocation
                // target is not aware that it is the active primary shard already.
                request.routedBasedOnClusterVersion(state.version());
            }
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}]",
                    actionName,
                    request.shardId(),
                    request,
                    state.version(),
                    primary.currentNodeId()
                );
            }
            setPhase(task, "rerouted");
            performAction(node, actionName, false, request);
        }

        private void performAction(
            final DiscoveryNode node,
            final String action,
            final boolean isPrimaryAction,
            final TransportRequest requestToPerform
        ) {
            transportService.sendRequest(node, action, requestToPerform, transportOptions, new TransportResponseHandler<Response>() {

                @Override
                public Response read(StreamInput in) throws IOException {
                    return newResponseInstance(in);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(Response response) {
                    finishOnSuccess(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    try {
                        // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                        final Throwable cause = exp.unwrapCause();
                        if (cause instanceof ConnectTransportException
                            || cause instanceof NodeClosedException
                            || (isPrimaryAction && retryPrimaryException(cause))) {
                            logger.trace(
                                () -> new ParameterizedMessage(
                                    "received an error from node [{}] for request [{}], scheduling a retry",
                                    node.getId(),
                                    requestToPerform
                                ),
                                exp
                            );
                            retry(exp);
                        } else {
                            finishAsFailed(exp);
                        }
                    } catch (Exception e) {
                        e.addSuppressed(exp);
                        finishWithUnexpectedFailure(e);
                    }
                }
            });
        }

        void retry(Exception failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                finishAsFailed(failure);
                return;
            }
            setPhase(task, "waiting_for_retry");
            request.onRetry();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    finishAsFailed(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // Try one more time...
                    run();
                }
            });
        }

        void finishAsFailed(Exception failure) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                logger.trace(() -> new ParameterizedMessage("operation failed. action [{}], request [{}]", actionName, request), failure);
                listener.onFailure(failure);
            } else {
                assert false : new AssertionError("finishAsFailed called but operation is already finished", failure);
            }
        }

        void finishWithUnexpectedFailure(Exception failure) {
            logger.warn(
                () -> new ParameterizedMessage(
                    "unexpected error during the primary phase for action [{}], request [{}]",
                    actionName,
                    request
                ),
                failure
            );
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                listener.onFailure(failure);
            } else {
                assert false : new AssertionError("finishWithUnexpectedFailure called but operation is already finished", failure);
            }
        }

        void finishOnSuccess(Response response) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "finished");
                if (logger.isTraceEnabled()) {
                    logger.trace("operation succeeded. action [{}],request [{}]", actionName, request);
                }
                listener.onResponse(response);
            } else {
                assert false : "finishOnSuccess called but operation is already finished";
            }
        }

        void retryBecauseUnavailable(ShardId shardId, String message) {
            retry(new UnavailableShardsException(shardId, "{} Timeout: [{}], request: [{}]", message, request.timeout(), request));
        }
    }

    /**
     * Executes the logic for acquiring one or more operation permit on a primary shard. The default is to acquire a single permit but this
     * method can be overridden to acquire more.
     */
    protected void acquirePrimaryOperationPermit(
        final IndexShard primary,
        final Request request,
        final ActionListener<Releasable> onAcquired
    ) {
        primary.acquirePrimaryOperationPermit(onAcquired, executor, request, forceExecutionOnPrimary);
    }

    /**
     * Executes the logic for acquiring one or more operation permit on a replica shard. The default is to acquire a single permit but this
     * method can be overridden to acquire more.
     */
    protected void acquireReplicaOperationPermit(
        final IndexShard replica,
        final ReplicaRequest request,
        final ActionListener<Releasable> onAcquired,
        final long primaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes
    ) {
        replica.acquireReplicaOperationPermit(primaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, onAcquired, executor, request);
    }

    /**
     * The primary shard reference
     *
     * @opensearch.internal
     */
    class PrimaryShardReference
        implements
            Releasable,
            ReplicationOperation.Primary<Request, ReplicaRequest, PrimaryResult<ReplicaRequest, Response>> {

        protected final IndexShard indexShard;
        private final Releasable operationLock;

        PrimaryShardReference(IndexShard indexShard, Releasable operationLock) {
            this.indexShard = indexShard;
            this.operationLock = operationLock;
        }

        @Override
        public void close() {
            operationLock.close();
        }

        public ShardRouting routingEntry() {
            return indexShard.routingEntry();
        }

        public boolean isRelocated() {
            return indexShard.isRelocatedPrimary();
        }

        @Override
        public void failShard(String reason, Exception e) {
            try {
                indexShard.failShard(reason, e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
        }

        @Override
        public void perform(Request request, ActionListener<PrimaryResult<ReplicaRequest, Response>> listener) {
            if (Assertions.ENABLED) {
                listener = ActionListener.map(listener, result -> {
                    assert result.replicaRequest() == null || result.finalFailure == null : "a replica request ["
                        + result.replicaRequest()
                        + "] with a primary failure ["
                        + result.finalFailure
                        + "]";
                    return result;
                });
            }
            assert indexShard.getActiveOperationsCount() != 0 : "must perform shard operation under a permit";
            shardOperationOnPrimary(request, indexShard, listener);
        }

        @Override
        public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
            indexShard.updateLocalCheckpointForShard(allocationId, checkpoint);
        }

        @Override
        public void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
            indexShard.updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
        }

        @Override
        public long localCheckpoint() {
            return indexShard.getLocalCheckpoint();
        }

        @Override
        public long globalCheckpoint() {
            return indexShard.getLastSyncedGlobalCheckpoint();
        }

        @Override
        public long computedGlobalCheckpoint() {
            return indexShard.getLastKnownGlobalCheckpoint();
        }

        @Override
        public long maxSeqNoOfUpdatesOrDeletes() {
            return indexShard.getMaxSeqNoOfUpdatesOrDeletes();
        }

        @Override
        public ReplicationGroup getReplicationGroup() {
            return indexShard.getReplicationGroup();
        }

        @Override
        public PendingReplicationActions getPendingReplicationActions() {
            return indexShard.getPendingReplicationActions();
        }
    }

    /**
     * The replica response
     *
     * @opensearch.internal
     */
    public static class ReplicaResponse extends ActionResponse implements ReplicationOperation.ReplicaResponse {
        private long localCheckpoint;
        private long globalCheckpoint;

        public ReplicaResponse(StreamInput in) throws IOException {
            super(in);
            localCheckpoint = in.readZLong();
            globalCheckpoint = in.readZLong();
        }

        public ReplicaResponse(long localCheckpoint, long globalCheckpoint) {
            /*
             * A replica should always know its own local checkpoints so this should always be a valid sequence number or the pre-6.0
             * checkpoint value when simulating responses to replication actions that pre-6.0 nodes are not aware of (e.g., the global
             * checkpoint background sync, and the primary/replica resync).
             */
            assert localCheckpoint != SequenceNumbers.UNASSIGNED_SEQ_NO;
            this.localCheckpoint = localCheckpoint;
            this.globalCheckpoint = globalCheckpoint;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(localCheckpoint);
            out.writeZLong(globalCheckpoint);
        }

        @Override
        public long localCheckpoint() {
            return localCheckpoint;
        }

        @Override
        public long globalCheckpoint() {
            return globalCheckpoint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReplicaResponse that = (ReplicaResponse) o;
            return localCheckpoint == that.localCheckpoint && globalCheckpoint == that.globalCheckpoint;
        }

        @Override
        public int hashCode() {
            return Objects.hash(localCheckpoint, globalCheckpoint);
        }
    }

    /**
     * The {@code ReplicasProxy} is an implementation of the {@code Replicas}
     * interface that performs the actual {@code ReplicaRequest} on the replica
     * shards. It also encapsulates the logic required for failing the replica
     * if deemed necessary as well as marking it as stale when needed.
     *
     * @opensearch.internal
     */
    protected class ReplicasProxy implements Replicas<ReplicaRequest> {

        @Override
        public void performOn(
            final ShardRouting replica,
            final ReplicaRequest request,
            final long primaryTerm,
            final long globalCheckpoint,
            final long maxSeqNoOfUpdatesOrDeletes,
            final ActionListener<ReplicationOperation.ReplicaResponse> listener
        ) {
            String nodeId = replica.currentNodeId();
            final DiscoveryNode node = clusterService.state().nodes().get(nodeId);
            if (node == null) {
                listener.onFailure(new NoNodeAvailableException("unknown node [" + nodeId + "]"));
                return;
            }
            final ConcreteReplicaRequest<ReplicaRequest> replicaRequest = new ConcreteReplicaRequest<>(
                request,
                replica.allocationId().getId(),
                primaryTerm,
                globalCheckpoint,
                maxSeqNoOfUpdatesOrDeletes
            );
            final ActionListenerResponseHandler<ReplicaResponse> handler = new ActionListenerResponseHandler<>(
                listener,
                ReplicaResponse::new
            );
            transportService.sendRequest(node, transportReplicaAction, replicaRequest, transportOptions, handler);
        }

        @Override
        public void failShardIfNeeded(
            ShardRouting replica,
            long primaryTerm,
            String message,
            Exception exception,
            ActionListener<Void> listener
        ) {
            // This does not need to fail the shard. The idea is that this
            // is a non-write operation (something like a refresh or a global
            // checkpoint sync) and therefore the replica should still be
            // "alive" if it were to fail.
            listener.onResponse(null);
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener) {
            // This does not need to make the shard stale. The idea is that this
            // is a non-write operation (something like a refresh or a global
            // checkpoint sync) and therefore the replica should still be
            // "alive" if it were to be marked as stale.
            listener.onResponse(null);
        }
    }

    /**
     * a wrapper class to encapsulate a request when being sent to a specific allocation id
     *
     * @opensearch.internal
     */
    public static class ConcreteShardRequest<R extends TransportRequest> extends TransportRequest {

        /**
         * {@link AllocationId#getId()} of the shard this request is sent to
         **/
        private final String targetAllocationID;
        private final long primaryTerm;
        private final R request;
        // Indicates if this primary shard request originated by a reroute on this local node.
        private final boolean sentFromLocalReroute;
        // Indicates if this local reroute was initiated by the NodeClient executing a transport action. This
        // is only true if sentFromLocalReroute is true.
        private final boolean localRerouteInitiatedByNodeClient;

        public ConcreteShardRequest(Writeable.Reader<R> requestReader, StreamInput in) throws IOException {
            targetAllocationID = in.readString();
            primaryTerm = in.readVLong();
            sentFromLocalReroute = false;
            localRerouteInitiatedByNodeClient = false;
            request = requestReader.read(in);
        }

        public ConcreteShardRequest(R request, String targetAllocationID, long primaryTerm) {
            this(request, targetAllocationID, primaryTerm, false, false);
        }

        public ConcreteShardRequest(
            R request,
            String targetAllocationID,
            long primaryTerm,
            boolean sentFromLocalReroute,
            boolean localRerouteInitiatedByNodeClient
        ) {
            Objects.requireNonNull(request);
            Objects.requireNonNull(targetAllocationID);
            this.request = request;
            this.targetAllocationID = targetAllocationID;
            this.primaryTerm = primaryTerm;
            this.sentFromLocalReroute = sentFromLocalReroute;
            this.localRerouteInitiatedByNodeClient = localRerouteInitiatedByNodeClient;
        }

        @Override
        public void setParentTask(String parentTaskNode, long parentTaskId) {
            request.setParentTask(parentTaskNode, parentTaskId);
        }

        @Override
        public void setParentTask(TaskId taskId) {
            request.setParentTask(taskId);
        }

        @Override
        public TaskId getParentTask() {
            return request.getParentTask();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return request.createTask(id, type, action, parentTaskId, headers);
        }

        @Override
        public String getDescription() {
            return "[" + request.getDescription() + "] for aID [" + targetAllocationID + "] and term [" + primaryTerm + "]";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // If sentFromLocalReroute is marked true, then this request should just be looped back through
            // the local transport. It should never be serialized to be sent over the wire. If it is sent over
            // the wire, then it was NOT sent from a local reroute.
            assert sentFromLocalReroute == false;
            assert localRerouteInitiatedByNodeClient == false;
            out.writeString(targetAllocationID);
            out.writeVLong(primaryTerm);
            request.writeTo(out);
        }

        public boolean sentFromLocalReroute() {
            return sentFromLocalReroute;
        }

        public boolean localRerouteInitiatedByNodeClient() {
            return localRerouteInitiatedByNodeClient;
        }

        public R getRequest() {
            return request;
        }

        public String getTargetAllocationID() {
            return targetAllocationID;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }

        @Override
        public String toString() {
            return "request: " + request + ", target allocation id: " + targetAllocationID + ", primary term: " + primaryTerm;
        }
    }

    /**
     * Internal request for concrete replica
     *
     * @opensearch.internal
     */
    protected static final class ConcreteReplicaRequest<R extends TransportRequest> extends ConcreteShardRequest<R> {

        private final long globalCheckpoint;
        private final long maxSeqNoOfUpdatesOrDeletes;

        public ConcreteReplicaRequest(Writeable.Reader<R> requestReader, StreamInput in) throws IOException {
            super(requestReader, in);
            globalCheckpoint = in.readZLong();
            maxSeqNoOfUpdatesOrDeletes = in.readZLong();
        }

        public ConcreteReplicaRequest(
            final R request,
            final String targetAllocationID,
            final long primaryTerm,
            final long globalCheckpoint,
            final long maxSeqNoOfUpdatesOrDeletes
        ) {
            super(request, targetAllocationID, primaryTerm);
            this.globalCheckpoint = globalCheckpoint;
            this.maxSeqNoOfUpdatesOrDeletes = maxSeqNoOfUpdatesOrDeletes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(globalCheckpoint);
            out.writeZLong(maxSeqNoOfUpdatesOrDeletes);
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }

        public long getMaxSeqNoOfUpdatesOrDeletes() {
            return maxSeqNoOfUpdatesOrDeletes;
        }

        @Override
        public String toString() {
            return "ConcreteReplicaRequest{"
                + "targetAllocationID='"
                + getTargetAllocationID()
                + '\''
                + ", primaryTerm='"
                + getPrimaryTerm()
                + '\''
                + ", request="
                + getRequest()
                + ", globalCheckpoint="
                + globalCheckpoint
                + ", maxSeqNoOfUpdatesOrDeletes="
                + maxSeqNoOfUpdatesOrDeletes
                + '}';
        }
    }

    /**
     * Sets the current phase on the task if it isn't null. Pulled into its own
     * method because its more convenient that way.
     */
    protected static void setPhase(ReplicationTask task, String phase) {
        if (task != null) {
            task.setPhase(phase);
        }
    }
}
