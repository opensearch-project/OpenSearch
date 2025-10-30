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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportActions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.WriteResponse;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.PrimaryShardClosedException;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.Translog.Location;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.listener.TraceableActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 * Allows performing async actions (e.g. refresh) after performing write operations on primary and replica shards
 *
 * @opensearch.internal
 */
public abstract class TransportWriteAction<
    Request extends ReplicatedWriteRequest<Request>,
    ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
    Response extends ReplicationResponse & WriteResponse> extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected final IndexingPressureService indexingPressureService;
    protected final SystemIndices systemIndices;

    private final Function<IndexShard, String> executorFunction;
    private final Tracer tracer;

    protected TransportWriteAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Writeable.Reader<ReplicaRequest> replicaRequest,
        Function<IndexShard, String> executorFunction,
        boolean forceExecutionOnPrimary,
        IndexingPressureService indexingPressureService,
        SystemIndices systemIndices,
        Tracer tracer,
        AdmissionControlActionType admissionControlActionType
    ) {
        // We pass ThreadPool.Names.SAME to the super class as we control the dispatching to the
        // ThreadPool.Names.WRITE/ThreadPool.Names.SYSTEM_WRITE thread pools in this class.
        super(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            request,
            replicaRequest,
            ThreadPool.Names.SAME,
            true,
            forceExecutionOnPrimary,
            admissionControlActionType
        );
        this.executorFunction = executorFunction;
        this.indexingPressureService = indexingPressureService;
        this.systemIndices = systemIndices;
        this.tracer = tracer;
    }

    protected TransportWriteAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Writeable.Reader<ReplicaRequest> replicaRequest,
        Function<IndexShard, String> executorFunction,
        boolean forceExecutionOnPrimary,
        IndexingPressureService indexingPressureService,
        SystemIndices systemIndices,
        Tracer tracer
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
            request,
            replicaRequest,
            executorFunction,
            forceExecutionOnPrimary,
            indexingPressureService,
            systemIndices,
            tracer,
            null
        );
    }

    protected String executor(IndexShard shard) {
        return executorFunction.apply(shard);
    }

    @Override
    protected Releasable checkOperationLimits(Request request) {
        return indexingPressureService.markPrimaryOperationStarted(request.shardId, primaryOperationSize(request), force(request));
    }

    protected boolean force(ReplicatedWriteRequest<?> request) {
        return forceExecutionOnPrimary || isSystemShard(request.shardId);
    }

    protected boolean isSystemShard(ShardId shardId) {
        final IndexAbstraction abstraction = clusterService.state().metadata().getIndicesLookup().get(shardId.getIndexName());
        return abstraction != null ? abstraction.isSystem() : systemIndices.isSystemIndex(shardId.getIndexName());
    }

    @Override
    protected Releasable checkPrimaryLimits(Request request, boolean rerouteWasLocal, boolean localRerouteInitiatedByNodeClient) {
        if (rerouteWasLocal) {
            // If this primary request was received from a local reroute initiated by the node client, we
            // must mark a new primary operation local to the coordinating node.
            if (localRerouteInitiatedByNodeClient) {
                return indexingPressureService.markPrimaryOperationLocalToCoordinatingNodeStarted(
                    request.shardId,
                    primaryOperationSize(request)
                );
            } else {
                return () -> {};
            }
        } else {
            // If this primary request was received directly from the network, we must mark a new primary
            // operation. This happens if the write action skips the reroute step (ex: rsync) or during
            // primary delegation, after the primary relocation hand-off.
            return indexingPressureService.markPrimaryOperationStarted(request.shardId, primaryOperationSize(request), force(request));
        }
    }

    protected long primaryOperationSize(Request request) {
        return 0;
    }

    @Override
    protected Releasable checkReplicaLimits(ReplicaRequest request) {
        return indexingPressureService.markReplicaOperationStarted(request.shardId, replicaOperationSize(request), force(request));
    }

    protected long replicaOperationSize(ReplicaRequest request) {
        return 0;
    }

    /** Syncs operation result to the translog or throws a shard not available failure */
    protected static Location syncOperationResultOrThrow(final Engine.Result operationResult, final Location currentLocation)
        throws Exception {
        final Location location;
        if (operationResult.getFailure() != null) {
            // check if any transient write operation failures should be bubbled up
            Exception failure = operationResult.getFailure();
            assert failure instanceof MapperParsingException : "expected mapper parsing failures. got " + failure;
            throw failure;
        } else {
            location = locationToSync(currentLocation, operationResult.getTranslogLocation());
        }
        return location;
    }

    public static Location locationToSync(Location current, Location next) {
        /* here we are moving forward in the translog with each operation. Under the hood this might
         * cross translog files which is ok since from the user perspective the translog is like a
         * tape where only the highest location needs to be fsynced in order to sync all previous
         * locations even though they are not in the same file. When the translog rolls over files
         * the previous file is fsynced on after closing if needed.*/
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 : "translog locations are not increasing";
        return next;
    }

    @Override
    protected ReplicationOperation.Replicas<ReplicaRequest> newReplicasProxy() {
        return new WriteActionReplicasProxy();
    }

    /**
     * Called on the primary with a reference to the primary {@linkplain IndexShard} to modify.
     *
     * @param listener listener for the result of the operation on primary, including current translog location and operation response
     * and failure async refresh is performed on the <code>primary</code> shard according to the <code>Request</code> refresh policy
     */
    @Override
    protected void shardOperationOnPrimary(
        Request request,
        IndexShard primary,
        ActionListener<PrimaryResult<ReplicaRequest, Response>> listener
    ) {
        final String executor = executorFunction.apply(primary);
        Span queueTimeSpan = tracer.startSpan(
            SpanBuilder.from("dispatchedShardOperationOnPrimaryQueued", clusterService.localNode().getId(), request)
        );

        try (SpanScope spanScope = tracer.withSpanInScope(queueTimeSpan)) {
            threadPool.executor(executor).execute(new ActionRunnable<PrimaryResult<ReplicaRequest, Response>>(listener) {
                @Override
                public void onFailure(Exception e) {
                    queueTimeSpan.setError(e);
                    queueTimeSpan.endSpan();
                    super.onFailure(e);
                }

                @Override
                public void onRejection(Exception e) {
                    queueTimeSpan.setError(e);
                    queueTimeSpan.endSpan();
                    super.onRejection(e);
                }

                @Override
                protected void doRun() {
                    queueTimeSpan.endSpan();
                    Span span = tracer.startSpan(
                        SpanBuilder.from("dispatchedShardOperationOnPrimary", clusterService.localNode().getId(), request)
                    );
                    try (SpanScope spanScope = tracer.withSpanInScope(span)) {
                        dispatchedShardOperationOnPrimary(request, primary, TraceableActionListener.create(listener, span, tracer));
                    }
                }

                @Override
                public boolean isForceExecution() {
                    return force(request);
                }
            });
        }
    }

    protected abstract void dispatchedShardOperationOnPrimary(
        Request request,
        IndexShard primary,
        ActionListener<PrimaryResult<ReplicaRequest, Response>> listener
    );

    /**
     * Called once per replica with a reference to the replica {@linkplain IndexShard} to modify.
     *
     * @param listener listener for the result of the operation on replica, including current translog location and operation
     * response and failure async refresh is performed on the <code>replica</code> shard according to the <code>ReplicaRequest</code>
     * refresh policy
     */
    @Override
    protected void shardOperationOnReplica(ReplicaRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        Span queueTimeSpan = tracer.startSpan(
            SpanBuilder.from("dispatchedShardOperationOnReplicaQueued", clusterService.localNode().getId(), request)
        );
        try (SpanScope spanScope = tracer.withSpanInScope(queueTimeSpan)) {
            threadPool.executor(executorFunction.apply(replica)).execute(new ActionRunnable<ReplicaResult>(listener) {
                @Override
                public void onFailure(Exception e) {
                    queueTimeSpan.setError(e);
                    queueTimeSpan.endSpan();
                    super.onFailure(e);
                }

                @Override
                public void onRejection(Exception e) {
                    queueTimeSpan.setError(e);
                    queueTimeSpan.endSpan();
                    super.onRejection(e);
                }

                @Override
                protected void doRun() {
                    queueTimeSpan.endSpan();
                    Span span = tracer.startSpan(
                        SpanBuilder.from("dispatchedShardOperationOnReplica", clusterService.localNode().getId(), request)
                    );
                    try (SpanScope spanScope = tracer.withSpanInScope(span)) {
                        dispatchedShardOperationOnReplica(request, replica, TraceableActionListener.create(listener, span, tracer));
                    }
                }

                @Override
                public boolean isForceExecution() {
                    return true;
                }
            });
        }
    }

    protected abstract void dispatchedShardOperationOnReplica(
        ReplicaRequest request,
        IndexShard replica,
        ActionListener<ReplicaResult> listener
    );

    /**
     * Result of taking the action on the primary.
     * <p>
     * NOTE: public for testing
     *
     * @opensearch.internal
     */
    public static class WritePrimaryResult<
        ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
        Response extends ReplicationResponse & WriteResponse> extends PrimaryResult<ReplicaRequest, Response> {
        public final Location location;
        public final IndexShard primary;
        private final Logger logger;

        public WritePrimaryResult(
            ReplicaRequest request,
            @Nullable Response finalResponse,
            @Nullable Location location,
            @Nullable Exception operationFailure,
            IndexShard primary,
            Logger logger
        ) {
            super(request, finalResponse, operationFailure);
            this.location = location;
            this.primary = primary;
            this.logger = logger;
            assert location == null || operationFailure == null : "expected either failure to be null or translog location to be null, "
                + "but found: ["
                + location
                + "] translog location and ["
                + operationFailure
                + "] failure";
        }

        @Override
        public void runPostReplicationActions(ActionListener<Void> listener) {
            if (finalFailure != null) {
                listener.onFailure(finalFailure);
            } else {
                /*
                 * We call this after replication because this might wait for a refresh and that can take a while.
                 * This way we wait for the refresh in parallel on the primary and on the replica.
                 */
                new AsyncAfterWriteAction(primary, replicaRequest, location, new RespondingWriteResult() {
                    @Override
                    public void onSuccess(boolean forcedRefresh) {
                        finalResponseIfSuccessful.setForcedRefresh(forcedRefresh);
                        listener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception ex) {
                        listener.onFailure(ex);
                    }
                }, logger).run();
            }
        }
    }

    /**
     * Result of taking the action on the replica.
     *
     * @opensearch.internal
     */
    public static class WriteReplicaResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>> extends ReplicaResult {
        public final Location location;
        private final ReplicaRequest request;
        private final IndexShard replica;
        private final Logger logger;

        public WriteReplicaResult(
            ReplicaRequest request,
            @Nullable Location location,
            @Nullable Exception operationFailure,
            IndexShard replica,
            Logger logger
        ) {
            super(operationFailure);
            this.location = location;
            this.request = request;
            this.replica = replica;
            this.logger = logger;
        }

        @Override
        public void runPostReplicaActions(ActionListener<Void> listener) {
            if (finalFailure != null) {
                listener.onFailure(finalFailure);
            } else {
                new AsyncAfterWriteAction(replica, request, location, new RespondingWriteResult() {
                    @Override
                    public void onSuccess(boolean forcedRefresh) {
                        listener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception ex) {
                        listener.onFailure(ex);
                    }
                }, logger).run();
            }
        }
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * callback used by {@link AsyncAfterWriteAction} to notify that all post
     * process actions have been executed
     */
    interface RespondingWriteResult {
        /**
         * Called on successful processing of all post write actions
         * @param forcedRefresh <code>true</code> iff this write has caused a refresh
         */
        void onSuccess(boolean forcedRefresh);

        /**
         * Called on failure if a post action failed.
         */
        void onFailure(Exception ex);
    }

    /**
     * This class encapsulates post write actions like async waits for
     * translog syncs or waiting for a refresh to happen making the write operation
     * visible.
     *
     * @opensearch.internal
     */
    static final class AsyncAfterWriteAction {
        private final Location location;
        private final boolean waitUntilRefresh;
        private final boolean sync;
        private final AtomicInteger pendingOps = new AtomicInteger(1);
        private final AtomicBoolean refreshed = new AtomicBoolean(false);
        private final AtomicReference<Exception> syncFailure = new AtomicReference<>(null);
        private final RespondingWriteResult respond;
        private final IndexShard indexShard;
        private final WriteRequest<?> request;
        private final Logger logger;

        AsyncAfterWriteAction(
            final IndexShard indexShard,
            final WriteRequest<?> request,
            @Nullable final Translog.Location location,
            final RespondingWriteResult respond,
            final Logger logger
        ) {
            this.indexShard = indexShard;
            this.request = request;
            boolean waitUntilRefresh = false;
            switch (request.getRefreshPolicy()) {
                case IMMEDIATE:
                    indexShard.refresh("refresh_flag_index");
                    refreshed.set(true);
                    break;
                case WAIT_UNTIL:
                    if (location != null) {
                        waitUntilRefresh = true;
                        pendingOps.incrementAndGet();
                    }
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("unknown refresh policy: " + request.getRefreshPolicy());
            }
            this.waitUntilRefresh = waitUntilRefresh;
            this.respond = respond;
            this.location = location;
            if ((sync = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null)) {
                pendingOps.incrementAndGet();
            }
            this.logger = logger;
            assert pendingOps.get() >= 0 && pendingOps.get() <= 3 : "pendingOpts was: " + pendingOps.get();
        }

        /** calls the response listener if all pending operations have returned otherwise it just decrements the pending opts counter.*/
        private void maybeFinish() {
            final int numPending = pendingOps.decrementAndGet();
            if (numPending == 0) {
                if (syncFailure.get() != null) {
                    respond.onFailure(syncFailure.get());
                } else {
                    respond.onSuccess(refreshed.get());
                }
            }
            assert numPending >= 0 && numPending <= 2 : "numPending must either 2, 1 or 0 but was " + numPending;
        }

        void run() {
            /*
             * We either respond immediately (i.e., if we do not fsync per request or wait for
             * refresh), or we there are past async operations and we wait for them to return to
             * respond.
             */
            indexShard.afterWriteOperation();
            // decrement pending by one, if there is nothing else to do we just respond with success
            maybeFinish();
            if (waitUntilRefresh) {
                assert pendingOps.get() > 0;
                indexShard.addRefreshListener(location, forcedRefresh -> {
                    if (forcedRefresh) {
                        logger.warn("block until refresh ran out of slots and forced a refresh: [{}]", request);
                    }
                    refreshed.set(forcedRefresh);
                    maybeFinish();
                });
            }
            if (sync) {
                assert pendingOps.get() > 0;
                indexShard.sync(location, (ex) -> {
                    syncFailure.set(ex);
                    maybeFinish();
                });
            }
        }
    }

    /**
     * A proxy for <b>write</b> operations that need to be performed on the
     * replicas, where a failure to execute the operation should fail
     * the replica shard and/or mark the replica as stale.
     * <p>
     * This extends {@code TransportReplicationAction.ReplicasProxy} to do the
     * failing and stale-ing.
     *
     * @opensearch.internal
     */
    protected class WriteActionReplicasProxy extends ReplicasProxy {

        @Override
        public void failShardIfNeeded(
            ShardRouting replica,
            long primaryTerm,
            String message,
            Exception exception,
            ActionListener<Void> listener
        ) {
            if (TransportActions.isShardNotAvailableException(exception) == false) {
                logger.warn(new ParameterizedMessage("[{}] {}", replica.shardId(), message), exception);
            }
            // If a write action fails due to the closure of the primary shard
            // then the replicas should not be marked as failed since they are
            // still up-to-date with the (now closed) primary shard
            if (exception instanceof PrimaryShardClosedException == false) {
                shardStateAction.remoteShardFailed(
                    replica.shardId(),
                    replica.allocationId().getId(),
                    primaryTerm,
                    true,
                    message,
                    exception,
                    listener
                );
            }
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }
}
