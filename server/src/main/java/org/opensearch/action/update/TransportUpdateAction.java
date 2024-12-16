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

package org.opensearch.action.update;

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.RoutingMissingException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.action.support.TransportActions;
import org.opensearch.action.support.single.instance.TransportInstanceSingleOperationAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.PlainShardIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexingStats.Stats.DocStatusStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.ExceptionsHelper.unwrapCause;
import static org.opensearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.opensearch.action.bulk.TransportSingleItemBulkWriteAction.wrapBulkResponse;

/**
 * Transport action for updating an index
 *
 * @opensearch.internal
 */
public class TransportUpdateAction extends TransportInstanceSingleOperationAction<UpdateRequest, UpdateResponse> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportUpdateAction.class);
    private final AutoCreateIndex autoCreateIndex;
    private final UpdateHelper updateHelper;
    private final IndicesService indicesService;
    private final NodeClient client;
    private final ClusterService clusterService;

    @Inject
    public TransportUpdateAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        AutoCreateIndex autoCreateIndex,
        NodeClient client
    ) {
        super(
            UpdateAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            UpdateRequest::new
        );
        this.updateHelper = updateHelper;
        this.indicesService = indicesService;
        this.autoCreateIndex = autoCreateIndex;
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected String executor(ShardId shardId) {
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getIndexSettings().getIndexMetadata().isSystem() ? Names.SYSTEM_WRITE : Names.WRITE;
    }

    @Override
    protected UpdateResponse newResponse(StreamInput in) throws IOException {
        return new UpdateResponse(in);
    }

    @Override
    protected boolean retryOnFailure(Exception e) {
        return TransportActions.isShardNotAvailableException(e);
    }

    @Override
    protected void resolveRequest(ClusterState state, UpdateRequest request) {
        resolveAndValidateRouting(state.metadata(), request.concreteIndex(), request);
    }

    public static void resolveAndValidateRouting(Metadata metadata, String concreteIndex, UpdateRequest request) {
        request.routing((metadata.resolveWriteIndexRouting(request.routing(), request.index())));
        // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
        if (request.routing() == null && metadata.routingRequired(concreteIndex)) {
            throw new RoutingMissingException(concreteIndex, request.id());
        }
    }

    @Override
    protected void doExecute(Task task, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        if (request.isRequireAlias() && (clusterService.state().getMetadata().hasAlias(request.index()) == false)) {
            IndexNotFoundException e = new IndexNotFoundException(
                "[" + DocWriteRequest.REQUIRE_ALIAS + "] request flag is [true] and [" + request.index() + "] is not an alias",
                request.index()
            );

            incDocStatusStats(e);
            throw e;
        }
        // if we don't have a master, we don't have metadata, that's fine, let it find a cluster-manager using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            client.admin()
                .indices()
                .create(
                    new CreateIndexRequest().index(request.index()).cause("auto(update api)").clusterManagerNodeTimeout(request.timeout()),
                    new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            innerExecute(task, request, listener);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                                // we have the index, do it
                                try {
                                    innerExecute(task, request, listener);
                                } catch (Exception inner) {
                                    inner.addSuppressed(e);
                                    listener.onFailure(inner);
                                }
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    }
                );
        } else {
            innerExecute(task, request, listener);
        }
    }

    private void innerExecute(final Task task, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        super.doExecute(task, request, ActionListener.wrap(listener::onResponse, e -> {
            incDocStatusStats(e);
            listener.onFailure(e);
        }));
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, UpdateRequest request) {
        if (request.getShardId() != null) {
            return clusterState.routingTable().index(request.concreteIndex()).shard(request.getShardId().getId()).primaryShardIt();
        }
        ShardIterator shardIterator = clusterService.operationRouting()
            .indexShards(clusterState, request.concreteIndex(), request.id(), request.routing());
        ShardRouting shard;
        while ((shard = shardIterator.nextOrNull()) != null) {
            if (shard.primary()) {
                return new PlainShardIterator(shardIterator.shardId(), Collections.singletonList(shard));
            }
        }
        return new PlainShardIterator(shardIterator.shardId(), Collections.emptyList());
    }

    @Override
    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        shardOperation(request, listener, 0);
    }

    protected void shardOperation(final UpdateRequest request, final ActionListener<UpdateResponse> listener, final int retryCount) {
        final ShardId shardId = request.getShardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.getId());
        final UpdateHelper.Result result = updateHelper.prepare(request, indexShard, threadPool::absoluteTimeInMillis);
        switch (result.getResponseResult()) {
            case CREATED:
                IndexRequest upsertRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference upsertSourceBytes = upsertRequest.source();
                client.bulk(toSingleItemBulkRequest(upsertRequest), wrapBulkResponse(ActionListener.<IndexResponse>wrap(response -> {
                    UpdateResponse update = new UpdateResponse(
                        response.getShardInfo(),
                        response.getShardId(),
                        response.getId(),
                        response.getSeqNo(),
                        response.getPrimaryTerm(),
                        response.getVersion(),
                        response.getResult()
                    );
                    if (request.fetchSource() != null && request.fetchSource().fetchSource()) {
                        Tuple<? extends MediaType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(
                            upsertSourceBytes,
                            true,
                            upsertRequest.getContentType()
                        );
                        update.setGetResult(
                            UpdateHelper.extractGetResult(
                                request,
                                request.concreteIndex(),
                                response.getSeqNo(),
                                response.getPrimaryTerm(),
                                response.getVersion(),
                                sourceAndContent.v2(),
                                sourceAndContent.v1(),
                                upsertSourceBytes
                            )
                        );
                    } else {
                        update.setGetResult(null);
                    }
                    update.setForcedRefresh(response.forcedRefresh());
                    listener.onResponse(update);
                }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount))));

                break;
            case UPDATED:
                IndexRequest indexRequest = result.action();
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference indexSourceBytes = indexRequest.source();
                final Settings indexSettings = indexService.getIndexSettings().getSettings();
                if (IndexSettings.DEFAULT_PIPELINE.exists(indexSettings) || IndexSettings.FINAL_PIPELINE.exists(indexSettings)) {
                    deprecationLogger.deprecate(
                        "update_operation_with_ingest_pipeline",
                        "the index ["
                            + indexRequest.index()
                            + "] has a default ingest pipeline or a final ingest pipeline, the support of the ingest pipelines for update operation causes unexpected result and will be removed in 3.0.0"
                    );
                }
                client.bulk(toSingleItemBulkRequest(indexRequest), wrapBulkResponse(ActionListener.<IndexResponse>wrap(response -> {
                    UpdateResponse update = new UpdateResponse(
                        response.getShardInfo(),
                        response.getShardId(),
                        response.getId(),
                        response.getSeqNo(),
                        response.getPrimaryTerm(),
                        response.getVersion(),
                        response.getResult()
                    );
                    update.setGetResult(
                        UpdateHelper.extractGetResult(
                            request,
                            request.concreteIndex(),
                            response.getSeqNo(),
                            response.getPrimaryTerm(),
                            response.getVersion(),
                            result.updatedSourceAsMap(),
                            result.updateSourceContentType(),
                            indexSourceBytes
                        )
                    );
                    update.setForcedRefresh(response.forcedRefresh());
                    listener.onResponse(update);
                }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount))));
                break;
            case DELETED:
                DeleteRequest deleteRequest = result.action();
                client.bulk(toSingleItemBulkRequest(deleteRequest), wrapBulkResponse(ActionListener.<DeleteResponse>wrap(response -> {
                    UpdateResponse update = new UpdateResponse(
                        response.getShardInfo(),
                        response.getShardId(),
                        response.getId(),
                        response.getSeqNo(),
                        response.getPrimaryTerm(),
                        response.getVersion(),
                        response.getResult()
                    );
                    update.setGetResult(
                        UpdateHelper.extractGetResult(
                            request,
                            request.concreteIndex(),
                            response.getSeqNo(),
                            response.getPrimaryTerm(),
                            response.getVersion(),
                            result.updatedSourceAsMap(),
                            result.updateSourceContentType(),
                            null
                        )
                    );
                    update.setForcedRefresh(response.forcedRefresh());
                    listener.onResponse(update);
                }, exception -> handleUpdateFailureWithRetry(listener, request, exception, retryCount))));
                break;
            case NOOP:
                UpdateResponse update = result.action();
                IndexService indexServiceOrNull = indicesService.indexService(shardId.getIndex());
                if (indexServiceOrNull != null) {
                    IndexShard shard = indexService.getShardOrNull(shardId.getId());
                    if (shard != null) {
                        shard.noopUpdate();
                    }
                }

                DocStatusStats stats = new DocStatusStats();
                stats.inc(RestStatus.OK);

                indicesService.addDocStatusStats(stats);
                listener.onResponse(update);

                break;
            default:
                throw new IllegalStateException("Illegal result " + result.getResponseResult());
        }
    }

    private void handleUpdateFailureWithRetry(
        final ActionListener<UpdateResponse> listener,
        final UpdateRequest request,
        final Exception failure,
        int retryCount
    ) {
        final Throwable cause = unwrapCause(failure);
        if (cause instanceof VersionConflictEngineException) {
            if (retryCount < request.retryOnConflict()) {
                logger.trace(
                    "Retry attempt [{}] of [{}] on version conflict on [{}][{}][{}]",
                    retryCount + 1,
                    request.retryOnConflict(),
                    request.index(),
                    request.getShardId(),
                    request.id()
                );
                threadPool.executor(executor(request.getShardId()))
                    .execute(ActionRunnable.wrap(listener, l -> shardOperation(request, l, retryCount + 1)));
                return;
            }
        }
        listener.onFailure(cause instanceof Exception ? (Exception) cause : new NotSerializableExceptionWrapper(cause));
    }

    private void incDocStatusStats(final Exception e) {
        DocStatusStats stats = new DocStatusStats();
        stats.inc(ExceptionsHelper.status(e));
        indicesService.addDocStatusStats(stats);
    }
}
