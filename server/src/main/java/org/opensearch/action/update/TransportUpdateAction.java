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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.RoutingMissingException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.TransportBulkAction;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Map;

import static org.opensearch.ExceptionsHelper.unwrapCause;
import static org.opensearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.opensearch.action.bulk.TransportSingleItemBulkWriteAction.wrapBulkResponse;

/**
 * Performs the update operation by delegating to {@link TransportBulkAction} with a single update operation.
 *
 * @opensearch.internal
 */
public class TransportUpdateAction extends HandledTransportAction<UpdateRequest, UpdateResponse> {

    private final TransportBulkAction bulkAction;

    // The following fields can be removed once we remove ShardTransportHandler.
    private static final Logger logger = LogManager.getLogger(TransportUpdateAction.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportUpdateAction.class);
    private static final String SHARD_ACTION_NAME = UpdateAction.NAME + "[s]";
    private final ThreadPool threadPool;
    private final IndicesService indicesService;
    private final UpdateHelper updateHelper;
    private final NodeClient client;

    @Inject
    public TransportUpdateAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        TransportBulkAction bulkAction,
        IndicesService indicesService,
        UpdateHelper updateHelper,
        NodeClient client
    ) {
        super(UpdateAction.NAME, transportService, actionFilters, UpdateRequest::new);
        this.threadPool = threadPool;
        this.bulkAction = bulkAction;
        this.indicesService = indicesService;
        this.updateHelper = updateHelper;
        this.client = client;
        transportService.registerRequestHandler(SHARD_ACTION_NAME, ThreadPool.Names.SAME, UpdateRequest::new, new ShardTransportHandler());
    }

    public static void resolveAndValidateRouting(Metadata metadata, String concreteIndex, UpdateRequest request) {
        request.routing((metadata.resolveWriteIndexRouting(request.routing(), request.index())));
        // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
        if (request.routing() == null && metadata.routingRequired(concreteIndex)) {
            throw new RoutingMissingException(concreteIndex, request.id());
        }
    }

    @Override
    protected void doExecute(Task task, UpdateRequest request, ActionListener<UpdateResponse> listener) {
        // The following is mostly copied from TransportSingleItemBulkAction
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setRefreshPolicy(request.getRefreshPolicy());
        bulkRequest.timeout(request.timeout());
        bulkRequest.waitForActiveShards(request.waitForActiveShards());
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        bulkAction.execute(task, bulkRequest, ActionListener.wrap(bulkItemResponses -> {
            assert bulkItemResponses.getItems().length == 1 : "expected only one item in bulk request";
            BulkItemResponse bulkItemResponse = bulkItemResponses.getItems()[0];
            if (bulkItemResponse.isFailed() == false) {
                final UpdateResponse response = bulkItemResponse.getResponse();
                listener.onResponse(response);
            } else {
                listener.onFailure(bulkItemResponse.getFailure().getCause());
            }
        }, listener::onFailure));
    }

    /**
     * Transport handler per shard.
     *
     * @deprecated This only exists for BWC with 2.x. We can remove this when we release OpenSearch 4.0.
     * @opensearch.internal
     */
    @Deprecated(forRemoval = true, since = "3.0")
    private class ShardTransportHandler implements TransportRequestHandler<UpdateRequest> {

        protected String executor(ShardId shardId) {
            final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            return indexService.getIndexSettings().getIndexMetadata().isSystem() ? ThreadPool.Names.SYSTEM_WRITE : ThreadPool.Names.WRITE;
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

                    IndexingStats.Stats.DocStatusStats stats = new IndexingStats.Stats.DocStatusStats();
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

        @Override
        public void messageReceived(final UpdateRequest request, final TransportChannel channel, Task task) throws Exception {
            threadPool.executor(executor(request.shardId())).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed to send response for " + SHARD_ACTION_NAME, inner);
                    }
                }

                @Override
                protected void doRun() {
                    shardOperation(request, ActionListener.wrap(channel::sendResponse, this::onFailure), 0);
                }
            });
        }
    }
}
