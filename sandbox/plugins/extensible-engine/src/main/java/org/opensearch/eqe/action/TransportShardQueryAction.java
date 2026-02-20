/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.action;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.eqe.engine.ShardQueryExecutor;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming transport action for shard-level plan fragment execution.
 *
 * <p>Registers a streaming request handler via {@link StreamTransportService}.
 * When a request arrives, the handler delegates to {@link ShardQueryExecutor}
 * to run the plan fragment, converts the Arrow {@link VectorSchemaRoot} result
 * to a {@link ShardQueryResponse}, and streams it back via
 * {@link TransportChannel#sendResponseBatch}.
 */
public class TransportShardQueryAction extends TransportAction<ShardQueryRequest, ShardQueryResponse> {

    private static final Logger logger = LogManager.getLogger(TransportShardQueryAction.class);

    private final ShardQueryExecutor shardQueryExecutor;
    private final IndicesService indicesService;

    @Inject
    public TransportShardQueryAction(
            StreamTransportService streamTransportService,
            ActionFilters actionFilters,
            IndicesService indicesService,
            ShardQueryExecutor shardQueryExecutor) {
        super(ShardQueryAction.NAME, actionFilters, streamTransportService.getTaskManager());
        this.shardQueryExecutor = shardQueryExecutor;
        this.indicesService = indicesService;

        streamTransportService.registerRequestHandler(
            ShardQueryAction.NAME,
            ThreadPool.Names.GENERIC,
            ShardQueryRequest::new,
            this::handleStreamRequest
        );
    }

    @Override
    protected void doExecute(Task task, ShardQueryRequest request, ActionListener<ShardQueryResponse> listener) {
        listener.onFailure(new UnsupportedOperationException("Use StreamTransportService for streaming requests"));
    }

    private void handleStreamRequest(ShardQueryRequest request, TransportChannel channel, Task task) throws IOException {
        try {
            IndexShard indexShard = indicesService.indexServiceSafe(request.shardId().getIndex())
                .getShard(request.shardId().id());
            VectorSchemaRoot root = shardQueryExecutor.execute(request.getPlanFragment(), indexShard);
            try {
                List<String> columns = new ArrayList<>();
                for (Field field : root.getSchema().getFields()) {
                    columns.add(field.getName());
                }

                List<Object[]> rows = new ArrayList<>();
                for (int rowIdx = 0; rowIdx < root.getRowCount(); rowIdx++) {
                    Object[] row = new Object[columns.size()];
                    for (int colIdx = 0; colIdx < columns.size(); colIdx++) {
                        FieldVector vector = root.getVector(colIdx);
                        Object value = vector.getObject(rowIdx);
                        // Convert Arrow-specific types (e.g. Text) to serializable Java types
                        if (value != null && !(value instanceof String) && !(value instanceof Number) && !(value instanceof Boolean)) {
                            value = value.toString();
                        }
                        row[colIdx] = value;
                    }
                    rows.add(row);
                }

                ShardQueryResponse response = new ShardQueryResponse(request.shardId(), columns, rows);
                channel.sendResponseBatch(response);
                channel.completeStream();
            } finally {
                root.close();
            }
        } catch (StreamException e) {
            if (e.getErrorCode() == StreamErrorCode.CANCELLED) {
                logger.info("Client cancelled stream: {}", e.getMessage());
            } else {
                channel.sendResponse(e);
            }
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }
}
