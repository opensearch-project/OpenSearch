/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FetchByRowIdsRequest;
import org.opensearch.analytics.exec.action.FetchByRowIdsResponse;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Late materialization (QTF) root stage execution.
 *
 * <p>Lifecycle: started by PlanWalker after the child COORDINATOR_REDUCE stage completes.
 * Reads the reduced output (sorted + limited rows with __row_id__ + shard_id),
 * builds a position map, dispatches fetch requests per shard, assembles the final
 * globally-sorted result, and feeds it into the output sink.
 *
 * @opensearch.internal
 */
public final class LateMaterializationStageExecution extends AbstractStageExecution
    implements
        DataProducer,
        org.opensearch.analytics.spi.DataConsumer {

    private static final Logger logger = LogManager.getLogger(LateMaterializationStageExecution.class);

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final AnalyticsSearchTransportService dispatcher;
    private final org.opensearch.analytics.exec.RowProducingSink inputSink;

    LateMaterializationStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        AnalyticsSearchTransportService dispatcher
    ) {
        super(stage);
        this.config = config;
        this.outputSink = outputSink;
        this.dispatcher = dispatcher;
        this.inputSink = new org.opensearch.analytics.exec.RowProducingSink();
    }

    @Override
    public ExchangeSink inputSink(int childStageId) {
        return inputSink;
    }

    @Override
    public void start() {
        if (transitionTo(StageExecution.State.RUNNING) == false) return;

        Iterable<VectorSchemaRoot> reducedResult = inputSink.readResult();
        BufferAllocator allocator = config.bufferAllocator();
        List<ShardExecutionTarget> shardTargets = config.getResolvedShardTargets();

        VectorSchemaRoot firstBatch = reducedResult.iterator().hasNext() ? reducedResult.iterator().next() : null;
        if (firstBatch == null || firstBatch.getVector("__row_id__") == null || firstBatch.getVector("shard_id") == null) {
            // Not a QTF result — pass through unchanged
            for (VectorSchemaRoot batch : reducedResult) {
                outputSink.feed(batch);
            }
            transitionTo(StageExecution.State.SUCCEEDED);
            return;
        }

        // Derive fetch columns from schema (exclude __row_id__ and shard_id)
        String[] fetchColumns = firstBatch.getSchema()
            .getFields()
            .stream()
            .map(Field::getName)
            .filter(name -> !"__row_id__".equals(name) && !"shard_id".equals(name))
            .toArray(String[]::new);

        // Build position map
        PositionMap positionMap = buildPositionMap(reducedResult);
        logger.info("[LateMat] Position map built: totalRows={}, shards={}", positionMap.totalRows(), positionMap.shardCount());

        // Close reduced batches — we've extracted what we need
        for (VectorSchemaRoot batch : reducedResult) {
            batch.close();
        }

        if (positionMap.totalRows() == 0) {
            transitionTo(StageExecution.State.SUCCEEDED);
            return;
        }

        // Dispatch fetches
        dispatchFetches(positionMap, fetchColumns, shardTargets, allocator);
    }

    @Override
    public void cancel(String reason) {
        transitionTo(StageExecution.State.CANCELLED);
    }

    @Override
    public ExchangeSource outputSource() {
        if (outputSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    // ── Position Map ─────────────────────────────────────────────────────────────

    private PositionMap buildPositionMap(Iterable<VectorSchemaRoot> reducedResult) {
        PositionMap map = new PositionMap();
        int pos = 0;
        for (VectorSchemaRoot batch : reducedResult) {
            FieldVector rowIdRaw = batch.getVector("__row_id__");
            IntVector shardIdCol = (IntVector) batch.getVector("shard_id");
            for (int i = 0; i < batch.getRowCount(); i++) {
                int shard = shardIdCol.get(i);
                long rowId;
                if (rowIdRaw instanceof BigIntVector bigInt) {
                    rowId = bigInt.get(i);
                } else {
                    rowId = ((Number) rowIdRaw.getObject(i)).longValue();
                }
                map.put(shard, rowId, pos);
                pos++;
            }
        }
        return map;
    }

    // ── Fetch Dispatch ───────────────────────────────────────────────────────────

    private void dispatchFetches(
        PositionMap positionMap,
        String[] fetchColumns,
        List<ShardExecutionTarget> shardTargets,
        BufferAllocator allocator
    ) {
        Map<Integer, long[]> fetchPlan = positionMap.getPerShardFetchPlan();
        AtomicInteger remaining = new AtomicInteger(fetchPlan.size());
        List<FetchResult> fetchResults = java.util.Collections.synchronizedList(new ArrayList<>());

        for (Map.Entry<Integer, long[]> entry : fetchPlan.entrySet()) {
            int shardOrdinal = entry.getKey();
            long[] rowIds = entry.getValue();

            if (shardOrdinal >= shardTargets.size()) {
                captureFailure(
                    new IllegalStateException("[LateMat] Shard ordinal " + shardOrdinal + " exceeds target count " + shardTargets.size())
                );
                transitionTo(StageExecution.State.FAILED);
                return;
            }

            ShardExecutionTarget target = shardTargets.get(shardOrdinal);
            FetchByRowIdsRequest fetchReq = new FetchByRowIdsRequest(config.queryId(), target.shardId(), rowIds, fetchColumns);

            dispatcher.dispatchFetch(
                fetchReq,
                target.node(),
                new FetchResponseListener(shardOrdinal, positionMap, fetchResults, remaining, allocator),
                config.parentTask()
            );
        }
    }

    // ── Assembly ─────────────────────────────────────────────────────────────────

    private class FetchResponseListener implements StreamingResponseListener<FetchByRowIdsResponse> {
        private final int shardOrdinal;
        private final PositionMap positionMap;
        private final List<FetchResult> fetchResults;
        private final AtomicInteger remaining;
        private final BufferAllocator allocator;

        FetchResponseListener(
            int shardOrdinal,
            PositionMap positionMap,
            List<FetchResult> fetchResults,
            AtomicInteger remaining,
            BufferAllocator allocator
        ) {
            this.shardOrdinal = shardOrdinal;
            this.positionMap = positionMap;
            this.fetchResults = fetchResults;
            this.remaining = remaining;
            this.allocator = allocator;
        }

        @Override
        public void onStreamResponse(FetchByRowIdsResponse response, boolean isLast) {
            fetchResults.add(new FetchResult(shardOrdinal, response));
            if (isLast && remaining.decrementAndGet() == 0) {
                assembleAndDeliver();
            }
        }

        @Override
        public void onFailure(Exception e) {
            captureFailure(e);
            transitionTo(StageExecution.State.FAILED);
        }

        private void assembleAndDeliver() {
            try {
                VectorSchemaRoot assembled = assembleResult(fetchResults, positionMap, allocator);
                outputSink.feed(assembled);
                transitionTo(StageExecution.State.SUCCEEDED);
            } catch (Exception e) {
                captureFailure(e);
                transitionTo(StageExecution.State.FAILED);
            }
        }
    }

    private VectorSchemaRoot assembleResult(List<FetchResult> fetchResults, PositionMap positionMap, BufferAllocator allocator) {
        int totalRows = positionMap.totalRows();
        VectorSchemaRoot output = null;
        List<Field> outputFields = null;

        for (FetchResult fr : fetchResults) {
            byte[] ipc = fr.response().getIpcPayload();
            if (ipc == null || ipc.length == 0) continue;
            int shardOrdinal = fr.shardOrdinal();

            try (var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(new java.io.ByteArrayInputStream(ipc), allocator)) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                    int batchRows = batch.getRowCount();

                    if (output == null) {
                        outputFields = batch.getSchema().getFields().stream().filter(f -> !"__row_id__".equals(f.getName())).toList();
                        output = VectorSchemaRoot.create(new Schema(outputFields), allocator);
                        output.allocateNew();
                        output.setRowCount(totalRows);
                        for (FieldVector v : output.getFieldVectors()) {
                            v.setValueCount(totalRows);
                        }
                    }

                    FieldVector rowIdRaw = batch.getVector("__row_id__");
                    for (int i = 0; i < batchRows; i++) {
                        long rowId;
                        if (rowIdRaw instanceof BigIntVector bigInt) {
                            rowId = bigInt.get(i);
                        } else {
                            rowId = ((Number) rowIdRaw.getObject(i)).longValue();
                        }
                        int destPos = positionMap.getPosition(shardOrdinal, rowId);
                        for (Field f : outputFields) {
                            FieldVector src = batch.getVector(f.getName());
                            FieldVector dst = output.getVector(f.getName());
                            dst.copyFrom(i, destPos, src);
                        }
                    }
                }
            } catch (Exception e) {
                if (output != null) output.close();
                throw new RuntimeException("[LateMat] Failed to decode fetch response", e);
            }
        }

        if (output == null) {
            return VectorSchemaRoot.create(new Schema(List.of()), allocator);
        }
        return output;
    }

    // ── Supporting types ─────────────────────────────────────────────────────────

    private record FetchResult(int shardOrdinal, FetchByRowIdsResponse response) {
    }

    static class PositionMap {
        private final Map<Long, Integer> positionLookup = new HashMap<>();
        private final Map<Integer, List<Long>> perShardRowIds = new HashMap<>();
        private int totalRows = 0;

        void put(int shard, long rowId, int position) {
            positionLookup.put(encode(shard, rowId), position);
            perShardRowIds.computeIfAbsent(shard, k -> new ArrayList<>()).add(rowId);
            totalRows++;
        }

        int getPosition(int shard, long rowId) {
            Integer pos = positionLookup.get(encode(shard, rowId));
            if (pos == null) {
                throw new IllegalStateException("[LateMat] No position for shard=" + shard + " rowId=" + rowId);
            }
            return pos;
        }

        Map<Integer, long[]> getPerShardFetchPlan() {
            return perShardRowIds.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream().mapToLong(Long::longValue).toArray()));
        }

        int totalRows() {
            return totalRows;
        }

        int shardCount() {
            return perShardRowIds.size();
        }

        private static long encode(int shard, long rowId) {
            return ((long) shard << 40) | rowId;
        }
    }
}
