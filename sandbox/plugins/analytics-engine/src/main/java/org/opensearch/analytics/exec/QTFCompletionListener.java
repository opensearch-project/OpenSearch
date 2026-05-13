/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * QTF completion listener: intercepts the reduced query-phase result,
 * builds a position map, dispatches fetch requests per shard, and
 * assembles the final globally-sorted output.
 *
 * <p>Wraps the real completion listener — the caller sees the final
 * assembled result as if the query executed in one shot.
 */
public class QTFCompletionListener implements ActionListener<Iterable<VectorSchemaRoot>> {

    private static final Logger logger = LogManager.getLogger(QTFCompletionListener.class);

    private final ActionListener<Iterable<VectorSchemaRoot>> realListener;
    private final String queryId;
    private final String[] fetchColumns;
    private final List<ShardExecutionTarget> shardTargets;
    private final AnalyticsSearchTransportService dispatcher;
    private final BufferAllocator allocator;

    public QTFCompletionListener(
        ActionListener<Iterable<VectorSchemaRoot>> realListener,
        String queryId,
        String[] fetchColumns,
        List<ShardExecutionTarget> shardTargets,
        AnalyticsSearchTransportService dispatcher,
        BufferAllocator allocator
    ) {
        this.realListener = realListener;
        this.queryId = queryId;
        this.fetchColumns = fetchColumns;
        this.shardTargets = shardTargets;
        this.dispatcher = dispatcher;
        this.allocator = allocator;
    }

    @Override
    public void onResponse(Iterable<VectorSchemaRoot> reducedResult) {
        try {
            // Check if this is actually a QTF query (has __row_id__ + shard_id in result)
            VectorSchemaRoot firstBatch = reducedResult.iterator().hasNext()
                ? reducedResult.iterator().next() : null;
            if (firstBatch == null
                || firstBatch.getVector("__row_id__") == null
                || firstBatch.getVector("shard_id") == null) {
                // Not a QTF query — pass through unchanged
                realListener.onResponse(reducedResult);
                return;
            }

            // Phase 2.5: Build position map from reduced output
            PositionMap positionMap = buildPositionMap(reducedResult);
            logger.info("[QTF] Position map built: totalRows={}, shards={}",
                positionMap.totalRows(), positionMap.shardCount());

            if (positionMap.totalRows() == 0) {
                realListener.onResponse(reducedResult);
                return;
            }

            // Phase 3: Dispatch fetch requests per shard
            dispatchFetches(positionMap);
        } catch (Exception e) {
            realListener.onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        realListener.onFailure(e);
    }

    // ── Phase 2.5: Build Position Map ──────────────────────────────────────────

    private PositionMap buildPositionMap(Iterable<VectorSchemaRoot> reducedResult) {
        PositionMap map = new PositionMap();
        int pos = 0;

        for (VectorSchemaRoot batch : reducedResult) {
            BigIntVector rowIdCol = (BigIntVector) batch.getVector("__row_id__");
            IntVector shardIdCol = (IntVector) batch.getVector("shard_id");

            if (rowIdCol == null || shardIdCol == null) {
                throw new IllegalStateException(
                    "[QTF] Reduced result missing __row_id__ or shard_id columns. "
                    + "Schema: " + batch.getSchema()
                );
            }

            for (int i = 0; i < batch.getRowCount(); i++) {
                int shard = shardIdCol.get(i);
                long rowId = rowIdCol.get(i);
                map.put(shard, rowId, pos);
                pos++;
            }
        }
        return map;
    }

    // ── Phase 3: Dispatch Fetches ──────────────────────────────────────────────

    private void dispatchFetches(PositionMap positionMap) {
        Map<Integer, long[]> fetchPlan = positionMap.getPerShardFetchPlan();

        // Pre-allocate final result buffer
        // For POC: we'll collect fetch results and assemble at the end
        AtomicInteger remaining = new AtomicInteger(fetchPlan.size());
        List<FetchResult> fetchResults = java.util.Collections.synchronizedList(new ArrayList<>());

        for (Map.Entry<Integer, long[]> entry : fetchPlan.entrySet()) {
            int shardOrdinal = entry.getKey();
            long[] rowIds = entry.getValue();

            if (shardOrdinal >= shardTargets.size()) {
                realListener.onFailure(new IllegalStateException(
                    "[QTF] Shard ordinal " + shardOrdinal + " exceeds target count " + shardTargets.size()
                ));
                return;
            }

            ShardExecutionTarget target = shardTargets.get(shardOrdinal);
            FragmentExecutionRequest fetchReq = FragmentExecutionRequest.fetchMode(
                queryId, target.shardId(), rowIds, fetchColumns
            );

            logger.info("[QTF] Dispatching fetch to shard {} (ordinal={}): {} row_ids",
                target.shardId(), shardOrdinal, rowIds.length);

            dispatcher.dispatchFragment(
                fetchReq,
                target.node(),
                new FetchResponseListener(shardOrdinal, positionMap, fetchResults, remaining),
                null,  // parentTask — POC simplification
                null   // pending — POC simplification
            );
        }
    }

    // ── Phase 4: Assembly ──────────────────────────────────────────────────────

    private class FetchResponseListener implements StreamingResponseListener<FragmentExecutionResponse> {
        private final int shardOrdinal;
        private final PositionMap positionMap;
        private final List<FetchResult> fetchResults;
        private final AtomicInteger remaining;

        FetchResponseListener(
            int shardOrdinal,
            PositionMap positionMap,
            List<FetchResult> fetchResults,
            AtomicInteger remaining
        ) {
            this.shardOrdinal = shardOrdinal;
            this.positionMap = positionMap;
            this.fetchResults = fetchResults;
            this.remaining = remaining;
        }

        @Override
        public void onStreamResponse(FragmentExecutionResponse response, boolean isLast) {
            // TODO: decode response to VectorSchemaRoot and collect
            // For now, collect raw response
            fetchResults.add(new FetchResult(shardOrdinal, response));

            if (isLast && remaining.decrementAndGet() == 0) {
                assembleAndDeliver();
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("[QTF] Fetch failed for shard ordinal={}", shardOrdinal, e);
            realListener.onFailure(e);
        }

        private void assembleAndDeliver() {
            try {
                VectorSchemaRoot assembled = assembleResult(fetchResults, positionMap);
                realListener.onResponse(List.of(assembled));
            } catch (Exception e) {
                realListener.onFailure(e);
            }
        }
    }

    /**
     * Assemble fetched rows into a single result buffer in globally-sorted order.
     */
    private VectorSchemaRoot assembleResult(List<FetchResult> fetchResults, PositionMap positionMap) {
        // TODO: Full implementation — decode responses, copy rows to correct positions.
        // For now, return a placeholder that demonstrates the flow.
        //
        // Production logic:
        // 1. Allocate VectorSchemaRoot with fetchColumns schema, totalRows capacity
        // 2. For each FetchResult:
        //    - Decode to VectorSchemaRoot (batch has __row_id__ + data columns)
        //    - For each row in batch:
        //      - Read __row_id__ from the batch
        //      - Look up position: positionMap.getPosition(shardOrdinal, rowId)
        //      - Copy data columns to finalResult at that position
        // 3. Strip __row_id__ from final output
        // 4. Return

        logger.info("[QTF] Assembling {} fetch results into {} positions",
            fetchResults.size(), positionMap.totalRows());

        // Placeholder: just return the first fetch result as-is for now
        // TODO: implement proper positional assembly
        throw new UnsupportedOperationException("[QTF] Assembly not yet implemented");
    }

    // ── Supporting types ───────────────────────────────────────────────────────

    private record FetchResult(int shardOrdinal, FragmentExecutionResponse response) {}

    /**
     * Maps (shard_ordinal, row_id) → position in final output.
     * Also provides grouped row_ids per shard for fetch dispatch.
     */
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
                throw new IllegalStateException(
                    "[QTF] No position for shard=" + shard + " rowId=" + rowId
                );
            }
            return pos;
        }

        Map<Integer, long[]> getPerShardFetchPlan() {
            return perShardRowIds.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToLong(Long::longValue).toArray()
                ));
        }

        int totalRows() { return totalRows; }
        int shardCount() { return perShardRowIds.size(); }

        private static long encode(int shard, long rowId) {
            return ((long) shard << 40) | rowId;
        }
    }
}
