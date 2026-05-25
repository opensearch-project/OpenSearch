/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FetchByRowIdsRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.stage.coordinator.LocalStageTask;
import org.opensearch.analytics.exec.stage.coordinator.LocalTaskRunner;
import org.opensearch.analytics.exec.stage.coordinator.ReduceStageExecution;
import org.opensearch.analytics.exec.stage.shard.ShardFragmentStageExecution;
import org.opensearch.analytics.planner.ArrowCalciteTypes;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.spi.DataConsumer;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QTF (Query-Then-Fetch / late-materialization) Scatter-Gather stage execution.
 *
 * <p>This stage orchestrates the four QTF phases internally — there is no Substrait
 * fragment and no DataFusion compute. The stage sits above a Sort+Limit reduce stage
 * (its only child) and below the parent stage that consumes the fully-materialized
 * top-K rows.
 *
 * <pre>
 *   parent stage (Post-Sort)
 *      ▲                        ◄── feeds parent's input sink
 *      │   stitched K rows
 *   LateMaterializationStageExecution
 *      │  Phase D: stitch → emit                 (Java)
 *      │  Phase C: gather per-node fetch results (Java)
 *      │  Phase B: scatter fetch by ___ugsi      (Java + transport)
 *      │  Phase A: drain reduce output           (Java)
 *      ▲                        ◄── filled by child Sort+Limit reduce
 *      │   K rows of [sort_cols, ___row_id, ___ugsi]
 *   child stage (Sort+Limit COORDINATOR_REDUCE)
 * </pre>
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li>{@link StageExecution.State#CREATED} — built by {@code LateMaterializationStageExecutionFactory};
 *       the parent (Post-Sort) stage's start is gated on this stage's SUCCEEDED.</li>
 *   <li>{@link StageExecution.State#RUNNING} — entered when the child Sort+Limit stage
 *       SUCCEEDED. The cascade in {@code PlanWalker} fires {@link #start()}; we drain the
 *       child's output, fan out fetches, stitch, feed the parent's input sink.</li>
 *   <li>{@link StageExecution.State#SUCCEEDED} — every fetch returned, every stitched batch
 *       fed to the parent sink, parent stage can start.</li>
 * </ol>
 *
 * <h2>Implementation status</h2>
 *
 * <p><b>SKELETON ONLY.</b> Today {@link #start()} throws
 * {@link UnsupportedOperationException}. The four phases below are the work to land:
 *
 * <h3>Phase A — drain reduce output (Java)</h3>
 *
 * <p>The child stage's reduced K rows are buffered in its
 * {@link RowProducingSink}-style output (arriving via {@code feed(VSR)} on whatever
 * sink the LM stage provides via {@link #inputSink(int)}). At {@link #start()} time the
 * full input is available — block-read it.
 *
 * <pre>
 *   ExchangeSource src = (ExchangeSource) downstreamSink;   // see inputSink() below
 *   for (VectorSchemaRoot batch : src.readResult()) {
 *       // each batch has schema [sort_cols, ___row_id, ___ugsi]
 *       buffer.add(batch);  // takes ownership; close on terminal
 *   }
 * </pre>
 *
 * <h3>Phase B — scatter fetch by {@code ___ugsi} (Java + transport)</h3>
 *
 * <p>Group rows by {@code ___ugsi} ordinal. Map each ordinal → (nodeId, shardId,
 * indexUUID) using a coord-side side table built when the child stage's per-shard tasks
 * dispatched. For each {@code (nodeId, list of (___row_id, rowPos))}, send a
 * {@code FetchByRowIdRequest} via {@link #transport}. Use
 * {@code stage.getStageId()} for correlation, {@code config.parentTask()} for cancel
 * propagation.
 *
 * <p><b>New transport action required.</b> Mirror
 * {@link org.opensearch.analytics.exec.action.FragmentExecutionAction} — define
 * {@code FetchByRowIdAction}, request and response types, register on
 * {@link org.opensearch.analytics.exec.AnalyticsSearchTransportService}.
 *
 * <p><b>Data-node handler required.</b> The handler reads the columns named in the
 * wrapper's {@link OpenSearchLateMaterialization#getAboveAnchorPhysicalFields()} from each shard's
 * doc-id mapped row, returning {@code (rowPos, fetched_col_values)} batches.
 * Backend-specific: DataFusion's parquet reader can produce these from a Lucene docId
 * lookup (see {@code ShardScanInstructionHandler} for the registration pattern; the
 * fetch-by-rowid handler is a sibling, not a fragment-instruction).
 *
 * <h3>Phase C — gather (Java)</h3>
 *
 * <p>Per-node responses arrive out of order. Maintain a {@code ConcurrentHashMap<Integer
 * rowPos, FetchedRowCells>}. As each response arrives, decode batches, write per-row
 * cells keyed by {@code rowPos}. Use an {@link java.util.concurrent.atomic.AtomicInteger}
 * for in-flight count (mirrors {@link ShardFragmentStageExecution}'s pattern). When
 * count hits zero, transition to Phase D.
 *
 * <h3>Phase D — stitch and feed parent (Java)</h3>
 *
 * <p>Walk {@code rowPos = 0..K-1}, building output batches with schema
 * {@code [sort_cols, fetch_cols...]} (helper columns {@code ___row_id} + {@code ___ugsi}
 * stripped per {@link OpenSearchLateMaterialization} contract). Each row's sort cols
 * come from the buffered Phase A batches; fetched cols from the Phase C map. Feed
 * batches into {@link #parentSink}. On EOF call {@link ExchangeSink#close()} on
 * parentSink and transition to {@link StageExecution.State#SUCCEEDED}.
 *
 * <h2>Cancellation</h2>
 *
 * <p>{@link #cancel(String)} cancels the parent task — propagates to in-flight fetch
 * transports and the (already-completed) child stage. Mirrors
 * {@link ShardFragmentStageExecution#cancel(String)}.
 *
 * <h2>Cross-references</h2>
 *
 * <ul>
 *   <li>{@link OpenSearchLateMaterialization} — wrapper RelNode in the stage's fragment;
 *       carries {@code getAboveAnchorPhysicalFields()} (List&lt;RelDataTypeField&gt;) and
 *       {@code getAboveAnchorPhysicalFieldStorage()} (List&lt;FieldStorageInfo&gt;).</li>
 *   <li>{@link ShardFragmentStageExecution} — closest existing analogue for transport
 *       fan-out + per-shard response handling.</li>
 *   <li>{@link ReduceStageExecution} — closest existing analogue for "consumes child's
 *       reduce output, feeds parent."</li>
 *   <li>{@link AnalyticsSearchTransportService#dispatchFragmentStreaming} — pattern for
 *       Arrow-streaming RPC; the fetch transport is similar but with a different request
 *       type.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class LateMaterializationStageExecution extends AbstractStageExecution implements DataConsumer, DataProducer {

    private static final Logger logger = LogManager.getLogger(LateMaterializationStageExecution.class);

    private final QueryContext config;
    private final ExchangeSink parentSink;
    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService transport;

    /**
     * Sink the child Sort+Limit stage feeds into (Phase A input).
     *
     * <p>Returned by {@link #inputSink(int)}; the child stage writes K rows here. The
     * stage execution drains this sink at {@link #start()} time. Today a
     * {@link RowProducingSink} works as a buffer (it implements both {@link ExchangeSink}
     * and {@link ExchangeSource}); a custom sink may be needed if Phase A wants to
     * stream-decode rather than block-buffer.
     */
    private final RowProducingSink childInputBuffer = new RowProducingSink();

    /** Wrapper RelNode pulled from the stage's fragment — carries aboveAnchorPhysicalFields + aboveAnchorPhysicalFieldStorage. */
    private final OpenSearchLateMaterialization wrapper;

    /**
     * Stage id of the SHARD_FRAGMENT descendant whose resolved targets feed our fetch
     * dispatch. Walked once at construction; used in Phase C to look up
     * {@code config.getResolvedTargets(shardStageId)}.
     *
     * <p>HACK: this id is paired with the side-table on {@link QueryContext} to map
     * {@code ___ugsi → (DiscoveryNode, ShardId)}. See {@code QueryContext.resolvedTargetsByStage}
     * for the rationale and revisit conditions — short version: stages leaving
     * lookup state for other stages is a placeholder seam, not the long-term shape.
     */
    private final int shardStageId;

    /**
     * BackendId honored by the data node when serving fetches. Resolved by the factory
     * from the SHARD_FRAGMENT descendant's first plan alternative.
     *
     * <p>FIXME assumes single-alternative shard plan, or all alternatives agree on backendId.
     * When multi-backend per-shard plans become real the data node will need to feed back
     * which backend it picked during the query phase so the fetch goes to the same one.
     */
    private final String fetchBackendId;

    /**
     * Per-node PendingExecutions, mirroring {@code ShardTaskRunner}'s pattern. Each node
     * gets its own concurrency budget so a slow shard on one node can't block dispatches
     * to others. {@link ConcurrentHashMap} because PendingExecutions callbacks run on
     * arbitrary transport threads.
     */
    private final Map<String, PendingExecutions> pendingByNodeId = new ConcurrentHashMap<>();

    public LateMaterializationStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink parentSink,
        ClusterService clusterService,
        AnalyticsSearchTransportService transport,
        int shardStageId,
        String fetchBackendId
    ) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.config = config;
        this.parentSink = parentSink;
        this.clusterService = clusterService;
        this.transport = transport;
        this.shardStageId = shardStageId;
        this.fetchBackendId = fetchBackendId;
        this.runner = new LocalTaskRunner(config.localTaskExecutor());
        this.wrapper = RelNodeUtils.findNode(stage.getFragment(), OpenSearchLateMaterialization.class);
        if (this.wrapper == null) {
            throw new IllegalStateException(
                "LATE_MATERIALIZATION stage " + stage.getStageId() + " missing OpenSearchLateMaterialization marker in fragment"
            );
        }
        logger.info(
            "[LMStage] CREATED stageId={} aboveAnchorPhysicalFieldCount={} backendId={}",
            stage.getStageId(),
            wrapper.getAboveAnchorPhysicalFields().size(),
            fetchBackendId
        );
    }

    /**
     * Phase A input. Child stage (Sort+Limit reduce) writes its K rows here. Returned
     * sink must be the same one we drain in {@link #start()}.
     */
    @Override
    public ExchangeSink inputSink(int childStageId) {
        return childInputBuffer;
    }

    /**
     * Phase D output for the parent stage. Parent stage (Post-Sort) reads from this
     * source after we transition to SUCCEEDED.
     */
    @Override
    public ExchangeSource outputSource() {
        if (parentSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("parentSink does not implement ExchangeSource: " + parentSink.getClass().getSimpleName());
    }

    /**
     * Phase A drain. Walks the child's buffered VSRs in arrival (sort) order, extracts
     * {@code ___row_id} and {@code ___ugsi} per row, and groups into per-shard fetch plans
     * keyed by {@code ___ugsi}. Each plan carries parallel {@code long[] rowIds} +
     * {@code int[] positions} arrays where {@code position} is the row's sort-order index
     * (0..K-1) — used by the stitcher to place fetched cells back into sort order.
     *
     * <p>Order contract with the data node: {@code AnalyticsSearchService.executeFetchByRowIds}
     * requires ascending rowIds in the request and the native side returns rows in the same
     * ascending order. Phase B will sort each plan's {@code (rowIds, positions)} as a unit
     * before dispatch; Phase C then walks request and response in lockstep — no per-row
     * lookup map needed.
     *
     * <p>Phases B/C/D will follow in subsequent commits; for now Phase A populates
     * {@link #drainedRowIds} / {@link #drainedUgsis} / {@link #drainedRowCount} and closes
     * the parent sink without producing output rows so the surrounding wiring stays
     * exercised.
     */
    @Override
    protected List<StageTask> materializeTasks() {
        return List.of(new LocalStageTask(new StageTaskId(getStageId(), 0), this::drainAndClose));
    }

    /** Drained {@code ___row_id} per row, indexed by sort-order position. Populated by Phase A. */
    private long[] drainedRowIds;

    /** Drained {@code ___ugsi} per row, indexed by sort-order position. Parallel to {@link #drainedRowIds}. */
    private int[] drainedUgsis;

    /** Total rows drained from the child (= K, ≤ anchor's LIMIT). */
    private int drainedRowCount;

    /** Per-shard fetch plans keyed by {@code ___ugsi}. Built by Phase B. */
    private Map<Integer, ShardFetchPlan> plansByUgsi;

    private void drainAndClose(ActionListener<Void> listener) {
        try {
            try {
                drainAndGroupByUgsi();
            } finally {
                // Drain copied helper columns into primitive arrays; the buffered VSRs are
                // no longer needed and their query-allocator buffers must be released here.
                childInputBuffer.close();
            }
            plansByUgsi = groupByUgsi();
            scatterFetchAndStitch(listener);
            // drainAndClose returns immediately after dispatching shards; the outer
            // ActionListener fires asynchronously via Stitcher's onComplete callback when
            // the last shard's response stream terminates.
        } catch (Exception e) {
            try {
                parentSink.close();
            } catch (Exception ignore) {}
            listener.onFailure(e);
        }
    }

    /**
     * Phase C + D combined. Builds a {@link Stitcher} sized to {@link #drainedRowCount},
     * fires one async {@link FetchByRowIdsRequest} per shard via
     * {@link AnalyticsSearchTransportService#dispatchFetchByRowIds}, and lets the per-shard
     * {@link GatherListener}s feed batches into the stitcher as they arrive. The stitcher
     * triggers {@code outerListener.onResponse} (or {@code onFailure}) when the last shard
     * completes — no blocking on this thread.
     */
    private void scatterFetchAndStitch(ActionListener<Void> outerListener) throws Exception {
        if (drainedRowCount == 0) {
            // Empty K: no shards to dispatch, nothing to emit. Match "K=0 short-circuit"
            // from the master doc's deferred-questions list.
            parentSink.close();
            outerListener.onResponse(null);
            return;
        }

        // Output schema: aboveAnchorPhysicalFields converted to Arrow Fields. ___row_id is
        // NOT included — it's a helper consumed during stitch, never surfaced.
        List<RelDataTypeField> aboveFields = wrapper.getAboveAnchorPhysicalFields();
        List<Field> outputFields = new ArrayList<>(aboveFields.size());
        for (RelDataTypeField f : aboveFields) {
            outputFields.add(Field.nullable(f.getName(), ArrowCalciteTypes.toArrow(f.getType())));
        }

        // Fetch projection sent to the data node: ___row_id (required by the data-node
        // contract — see AnalyticsSearchService.executeFetchByRowIds) + the physical fields
        // the user wants displayed.
        String[] columns = new String[aboveFields.size() + 1];
        columns[0] = OpenSearchLateMaterialization.ROW_ID_FIELD;
        for (int i = 0; i < aboveFields.size(); i++) {
            columns[i + 1] = aboveFields.get(i).getName();
        }

        Map<Integer, ShardExecutionTarget> targetsByUgsi = config.getResolvedTargets(shardStageId);
        if (targetsByUgsi == null) {
            throw new IllegalStateException(
                "No resolved targets for shardStageId=" + shardStageId + " — shard fragment stage didn't record targets"
            );
        }

        Stitcher stitcher = new Stitcher(config.bufferAllocator(), outputFields, drainedRowCount, plansByUgsi.size(), parentSink, () -> {
            Exception failure = this.stitcher.surfaceableFailure();
            if (failure == null) {
                outerListener.onResponse(null);
            } else {
                outerListener.onFailure(failure);
            }
        });
        this.stitcher = stitcher;

        for (Map.Entry<Integer, ShardFetchPlan> entry : plansByUgsi.entrySet()) {
            int ugsi = entry.getKey();
            ShardFetchPlan plan = entry.getValue();
            ShardExecutionTarget target = targetsByUgsi.get(ugsi);
            if (target == null) {
                stitcher.shardFailed(new IllegalStateException("No resolved target for ugsi=" + ugsi));
                continue;
            }
            FetchByRowIdsRequest request = new FetchByRowIdsRequest(
                config.queryId(),
                stage.getStageId(),
                target.shardId(),
                fetchBackendId,
                plan.rowIds(),
                columns
            );
            // Per-node PendingExecutions: mirrors ShardTaskRunner — keeps a slow node from
            // blocking dispatches to other nodes.
            PendingExecutions pending = pendingByNodeId.computeIfAbsent(
                target.node().getId(),
                n -> new PendingExecutions(config.maxConcurrentShardRequests())
            );
            transport.dispatchFetchByRowIds(request, target.node(), new GatherListener(stitcher, plan), config.parentTask(), pending);
        }
    }

    /** Stashed for the {@code onComplete} closure to read {@link Stitcher#surfaceableFailure()}. */
    private Stitcher stitcher;

    /**
     * Per-shard streaming response listener: forwards each Arrow batch to the
     * {@link Stitcher} (which copies cells into the output VSR at the row's sort-order
     * position), tracks {@code rowsCopiedSoFar} across batches, and signals shard
     * completion / failure to the stitcher's countdown.
     */
    private static final class GatherListener implements StreamingResponseListener<FragmentExecutionArrowResponse> {
        private final Stitcher stitcher;
        private final ShardFetchPlan plan;
        private int rowsCopiedSoFar;

        GatherListener(Stitcher stitcher, ShardFetchPlan plan) {
            this.stitcher = stitcher;
            this.plan = plan;
        }

        @Override
        public void onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
            VectorSchemaRoot batch = response.getRoot();
            try {
                stitcher.acceptBatch(batch, plan.positions(), rowsCopiedSoFar);
                rowsCopiedSoFar += batch.getRowCount();
            } catch (Exception e) {
                stitcher.shardFailed(e);
                return;
            } finally {
                // Stitcher.acceptBatch only reads + copyFromSafe; ownership of the response
                // batch's query-allocator buffers stays with this listener.
                if (batch != null) batch.close();
            }
            if (isLast) stitcher.shardComplete();
        }

        @Override
        public void onFailure(Exception e) {
            stitcher.shardFailed(e);
        }
    }

    /**
     * Reads K rows of {@code [reduce-set, ___row_id, ___ugsi]} from the child input
     * batches (already in sort order) and copies the helper columns into two parallel
     * primitive arrays indexed by sort-order position. Reduce-set columns are discarded.
     *
     * <p>Pre-sizes the arrays via {@link RowProducingSink#getRowCount()} — the sink
     * maintains {@code totalRows} incrementally on each {@code feed()}, so this is O(1)
     * and exact. No need to walk the batches twice.
     *
     * <p>Despite the name, the actual partition-by-{@code ___ugsi} into per-shard fetch
     * plans is Phase B's job; here we only flatten the helper columns into the two
     * parallel arrays Phase B will then walk to build its per-shard plans.
     */
    private void drainAndGroupByUgsi() {
        long total = childInputBuffer.getRowCount();
        if (total > Integer.MAX_VALUE) {
            throw new IllegalStateException("Drained row count exceeds Integer.MAX_VALUE: " + total);
        }
        int K = (int) total;
        drainedRowIds = new long[K];
        drainedUgsis = new int[K];

        int position = 0;
        for (VectorSchemaRoot vsr : childInputBuffer.readResult()) {
            int rowIdIdx = vsr.getSchema().getFields().indexOf(vsr.getSchema().findField(OpenSearchLateMaterialization.ROW_ID_FIELD));
            int ugsiIdx = vsr.getSchema().getFields().indexOf(vsr.getSchema().findField(OpenSearchLateMaterialization.UGSI_FIELD));
            if (rowIdIdx < 0 || ugsiIdx < 0) {
                throw new IllegalStateException(
                    "LM stage drain expected ["
                        + OpenSearchLateMaterialization.ROW_ID_FIELD
                        + ", "
                        + OpenSearchLateMaterialization.UGSI_FIELD
                        + "] in batch schema; got "
                        + vsr.getSchema()
                );
            }
            BigIntVector rowIds = (BigIntVector) vsr.getVector(rowIdIdx);
            IntVector ugsis = (IntVector) vsr.getVector(ugsiIdx);
            int rows = vsr.getRowCount();
            for (int i = 0; i < rows; i++) {
                drainedRowIds[position] = rowIds.get(i);
                drainedUgsis[position] = ugsis.get(i);
                position++;
            }
        }
        drainedRowCount = position;
        logger.debug("[LMStage] phase-A stageId={} drainedRows={}", getStageId(), drainedRowCount);
    }

    /**
     * Phase B. Partitions the drained flat arrays by {@code ___ugsi} into one
     * {@link ShardFetchPlan} per shard. Each plan's parallel {@code rowIds} and
     * {@code positions} arrays are sorted by {@code rowId} ascending — required by the
     * data node's fetch contract (ascending input rowIds, ascending response order).
     *
     * <p>Two passes over the flat arrays: one to count rows per shard so we can
     * allocate exactly-sized per-shard arrays, one to fill them. No per-row boxing.
     */
    private Map<Integer, ShardFetchPlan> groupByUgsi() {
        Map<Integer, Integer> sizesByUgsi = new HashMap<>();
        for (int i = 0; i < drainedRowCount; i++) {
            sizesByUgsi.merge(drainedUgsis[i], 1, Integer::sum);
        }
        Map<Integer, long[]> rowIdsByUgsi = new HashMap<>(sizesByUgsi.size());
        Map<Integer, int[]> positionsByUgsi = new HashMap<>(sizesByUgsi.size());
        Map<Integer, Integer> cursorByUgsi = new HashMap<>(sizesByUgsi.size());
        for (Map.Entry<Integer, Integer> e : sizesByUgsi.entrySet()) {
            rowIdsByUgsi.put(e.getKey(), new long[e.getValue()]);
            positionsByUgsi.put(e.getKey(), new int[e.getValue()]);
            cursorByUgsi.put(e.getKey(), 0);
        }
        for (int i = 0; i < drainedRowCount; i++) {
            int ugsi = drainedUgsis[i];
            int cur = cursorByUgsi.get(ugsi);
            rowIdsByUgsi.get(ugsi)[cur] = drainedRowIds[i];
            positionsByUgsi.get(ugsi)[cur] = i;
            cursorByUgsi.put(ugsi, cur + 1);
        }
        Map<Integer, ShardFetchPlan> plans = new HashMap<>(sizesByUgsi.size());
        for (Integer ugsi : sizesByUgsi.keySet()) {
            long[] rowIds = rowIdsByUgsi.get(ugsi);
            int[] positions = positionsByUgsi.get(ugsi);
            sortParallelByRowIdAscending(rowIds, positions);
            plans.put(ugsi, new ShardFetchPlan(ugsi, rowIds, positions));
        }
        logger.debug("[LMStage] phase-B stageId={} shards={}", getStageId(), plans.size());
        return plans;
    }

    /**
     * Sorts {@code rowIds} ascending in place, permuting {@code positions} along.
     * Indirect sort via an index array — ~O(N log N) with bounded extra allocation; fine
     * for typical per-shard N (= K/numShards ≤ a few thousand).
     */
    static void sortParallelByRowIdAscending(long[] rowIds, int[] positions) {
        int n = rowIds.length;
        if (n <= 1) return;
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++)
            order[i] = i;
        java.util.Arrays.sort(order, (a, b) -> Long.compare(rowIds[a], rowIds[b]));
        long[] sortedRowIds = new long[n];
        int[] sortedPositions = new int[n];
        for (int i = 0; i < n; i++) {
            sortedRowIds[i] = rowIds[order[i]];
            sortedPositions[i] = positions[order[i]];
        }
        System.arraycopy(sortedRowIds, 0, rowIds, 0, n);
        System.arraycopy(sortedPositions, 0, positions, 0, n);
    }

    /**
     * Per-shard fetch plan: {@code rowIds} sorted ascending, {@code positions[j]} the
     * sort-order position in the LM stage's drained-row index for {@code rowIds[j]}.
     * Phase C dispatches {@code rowIds} to shard {@code ugsi}'s data node; the response
     * arrives in the same ascending order, and Phase D writes each response cell into
     * {@code stitched[positions[j]]}.
     */
    record ShardFetchPlan(int ugsi, long[] rowIds, int[] positions) {
    }
}
