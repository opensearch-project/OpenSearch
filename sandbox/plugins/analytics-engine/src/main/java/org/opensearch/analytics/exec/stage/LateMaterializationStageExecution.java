/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.spi.DataConsumer;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;

import java.util.List;

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
 * wrapper's {@link OpenSearchLateMaterialization#getFetchList()} from each shard's
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
 *       carries {@code getFetchList()} (List&lt;RelDataTypeField&gt;) and
 *       {@code getFetchListStorage()} (List&lt;FieldStorageInfo&gt;).</li>
 *   <li>{@link ShardFragmentStageExecution} — closest existing analogue for transport
 *       fan-out + per-shard response handling.</li>
 *   <li>{@link LocalStageExecution} — closest existing analogue for "consumes child's
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

    /** Wrapper RelNode pulled from the stage's fragment — carries fetchList + fetchListStorage. */
    private final OpenSearchLateMaterialization wrapper;

    public LateMaterializationStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink parentSink,
        ClusterService clusterService,
        AnalyticsSearchTransportService transport
    ) {
        super(stage, config.queryId(), config.operationListeners(), config.parentTask());
        this.config = config;
        this.parentSink = parentSink;
        this.clusterService = clusterService;
        this.transport = transport;
        this.wrapper = RelNodeUtils.findNode(stage.getFragment(), OpenSearchLateMaterialization.class);
        if (this.wrapper == null) {
            throw new IllegalStateException(
                "LATE_MATERIALIZATION stage "
                    + stage.getStageId()
                    + " missing OpenSearchLateMaterialization marker in fragment — DAGBuilder bug"
            );
        }
        logger.info(
            "[LMStage] CREATED stageId={} fetchListSize={} childCount={}",
            stage.getStageId(),
            wrapper.getFetchList().size(),
            stage.getChildStages().size()
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
     * Skeleton: returns an empty task list so the base template auto-transitions to SUCCEEDED.
     * QTF Phases A-D will replace this with real tasks that drain {@link #childInputBuffer},
     * scatter-fetch by {@code ___ugsi}, gather, and stitch into {@link #parentSink}.
     *
     * <p>The empty-task short-circuit means a real QTF query routed through this stage today
     * produces no rows upstream — wiring is exercised, semantics are not.
     */
    @Override
    protected List<StageTask> materializeTasks() {
        return List.of();
    }
}
