/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.calcite.rel.RelDistribution;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds {@link StageExecution} instances for the walker. Owns two orthogonal
 * responsibilities:
 *
 * <ol>
 *   <li><b>Output classification.</b> Decides what kind of output a stage
 *       produces — row data (for scans, filters, aggregates, local compute)
 *       versus manifest/metadata (for shuffle-write, broadcast-write). Each
 *       output shape corresponds to a different scheduler type with a
 *       different {@code createExecution} signature.</li>
 *   <li><b>Output resolution.</b> For row-producing stages, resolves the
 *       {@link ExchangeSink} from the parent's
 *       {@link DataConsumer} contract. For manifest-producing
 *       stages (future), it will resolve a manifest receiver from the parent's
 *       corresponding contract.</li>
 * </ol>
 *
 * <p>{@code PlanWalker} never sees schedulers or output targets. It calls
 * {@link #buildRootExecution} for the root stage and
 * {@link #buildExecution} for every child. Both methods delegate to the
 * same internal dispatch logic; the only difference is where the output
 * sink comes from (fresh for root, parent-provided for children).
 *
 * <p><b>Branching in {@link #buildExecution}</b> follows most-specific first:
 * <ol>
 *   <li>Shuffle-write: {@code exchange.distributionType() == HASH_DISTRIBUTED}
 *       → manifest output → shuffle-write scheduler <i>(TODO)</i></li>
 *   <li>Broadcast-write: {@code exchange.distributionType() == BROADCAST_DISTRIBUTED}
 *       → manifest output → broadcast-write scheduler <i>(TODO)</i></li>
 *   <li>LOCAL: row output → {@link LocalStageScheduler}</li>
 *   <li>DATA_NODE (fallback): row output → {@link ShardFragmentStageScheduler}</li>
 * </ol>
 *
 * <p>When shuffle/broadcast schedulers are reintroduced, each arm becomes a
 * two-line delegation — no other file needs to change. The row branches, the
 * walker, and the row schedulers are untouched.
 *
 * @opensearch.internal
 */
public class StageExecutionBuilder {

    private static final Logger logger = LogManager.getLogger(StageExecutionBuilder.class);

    private final Map<StageExecutionType, StageScheduler> schedulers;

    /**
     * Guice-injected constructor. Registers the default schedulers for
     * {@link StageExecutionType#LOCAL} and {@link StageExecutionType#DATA_NODE}.
     * Additional stage types (shuffle, broadcast) can be registered via
     * {@link #registerScheduler}.
     */
    @Inject
    public StageExecutionBuilder(
        ClusterService clusterService,
        AnalyticsSearchTransportService dispatcher,
        Map<String, AnalyticsSearchBackendPlugin> backends
    ) {
        this.schedulers = new HashMap<>();
        schedulers.put(StageExecutionType.LOCAL, new LocalStageScheduler(backends));
        schedulers.put(StageExecutionType.DATA_NODE, new ShardFragmentStageScheduler(clusterService, dispatcher));
    }

    /**
     * Registers a scheduler for a stage execution type. Enables extending
     * the builder with new stage types (shuffle-write, broadcast-write)
     * without modifying this class.
     */
    public void registerScheduler(StageExecutionType type, StageScheduler scheduler) {
        schedulers.put(type, scheduler);
    }

    /**
     * Builds the root stage's execution. Creates a fresh {@link ExchangeSink}
     * internally — the walker doesn't see it. The walker reads the final
     * result by casting the returned execution to
     * {@link DataProducer} and calling
     * {@code outputSink().readResult()}.
     */
    public StageExecution buildRootExecution(Stage rootStage, QueryContext config) {
        org.opensearch.analytics.exec.RowProducingSink rootSink = new org.opensearch.analytics.exec.RowProducingSink();
        return dispatchRowStage(rootStage, rootSink, config);
    }

    /**
     * Builds a child stage's execution. Resolves the child's output target
     * from {@code parentExec}.
     *
     * <p>Classifies {@code stage} by its {@link ExchangeInfo} to decide the
     * output shape (row vs. manifest), resolves the appropriate output target
     * from {@code parentExec}, then delegates to the scheduler for that
     * (output-shape, execution-type) combination.
     *
     * @throws IllegalStateException if a row-producing stage's
     *     {@code parentExec} does not implement
     *     {@link DataConsumer} — this is a planner bug.
     * @throws UnsupportedOperationException if {@code stage} produces a
     *     manifest (shuffle-write or broadcast-write) — not yet implemented.
     */
    public StageExecution buildExecution(Stage stage, StageExecution parentExec, QueryContext config) {
        ExchangeInfo exchange = stage.getExchangeInfo();

        // ── Manifest-producing branches (structural placeholders) ──────
        if (exchange != null && exchange.distributionType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            throw new UnsupportedOperationException(
                "shuffle-write (HASH_DISTRIBUTED) scheduler not yet implemented — stage " + stage.getStageId()
            );
        }
        if (exchange != null && exchange.distributionType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
            throw new UnsupportedOperationException(
                "broadcast-write (BROADCAST_DISTRIBUTED) scheduler not yet implemented — stage " + stage.getStageId()
            );
        }

        // ── Row-producing branch ───────────────────────────────────────
        ExchangeSink sink = resolveRowSink(stage, parentExec);
        return dispatchRowStage(stage, sink, config);
    }

    // ── Internal dispatch ───────────────────────────────────────────────

    private StageExecution dispatchRowStage(Stage stage, ExchangeSink sink, QueryContext config) {
        StageScheduler scheduler = schedulers.get(stage.getExecutionType());
        if (scheduler == null) {
            throw new IllegalStateException(
                "No scheduler registered for execution type " + stage.getExecutionType()
                    + " — stage " + stage.getStageId()
            );
        }
        return scheduler.createExecution(stage, sink, config);
    }

    /**
     * Resolves the output sink for a child stage from its parent. The parent
     * must implement {@link DataConsumer} to accept child input. The returned
     * sink is the same object the child will write into via
     * {@link DataProducer#outputSink()}.
     */
    private static ExchangeSink resolveRowSink(Stage stage, StageExecution parentExec) {
        if (parentExec instanceof DataConsumer consumer) {
            return consumer.inputSink(stage.getStageId());
        }
        throw new IllegalStateException(
            "row-producing stage "
                + stage.getStageId()
                + " has parent "
                + parentExec.getClass().getSimpleName()
                + " which does not accept child input (does not implement DataConsumer)"
        );
    }
}
