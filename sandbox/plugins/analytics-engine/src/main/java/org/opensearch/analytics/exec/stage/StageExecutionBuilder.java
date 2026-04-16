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
import org.opensearch.analytics.backend.ExchangeSink;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;

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
 *       {@link SinkProvidingStageExecution} contract. For manifest-producing
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
 *   <li>DATA_NODE (fallback): row output → {@link ShardScanStageScheduler}</li>
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

    private final LocalStageScheduler localScheduler;
    private final ShardScanStageScheduler shardFanOutScheduler;

    /**
     * Guice-injected constructor. {@code backends} maps backend name →
     * {@link AnalyticsSearchBackendPlugin} instance. It may be empty in test
     * environments that don't register any backend plugins (e.g.
     * {@code MockTransportService}-based ITs). An empty map propagates to
     * {@link LocalStageScheduler}, which fast-fails with a clear
     * {@link IllegalStateException} if a compute LOCAL stage is dispatched.
     * Production clusters always have at least one backend, so the empty-map
     * path only fires in tests.
     */
    @Inject
    public StageExecutionBuilder(
        ClusterService clusterService,
        AnalyticsSearchTransportService dispatcher,
        Map<String, AnalyticsSearchBackendPlugin> backends
    ) {
        this.localScheduler = new LocalStageScheduler(backends);
        this.shardFanOutScheduler = new ShardScanStageScheduler(clusterService, dispatcher);
    }

    /**
     * Builds the root stage's execution. Creates a fresh {@link ExchangeSink}
     * internally — the walker doesn't see it. The walker reads the final
     * result by casting the returned execution to
     * {@link SinkProvidingStageExecution} and calling
     * {@code sink().readResult()}.
     */
    public StageExecution buildRootExecution(Stage rootStage, QueryContext config) {
        ExchangeSink rootSink = new org.opensearch.analytics.exec.RowProducingSink();
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
     *     {@link SinkProvidingStageExecution} — this is a planner bug.
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
        if (stage.getExecutionType() == StageExecutionType.LOCAL) {
            return localScheduler.createExecution(stage, sink, config);
        }
        return shardFanOutScheduler.createExecution(stage, sink, config);
    }

    private static ExchangeSink resolveRowSink(Stage stage, StageExecution parentExec) {
        if (parentExec instanceof SinkProvidingStageExecution exec) {
            return exec.sink(stage.getStageId());
        }
        throw new IllegalStateException(
            "row-producing stage "
                + stage.getStageId()
                + " has parent "
                + parentExec.getClass().getSimpleName()
                + " which does not receive row input"
        );
    }
}
