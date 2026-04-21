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
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.spi.DataConsumer;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
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
     * Guice-injected constructor. Registers default schedulers for every value of
     * {@link StageExecutionType}. Shuffle-write and broadcast-write are registered
     * as NYI throw-lambdas until their real schedulers land — keeping the
     * unsupported-type contract inside the registry rather than scattered as
     * inline checks in {@link #buildExecution}.
     */
    @Inject
    public StageExecutionBuilder(
        ClusterService clusterService,
        AnalyticsSearchTransportService dispatcher,
        Map<String, AnalyticsSearchBackendPlugin> backends
    ) {
        this.schedulers = new HashMap<>();
        registerScheduler(StageExecutionType.SHARD_FRAGMENT, new ShardFragmentStageScheduler(clusterService, dispatcher));
        registerScheduler(StageExecutionType.COORDINATOR_REDUCE, new LocalStageScheduler(backends));
        registerScheduler(StageExecutionType.LOCAL_PASSTHROUGH, (stage, sink, config) -> new PassThroughStageExecution(stage, sink));
        registerScheduler(StageExecutionType.SHUFFLE_WRITE, (stage, sink, config) -> {
            throw new UnsupportedOperationException("SHUFFLE_WRITE scheduler not yet implemented — stage " + stage.getStageId());
        });
        registerScheduler(StageExecutionType.BROADCAST_WRITE, (stage, sink, config) -> {
            throw new UnsupportedOperationException("BROADCAST_WRITE scheduler not yet implemented — stage " + stage.getStageId());
        });
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
        return buildStageExecution(rootStage, new RowProducingSink(), config);
    }

    /**
     * Builds a child stage's execution by dispatching to the scheduler registered
     * for the stage's {@link StageExecutionType}. For row-producing stages
     * (SHARD_SCAN, COORDINATOR_REDUCE, LOCAL_PASSTHROUGH) the sink is resolved
     * from {@code parentExec}'s {@link DataConsumer} contract. Manifest-producing
     * stages (SHUFFLE_WRITE, BROADCAST_WRITE) get a null sink — TODO: they will wire to
     * their parent through a separate contract that the scheduler owns.
     *
     * @throws IllegalStateException if a row-producing stage's {@code parentExec}
     *     does not implement {@link DataConsumer} — this is a planner bug.
     * @throws UnsupportedOperationException if the registered scheduler is a
     *     NYI placeholder (shuffle-write, broadcast-write at present).
     */
    public StageExecution buildExecution(Stage stage, StageExecution parentExec, QueryContext config) {
        ExchangeSink sink = switch (stage.getExecutionType()) {
            case SHARD_FRAGMENT, COORDINATOR_REDUCE, LOCAL_PASSTHROUGH -> resolveRowSink(stage, parentExec);
            case SHUFFLE_WRITE, BROADCAST_WRITE -> null;
        };
        return buildStageExecution(stage, sink, config);
    }

    // ── Internal dispatch ───────────────────────────────────────────────

    private StageExecution buildStageExecution(Stage stage, ExchangeSink sink, QueryContext config) {
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
