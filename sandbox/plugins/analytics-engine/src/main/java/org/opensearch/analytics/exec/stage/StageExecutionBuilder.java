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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds {@link StageExecution} instances for the walker. Resolves the
 * row-data output sink from each stage's parent via the {@link DataConsumer}
 * contract, then delegates to the scheduler registered for the stage's
 * {@link StageExecutionType} to construct the concrete execution.
 *
 * <p>{@code PlanWalker} never sees schedulers or output targets. It calls
 * {@link #buildRootExecution} for the root stage and {@link #buildExecution}
 * for every child. Both methods delegate to the same internal dispatch logic;
 * the only difference is where the output sink comes from (fresh for root,
 * parent-provided for children).
 *
 * <p>Shuffle-write and broadcast-write are not yet modeled. When they land
 * they will add new {@link StageExecutionType} values with their own scheduler
 * registrations and (most likely) a separate manifest-receiver contract.
 *
 * @opensearch.internal
 */
public class StageExecutionBuilder {

    private static final Logger logger = LogManager.getLogger(StageExecutionBuilder.class);

    private final Map<StageExecutionType, StageScheduler> schedulers;

    /**
     * Guice-injected constructor. Registers default schedulers for every value
     * of {@link StageExecutionType}.
     */
    @Inject
    public StageExecutionBuilder(ClusterService clusterService, AnalyticsSearchTransportService dispatcher) {
        this.schedulers = new HashMap<>();
        registerScheduler(StageExecutionType.SHARD_FRAGMENT, new ShardFragmentStageScheduler(clusterService, dispatcher));
        registerScheduler(StageExecutionType.COORDINATOR_REDUCE, new LocalStageScheduler());
        registerScheduler(StageExecutionType.LOCAL_PASSTHROUGH, (stage, sink, config) -> new PassThroughStageExecution(stage, sink));
    }

    /**
     * Registers a scheduler for a stage execution type. Enables adding new
     * stage types without modifying this class's constructor.
     */
    public void registerScheduler(StageExecutionType type, StageScheduler scheduler) {
        schedulers.put(type, scheduler);
    }

    /**
     * Builds the root stage's execution. The root accumulates into a fresh
     * {@link RowProducingSink}; the walker reads the final result via the
     * stage's {@code outputSource()} contract.
     */
    public StageExecution buildRootExecution(Stage rootStage, QueryContext config) {
        // TODO: Update to read directly from back-end provided ExchangeSource when the root stage has a fragment
        return buildStageExecution(rootStage, new RowProducingSink(), config);
    }

    /**
     * Builds a child stage's execution by dispatching to the scheduler registered
     * for the stage's {@link StageExecutionType}. All stage types today are
     * row-producing — the sink is resolved from {@code parentExec}'s
     * {@link DataConsumer} contract.
     *
     * @throws IllegalStateException if {@code parentExec} does not implement
     *     {@link DataConsumer} — this is a planner bug.
     */
    public StageExecution buildExecution(Stage stage, StageExecution parentExec, QueryContext config) {
        ExchangeSink sink = switch (stage.getExecutionType()) {
            case SHARD_FRAGMENT, COORDINATOR_REDUCE, LOCAL_PASSTHROUGH -> resolveRowSink(stage, parentExec);
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
     * must implement {@link DataConsumer} to accept child input. The child
     * stage writes its output into the returned sink.
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
