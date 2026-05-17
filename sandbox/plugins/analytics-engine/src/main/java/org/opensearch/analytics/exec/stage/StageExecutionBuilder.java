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
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.RowProducingSink;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.DataConsumer;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds {@link StageExecution} instances for the walker. Resolves the
 * row-data output sink from each stage's parent via the {@link DataConsumer}
 * contract, then delegates to the factory registered for the stage's
 * {@link StageExecutionType} to construct the concrete execution.
 *
 * <p>{@code QueryExecution} never sees factories or output targets. It calls
 * {@link #buildRootExecution} for the root stage and {@link #buildExecution}
 * for every child. Both methods delegate to the same internal dispatch logic;
 * the only difference is where the output sink comes from (fresh for root,
 * parent-provided for children).
 *
 * <p>Shuffle-write and broadcast-write are not yet modeled. When they land
 * they will add new {@link StageExecutionType} values with their own factory
 * registrations and (most likely) a separate manifest-receiver contract.
 *
 * @opensearch.internal
 */
public class StageExecutionBuilder {

    private static final Logger logger = LogManager.getLogger(StageExecutionBuilder.class);

    private final Map<StageExecutionType, StageExecutionFactory> factories;

    /**
     * Guice-injected constructor. Registers default factories for every value
     * of {@link StageExecutionType}.
     */
    @Inject
    public StageExecutionBuilder(ClusterService clusterService, AnalyticsSearchTransportService dispatcher) {
        this.factories = new HashMap<>();
        registerFactory(StageExecutionType.SHARD_FRAGMENT, new ShardFragmentStageExecutionFactory(clusterService, dispatcher));
        registerFactory(StageExecutionType.COORDINATOR_REDUCE, new LocalStageExecutionFactory());
        registerFactory(StageExecutionType.LOCAL_PASSTHROUGH, (stage, sink, config) -> new PassThroughStageExecution(stage, config, sink));
        registerFactory(StageExecutionType.LOCAL_COMPUTE, new LocalComputeStageExecutionFactory());
    }

    /**
     * Registers a factory for a stage execution type, replacing any prior registration.
     * Returns the previously-registered factory for {@code type}, or {@code null} —
     * useful for tests that swap in a faulting factory and restore on teardown.
     */
    public StageExecutionFactory registerFactory(StageExecutionType type, StageExecutionFactory factory) {
        return factories.put(type, factory);
    }

    /**
     * Builds the root stage's execution. The root accumulates into a fresh
     * {@link RowProducingSink}; the query execution reads the final result via
     * the stage's {@code outputSource()} contract — the root must therefore
     * implement {@link DataProducer}, enforced here as a fail-fast invariant
     * rather than a runtime cast at every terminal listener fire.
     */
    public StageExecution buildRootExecution(Stage rootStage, QueryContext config) {
        // TODO: Update to read directly from back-end provided ExchangeSource when the root stage has a fragment
        StageExecution rootExec = buildStageExecution(rootStage, new RowProducingSink(), config);
        if ((rootExec instanceof DataProducer) == false) {
            throw new IllegalStateException(
                "Root execution "
                    + rootExec.getClass().getSimpleName()
                    + " (stage "
                    + rootStage.getStageId()
                    + ") does not implement DataProducer"
            );
        }
        return rootExec;
    }

    /**
     * Builds a child stage's execution by dispatching to the factory registered
     * for the stage's {@link StageExecutionType}. All stage types today are
     * row-producing — the sink is resolved from {@code parentExec}'s
     * {@link DataConsumer} contract.
     *
     * @throws IllegalStateException if {@code parentExec} does not implement
     *     {@link DataConsumer} — this is a planner bug.
     */
    public StageExecution buildExecution(Stage stage, StageExecution parentExec, QueryContext config) {
        ExchangeSink sink = switch (stage.getExecutionType()) {
            case SHARD_FRAGMENT, COORDINATOR_REDUCE, LOCAL_PASSTHROUGH, LOCAL_COMPUTE -> resolveRowSink(stage, parentExec);
        };
        return buildStageExecution(stage, sink, config);
    }

    // ── Internal dispatch ───────────────────────────────────────────────

    private StageExecution buildStageExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        StageExecutionFactory factory = factories.get(stage.getExecutionType());
        if (factory == null) {
            throw new IllegalStateException(
                "No factory registered for execution type " + stage.getExecutionType() + " — stage " + stage.getStageId()
            );
        }
        return factory.createExecution(stage, sink, config);
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
