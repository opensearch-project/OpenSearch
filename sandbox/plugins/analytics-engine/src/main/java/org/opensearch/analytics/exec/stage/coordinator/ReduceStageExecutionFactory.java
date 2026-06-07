/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionFactory;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.analytics.settings.PartitionLanePolicy;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ReducingExchangeSink;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds executions for {@link StageExecutionType#COORDINATOR_REDUCE} stages —
 * those that run at the coordinator with a backend-provided {@link ReducingExchangeSink}.
 * Creates the sink via {@link Stage#getExchangeSinkProvider()} using an
 * {@link ExchangeSinkContext} carrying the plan bytes, allocator, per-child
 * input descriptors (one per child stage, each with its stage id + the
 * producer-side plan bytes the backend lowers to derive the input schema),
 * and the downstream sink. Wraps the resulting sink in a {@link ReduceStageExecution};
 * the sink's {@link ReducingExchangeSink#supportsEagerScheduling()} drives the
 * scheduling mode (streaming vs buffered) without needing distinct execution subclasses.
 *
 * @opensearch.internal
 */
public final class ReduceStageExecutionFactory implements StageExecutionFactory {

    private static final Logger logger = LogManager.getLogger(ReduceStageExecutionFactory.class);

    /**
     * Cluster state source used to size each child's input parallelism (one
     * {@link org.opensearch.analytics.spi.ExchangeSinkContext.ChildInput#numInputPartitions()
     * numInputPartitions} per resolved {@link ExecutionTarget}, modulo the configured
     * {@link PartitionLanePolicy}). May be {@code null} in unit-test contexts that don't
     * construct a {@code ClusterService} — the factory falls back to a single-lane input
     * and the backend treats the child as if it had one producer.
     */
    private final ClusterService clusterService;

    /**
     * Live policy controlling how a child's resolved producer count maps to the lane
     * count we declare on the {@link org.opensearch.analytics.spi.ExchangeSinkContext.ChildInput}.
     * Updated atomically when
     * {@link AnalyticsQuerySettings#REDUCE_PARTITION_LANE_POLICY} changes; reads
     * are volatile so a setting update mid-flight is visible on the next reduce stage.
     */
    private volatile PartitionLanePolicy lanePolicy = PartitionLanePolicy.PER_SHARD;

    /** Default constructor (test-only): no cluster state, every child input is sized to 1 lane. */
    public ReduceStageExecutionFactory() {
        this(null);
    }

    public ReduceStageExecutionFactory(ClusterService clusterService) {
        this.clusterService = clusterService;
        if (clusterService != null) {
            // Pick up the configured policy from initial settings, then watch for updates.
            this.lanePolicy = PartitionLanePolicy.parse(
                AnalyticsQuerySettings.REDUCE_PARTITION_LANE_POLICY.get(clusterService.getSettings())
            );
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    AnalyticsQuerySettings.REDUCE_PARTITION_LANE_POLICY,
                    raw -> this.lanePolicy = PartitionLanePolicy.parse(raw)
                );
        }
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        ExchangeSinkProvider provider = stage.getExchangeSinkProvider();
        ExchangeSinkContext context = new ExchangeSinkContext(
            config.queryId(),
            stage.getStageId(),
            config.parentTask() != null ? config.parentTask().getId() : 0L,
            chosenBytes(stage),
            config.bufferAllocator(),
            buildChildInputs(stage),
            sink
        );

        // Apply instruction handlers for the reduce stage.
        // Unlike AnalyticsSearchService (shard path) which resolves the factory from its
        // local backends map, the coordinator-reduce path has no backends map — the factory
        // is stored on the Stage during FragmentConversionDriver.convertAll (root stage only,
        // no serialization needed since reduce executes locally at the coordinator).
        // TODO: find a cleaner way to provide the factory without storing it on Stage.
        BackendExecutionContext backendContext = null;
        FragmentInstructionHandlerFactory factory = stage.getInstructionHandlerFactory();
        if (factory != null) {
            Throwable primaryFailure = null;
            try {
                for (InstructionNode node : stage.getPlanAlternatives().getFirst().instructions()) {
                    FragmentInstructionHandler handler = factory.createHandler(node);
                    BackendExecutionContext previous = backendContext;
                    backendContext = handler.apply(node, context, backendContext);
                    // A handler that returns a new reference implicitly abandons the previous
                    // context — close it now so its resources aren't orphaned.
                    if (previous != null && previous != backendContext) {
                        previous.close();
                    }
                }
            } catch (Throwable t) {
                primaryFailure = t;
                // On failure, close the backendContext since it won't be handed to the sink.
                if (backendContext != null) {
                    try {
                        backendContext.close();
                    } catch (Exception closeFailure) {
                        primaryFailure.addSuppressed(closeFailure);
                    }
                }
            }
            if (primaryFailure != null) {
                if (primaryFailure instanceof RuntimeException re) throw re;
                if (primaryFailure instanceof Error err) throw err;
                throw new RuntimeException("Instruction handler failed for stageId=" + stage.getStageId(), primaryFailure);
            }
        }

        ExchangeSink backendSink;
        try {
            backendSink = provider.createSink(context, backendContext);
        } catch (Exception e) {
            // Sink creation failed — close backendContext to avoid resource leak.
            if (backendContext != null) {
                try {
                    backendContext.close();
                } catch (Exception closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw new RuntimeException("Failed to create exchange sink for stageId=" + stage.getStageId(), e);
        }
        if (backendSink instanceof ReducingExchangeSink reducing) {
            return new ReduceStageExecution(stage, config, reducing, sink);
        }
        throw new IllegalStateException(
            "Backend exchange sink for COORDINATOR_REDUCE stage "
                + stage.getStageId()
                + " must implement ReducingExchangeSink, got "
                + backendSink.getClass().getName()
        );
    }

    /** Picks the plan-alternative bytes bound to the stage's exchange sink provider. */
    private static byte[] chosenBytes(Stage stage) {
        assert stage.getPlanAlternatives().size() == 1 : "COORDINATOR_REDUCE stage "
            + stage.getStageId()
            + " expected exactly one plan alternative, got "
            + stage.getPlanAlternatives().size();
        return stage.getPlanAlternatives().getFirst().convertedBytes();
    }

    /**
     * Builds one {@link ExchangeSinkContext.ChildInput} per child stage. Each input's
     * {@code numInputPartitions} is the resolved producer-task count (shard count for
     * SHARD_FRAGMENT children) passed through the configured {@link PartitionLanePolicy};
     * the backend registers exactly that many native partition lanes per child.
     */
    private List<ExchangeSinkContext.ChildInput> buildChildInputs(Stage stage) {
        List<Stage> children = stage.getChildStages();
        if (children.isEmpty()) {
            throw new IllegalStateException(
                "COORDINATOR_REDUCE stage " + stage.getStageId() + " expected at least one child stage, got zero"
            );
        }
        PartitionLanePolicy policy = this.lanePolicy;
        List<ExchangeSinkContext.ChildInput> inputs = new ArrayList<>(children.size());
        for (Stage child : children) {
            int producers = resolveNumProducers(clusterService, child);
            int lanes = policy.resolveLanes(producers);
            inputs.add(new ExchangeSinkContext.ChildInput(child.getStageId(), producerPlanBytesOf(child), lanes));
            // Operators have no other on-the-wire confirmation that the policy resolved
            // as expected — log it once per child at DEBUG so a runtime check is enough.
            logger.debug(
                "[reduce-factory] stage={} childStageId={} producers={} lanes={} policy={}",
                stage.getStageId(),
                child.getStageId(),
                producers,
                lanes,
                policy.toWireString()
            );
        }
        return inputs;
    }

    /**
     * Returns the producer-side plan bytes for {@code child}: the post-decoration schema
     * bytes when a decorator widens the wire schema (e.g. QTF's {@code OrdinalAppendingSink}),
     * else the producer's natural converted bytes.
     */
    private static byte[] producerPlanBytesOf(Stage child) {
        StagePlan plan = child.getPlanAlternatives().getFirst();
        byte[] decorated = plan.postDecorationSchemaBytes();
        return decorated != null ? decorated : plan.convertedBytes();
    }

    /**
     * Resolves the producer count for {@code child}: shard count for SHARD_FRAGMENT
     * children, 1 for childless / coordinator-side children. Falls back to 1 on any
     * failure so plan-build can't fail here — the scheduler will surface the same error
     * with full context.
     *
     * <p>Calls the same {@code resolver.resolve(clusterService.state(), null)} as
     * {@code ShardFragmentStageExecution.materializeTasks}. The two calls happen at
     * different times and pick up different cluster-state snapshots; the runtime
     * ordinal range may drift from the lane count we register here. {@code ChildSenders}
     * handles the drift safely:
     * <ul>
     *   <li>runtime &gt; lanes: {@code laneForOrdinal} wraps modulo lane count — extra
     *       producers share lanes (contention, no correctness issue).</li>
     *   <li>runtime &lt; lanes: extra lanes stay idle and are dropped on sink close.</li>
     * </ul>
     *
     * <p>When can-match shard pruning lands, keep it inside {@code resolve()} so both
     * call sites see the same pruned set. Pruning between resolve and dispatch leaves
     * idle lanes registered (correct but wasteful).
     *
     * <p>Static + package-private for unit-test access.
     */
    static int resolveNumProducers(ClusterService clusterService, Stage child) {
        if (clusterService == null) {
            return 1;  // test-only / no-cluster path
        }
        TargetResolver resolver = child.getTargetResolver();
        if (resolver == null) {
            return 1;  // childless / non-shard child
        }
        try {
            List<ExecutionTarget> targets = resolver.resolve(clusterService.state(), null);
            int n = targets == null ? 0 : targets.size();
            return n >= 1 ? n : 1;
        } catch (Exception e) {
            return 1;
        }
    }

}
