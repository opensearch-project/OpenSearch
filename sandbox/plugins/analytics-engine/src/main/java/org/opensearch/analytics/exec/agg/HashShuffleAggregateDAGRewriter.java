/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.agg;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.WorkerTargetResolver;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a hash-shuffle aggregate DAG to insert a worker tier between the single producer
 * scan stage and the existing consumer (typically the root coord-side gather). Sibling of
 * {@link org.opensearch.analytics.exec.join.HashShuffleDAGRewriter}: same lift-the-operator
 * pattern, but specialized to {@link OpenSearchAggregate}({@link AggregateMode#FINAL}) instead
 * of {@code OpenSearchJoin}, and with one shuffle producer instead of two.
 *
 * <pre>
 *   consumer (was: FINAL inside) → consumer (now: StageInputScan referencing workerStageId)
 *     └── producer                   └── worker (new, WORKER_FRAGMENT)
 *                                          └── producer
 * </pre>
 *
 * <p>The worker stage's fragment is the lifted {@code FINAL + ShuffleExchange} subtree; the
 * convertor strips the shuffle wrapper and emits a FINAL aggregate over a single NamedScan
 * ({@code "input-<producerId>"}) that the {@code ShuffleScanHandler} resolves to a per-partition
 * streaming table at runtime.
 *
 * @opensearch.internal
 */
public final class HashShuffleAggregateDAGRewriter {

    private HashShuffleAggregateDAGRewriter() {}

    /**
     * Rewrites {@code dag} to insert a worker stage holding the lifted FINAL aggregate.
     *
     * @param dag                 current DAG with {@code consumer} holding the FINAL inline
     * @param consumer            stage whose direct child is {@code producer}; its fragment
     *                            contains the OpenSearchAggregate(mode=FINAL)
     * @param producer            existing SHUFFLE_SCAN_AGG child
     * @param targetWorkerNodeIds one node id per partition; the worker stage's
     *                            {@link WorkerTargetResolver} fans out one task per id
     * @param registry            capability registry — used to re-run fragment conversion on
     *                            the worker stage and the modified consumer
     * @return rewritten DAG ready for {@link org.opensearch.analytics.exec.QueryScheduler#execute}
     */
    public static Rewritten rewrite(
        QueryDAG dag,
        Stage consumer,
        Stage producer,
        List<String> targetWorkerNodeIds,
        CapabilityRegistry registry,
        boolean preferMetadataDriver
    ) {
        RelNode consumerFragment = consumer.getFragment();
        OpenSearchAggregate finalAgg = findFinalAggregate(consumerFragment);
        if (finalAgg == null) {
            throw new IllegalStateException(
                "HashShuffleAggregateDAGRewriter: consumer fragment does not contain an "
                    + "OpenSearchAggregate(mode=FINAL): "
                    + consumerFragment.explain()
            );
        }

        int workerStageId = nextStageId(dag);

        AnalyticsSearchBackendPlugin backend = registry.getBackend(consumer.getPlanAlternatives().getFirst().backendId());
        Stage worker = new Stage(
            workerStageId,
            finalAgg,
            List.of(producer),
            ExchangeInfo.singleton(),
            backend.getExchangeSinkProvider(),
            new WorkerTargetResolver(targetWorkerNodeIds)
        );
        worker.setRole(Stage.StageRole.SHUFFLE_WORKER);
        worker.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());

        OpenSearchStageInputScan workerInputScan = new OpenSearchStageInputScan(
            finalAgg.getCluster(),
            finalAgg.getTraitSet(),
            workerStageId,
            finalAgg.getRowType(),
            ((OpenSearchRelNode) finalAgg).getViableBackends(),
            ((OpenSearchRelNode) finalAgg).getOutputFieldStorage()
        );
        RelNode rewrittenConsumerFragment = replaceWith(consumerFragment, finalAgg, workerInputScan);

        Stage rewrittenConsumer = new Stage(
            consumer.getStageId(),
            rewrittenConsumerFragment,
            List.of(worker),
            consumer.getExchangeInfo(),
            consumer.getExchangeSinkProvider(),
            consumer.getTargetResolver()
        );
        rewrittenConsumer.setRole(consumer.getRole());
        if (consumer.getInstructionHandlerFactory() != null) {
            rewrittenConsumer.setInstructionHandlerFactory(consumer.getInstructionHandlerFactory());
        }

        Stage newRoot = consumer == dag.rootStage()
            ? rewrittenConsumer
            : rebuildRootSwappingConsumer(dag.rootStage(), consumer, rewrittenConsumer);

        QueryDAG rewrittenDag = new QueryDAG(dag.queryId(), newRoot);
        // forkAll rebuilds each stage's plan alternatives straight from its fragment, discarding
        // any prior adaptAll output (which only lived in the old alternatives, never on the
        // fragment RelNode). The worker stage's lifted FINAL aggregate is therefore un-adapted, so
        // adaptAll MUST re-run before convertAll: BackendPlanAdapter applies DistributedAggregate-
        // Rewriter to the FINAL aggregate, which rebases each agg-call's arg to the partial-output
        // state column (groupCount + i) and swaps decomposable functions (e.g. COUNT → SUM). Skip
        // it and the worker would run the original SINGLE aggregate calls over the wrong columns,
        // producing incorrect results once a group has partial rows from multiple shards. The join
        // sibling (HashShuffleDAGRewriter) omits this because joins need no aggregate adaptation;
        // the aggregate path does. forkAll → adaptAll → convertAll mirrors DefaultPlanExecutor.
        PlanForker.forkAll(rewrittenDag, registry);
        BackendPlanAdapter.adaptAll(rewrittenDag, registry);
        // selectAll MUST run before convertAll, exactly as DefaultPlanExecutor does: forkAll
        // re-expands every stage to all viable-backend alternatives straight from its fragment,
        // so without re-selecting, a child stage feeding a single-backend parent operator (e.g.
        // the supplier scan under a DataFusion-only coordinator join) keeps its Lucene alternative
        // and the data node picks Lucene first — yielding a 0-column metadata batch the join can't
        // read and a downstream hang. selectAll re-applies the parent-backend correctness
        // constraint (and metadata-driver scoring when enabled).
        PlanAlternativeSelector.selectAll(rewrittenDag, registry, preferMetadataDriver);
        FragmentConversionDriver.convertAll(rewrittenDag, registry);

        return new Rewritten(rewrittenDag, newRoot, worker, rewrittenConsumer);
    }

    /** Returned by {@link #rewrite}: the new DAG plus references to the worker stage and the
     *  rebuilt consumer. The old consumer reference is no longer attached to the DAG. */
    public record Rewritten(QueryDAG dag, Stage rootStage, Stage worker, Stage consumer) {
    }

    /** Finds the first {@link OpenSearchAggregate}({@code mode=FINAL}) in {@code root}'s subtree;
     *  null if none. */
    private static OpenSearchAggregate findFinalAggregate(RelNode root) {
        if (root instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) {
            return agg;
        }
        for (RelNode input : root.getInputs()) {
            OpenSearchAggregate found = findFinalAggregate(input);
            if (found != null) return found;
        }
        return null;
    }

    /** Rebuilds {@code root} replacing the matching {@code target} node with {@code replacement};
     *  ancestors are copied with the new input. Assumes exactly one match in the tree. */
    private static RelNode replaceWith(RelNode root, RelNode target, RelNode replacement) {
        if (root == target) {
            return replacement;
        }
        List<RelNode> oldInputs = root.getInputs();
        if (oldInputs.isEmpty()) {
            return root;
        }
        List<RelNode> newInputs = new ArrayList<>(oldInputs.size());
        boolean changed = false;
        for (RelNode input : oldInputs) {
            RelNode rewritten = replaceWith(input, target, replacement);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        return changed ? root.copy(root.getTraitSet(), newInputs) : root;
    }

    private static Stage rebuildRootSwappingConsumer(Stage current, Stage oldConsumer, Stage newConsumer) {
        if (current == oldConsumer) {
            return newConsumer;
        }
        List<Stage> children = current.getChildStages();
        if (children.isEmpty()) {
            return current;
        }
        List<Stage> newChildren = new ArrayList<>(children.size());
        boolean changed = false;
        for (Stage child : children) {
            Stage rebuilt = rebuildRootSwappingConsumer(child, oldConsumer, newConsumer);
            newChildren.add(rebuilt);
            if (rebuilt != child) {
                changed = true;
            }
        }
        if (!changed) {
            return current;
        }
        Stage copy = new Stage(
            current.getStageId(),
            current.getFragment(),
            newChildren,
            current.getExchangeInfo(),
            current.getExchangeSinkProvider(),
            current.getTargetResolver()
        );
        copy.setRole(current.getRole());
        copy.setPlanAlternatives(current.getPlanAlternatives());
        if (current.getInstructionHandlerFactory() != null) {
            copy.setInstructionHandlerFactory(current.getInstructionHandlerFactory());
        }
        return copy;
    }

    private static int nextStageId(QueryDAG dag) {
        int[] max = { -1 };
        walkStages(dag.rootStage(), max);
        return max[0] + 1;
    }

    private static void walkStages(Stage stage, int[] max) {
        if (stage == null) return;
        if (stage.getStageId() > max[0]) max[0] = stage.getStageId();
        for (Stage child : stage.getChildStages()) {
            walkStages(child, max);
        }
    }
}
