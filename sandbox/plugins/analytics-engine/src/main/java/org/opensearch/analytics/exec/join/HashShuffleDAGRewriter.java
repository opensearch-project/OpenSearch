/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

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
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a hash-shuffle DAG to insert a worker tier between the producer scan stages and the
 * existing consumer stage (typically the root). Today's CBO-produced shape places the
 * {@link OpenSearchJoin} directly inside the consumer fragment with two
 * {@link org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange} children — that runs
 * the join at the coordinator on the gathered partitions, defeating parallelism. After this
 * rewrite:
 *
 * <pre>
 *   consumer (was: Join inside) → consumer (now: StageInputScan referencing workerStageId)
 *     ├── leftProducer                     ├── worker (new, WORKER_FRAGMENT)
 *     └── rightProducer                    │     ├── leftProducer
 *                                          │     └── rightProducer
 * </pre>
 *
 * <p>The worker stage's fragment is the lifted {@code Join + ShuffleExchanges} subtree; the
 * convertor strips the shuffle wrappers and emits a join over two NamedScans
 * ({@code "input-<leftProducerId>"} / {@code "input-<rightProducerId>"}) that the
 * {@code ShuffleScanHandler} resolves to per-partition streaming tables at runtime.
 *
 * @opensearch.internal
 */
public final class HashShuffleDAGRewriter {

    private HashShuffleDAGRewriter() {}

    /**
     * Rewrites {@code dag} to insert a worker stage. Returns the rewritten DAG (new root); the
     * caller's {@code consumer} pointer is replaced by the rewritten consumer in the new DAG.
     *
     * @param dag                  current DAG with {@code consumer} holding the join inline
     * @param consumer             stage whose direct children are {@code leftProducer} +
     *                             {@code rightProducer}; its fragment contains the join
     * @param leftProducer         existing SHUFFLE_SCAN_LEFT child
     * @param rightProducer        existing SHUFFLE_SCAN_RIGHT child
     * @param targetWorkerNodeIds  one node id per partition; the worker stage's
     *                             {@link WorkerTargetResolver} fans out one task per id
     * @param registry             capability registry — used to re-run fragment conversion on
     *                             the worker stage and the modified consumer
     * @return rewritten DAG ready for {@link org.opensearch.analytics.exec.QueryScheduler#execute}
     */
    public static Rewritten rewrite(
        QueryDAG dag,
        Stage consumer,
        Stage leftProducer,
        Stage rightProducer,
        List<String> targetWorkerNodeIds,
        CapabilityRegistry registry,
        boolean preferMetadataDriver
    ) {
        // 1) Find the OpenSearchJoin inside the consumer fragment. Its children carry the
        // OpenSearchShuffleExchange wrappers around StageInputScans that the convertor will
        // strip. The lifted subtree becomes the worker fragment as-is.
        RelNode consumerFragment = consumer.getFragment();
        OpenSearchJoin join = findJoin(consumerFragment);
        if (join == null) {
            throw new IllegalStateException(
                "HashShuffleDAGRewriter: consumer fragment does not contain an OpenSearchJoin: " + consumerFragment.explain()
            );
        }

        // 2) Allocate a new stage id for the worker. Stage ids must be unique across the DAG;
        // the simplest deterministic source is the count of existing stages plus the root.
        int workerStageId = nextStageId(dag);

        // 3) Build the worker stage. Fragment = the lifted join subtree (still has shuffle
        // wrappers; FragmentConvertor strips them). Children = the two producers (moved from
        // the consumer's child list).
        AnalyticsSearchBackendPlugin backend = registry.getBackend(consumer.getPlanAlternatives().getFirst().backendId());
        Stage worker = new Stage(
            workerStageId,
            join,
            List.of(leftProducer, rightProducer),
            ExchangeInfo.singleton(),
            backend.getExchangeSinkProvider(),
            new WorkerTargetResolver(targetWorkerNodeIds)
        );
        worker.setRole(Stage.StageRole.SHUFFLE_WORKER);
        worker.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());

        // 4) Rewrite the consumer fragment: replace the join subtree with a single
        // OpenSearchStageInputScan(workerStageId). The convertor's existing rewriter maps
        // OpenSearchStageInputScan → "input-<workerStageId>" NamedScan; the coordinator's
        // reduce sink sees worker output flowing in via the standard scheduler path.
        OpenSearchStageInputScan workerInputScan = new OpenSearchStageInputScan(
            join.getCluster(),
            join.getTraitSet(),
            workerStageId,
            join.getRowType(),
            ((OpenSearchRelNode) join).getViableBackends(),
            ((OpenSearchRelNode) join).getOutputFieldStorage()
        );
        RelNode rewrittenConsumerFragment = replaceJoinWith(consumerFragment, join, workerInputScan);

        // 5) Build the rewritten consumer with new fragment + new child list. Stage construction
        // is immutable for the children list, so we copy.
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

        // 6) Rebuild the rest of the DAG, swapping the consumer when we hit it. For the simple
        // "consumer == root" case this is a one-step swap; for the wrappers-above-join shape
        // the consumer might be deeper, in which case ancestors copy through unchanged.
        Stage newRoot = consumer == dag.rootStage()
            ? rewrittenConsumer
            : rebuildRootSwappingConsumer(dag.rootStage(), consumer, rewrittenConsumer);

        QueryDAG rewrittenDag = new QueryDAG(dag.queryId(), newRoot);

        // 7) Re-run the FULL fork → adapt → select → convert pipeline, mirroring DefaultPlanExecutor
        // and CascadeShuffleDAGRewriter. forkAll re-expands each stage's per-backend alternatives FROM
        // ITS FRAGMENT — which discards the scalar-function adaptation BackendPlanAdapter.adaptAll
        // already applied (e.g. DATE(string) → to_date, the q14 join-residual case). So adaptAll MUST
        // re-run here, else convertAll hands isthmus the un-adapted RexCall and it throws "Unable to
        // convert call DATE(string)". selectAll re-applies the parent-backend correctness constraint
        // (a child stage restricted to the backends its consumer declares) that forkAll likewise wiped.
        // Skipping either is the bug class documented for HashShuffleAggregateDAGRewriter / the q15 hang.
        PlanForker.forkAll(rewrittenDag, registry);
        BackendPlanAdapter.adaptAll(rewrittenDag, registry);
        PlanAlternativeSelector.selectAll(rewrittenDag, registry, preferMetadataDriver);
        FragmentConversionDriver.convertAll(rewrittenDag, registry);

        return new Rewritten(rewrittenDag, newRoot, worker, rewrittenConsumer);
    }

    /** Returned by {@link #rewrite}: the new DAG plus references to the worker stage and the
     *  rebuilt consumer. The old consumer reference is no longer attached to the DAG. */
    public record Rewritten(QueryDAG dag, Stage rootStage, Stage worker, Stage consumer) {
    }

    /** Finds the first {@link OpenSearchJoin} in {@code root}'s subtree; null if none. */
    private static OpenSearchJoin findJoin(RelNode root) {
        if (root instanceof OpenSearchJoin j) return j;
        for (RelNode input : root.getInputs()) {
            OpenSearchJoin found = findJoin(input);
            if (found != null) return found;
        }
        return null;
    }

    /** Rebuilds {@code root} replacing the matching {@code target} node with {@code replacement};
     *  ancestors are copied with the new input. Assumes exactly one match in the tree. */
    private static RelNode replaceJoinWith(RelNode root, RelNode target, RelNode replacement) {
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
            RelNode rewritten = replaceJoinWith(input, target, replacement);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        return changed ? root.copy(root.getTraitSet(), newInputs) : root;
    }

    /** Rebuilds the DAG when the consumer is not the root: ancestors are copied with the new
     *  child list when they contained the original consumer. */
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

    /** Returns a stage id one greater than the largest existing stage id in the DAG. */
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
