/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

/**
 * Strategy for distributed join + aggregate execution, surfaced as the per-dispatch counter the
 * {@link MppStrategyMetrics} + {@code GET /_analytics/_strategies} endpoint expose. Under the general
 * post-CBO scheduler the coordinator records which path {@link UnifiedDispatch} ran for a query.
 *
 * <p>Also hosts the small read-only DAG-shape predicates the dispatcher uses to pick the counter
 * ({@link #containsJoin} / {@link #containsFinalAggregate} / {@link #findBroadcastBuild}) — relocated here
 * when the enumerated legacy advisors were removed.
 *
 * @opensearch.internal
 */
public enum MppStrategy {

    /** Both sides reduced to coordinator (SINGLETON), join runs there. Safe default — also the fallback
     *  when the size floor keeps a join/aggregate coordinator-centric. */
    COORDINATOR_CENTRIC,

    /** Small (build) side broadcast to all probe-side data nodes; join runs in parallel on each. */
    BROADCAST,

    /** Both sides hash-partitioned by join key and shuffled to a worker tier. */
    HASH_SHUFFLE,

    /** Hash-shuffle aggregate: a distributed aggregate split into PARTIAL (per-partition, on the worker)
     *  + FINAL (coordinator gather). */
    HASH_SHUFFLE_AGG;

    // ── Read-only DAG-shape predicates (used by DefaultPlanExecutor to pick the dispatch counter) ──

    /** True iff any stage fragment in {@code dag} contains an {@link OpenSearchJoin}. */
    public static boolean containsJoin(QueryDAG dag) {
        Stage rootStage = dag.rootStage();
        if (rootStage == null || rootStage.getFragment() == null) {
            return false;
        }
        return containsJoinRecursive(rootStage);
    }

    private static boolean containsJoinRecursive(Stage stage) {
        if (stage.getFragment() != null && RelNodeUtils.findNode(stage.getFragment(), OpenSearchJoin.class) != null) {
            return true;
        }
        for (Stage child : stage.getChildStages()) {
            if (containsJoinRecursive(child)) return true;
        }
        return false;
    }

    /** True iff any stage fragment in {@code dag} contains a {@code FINAL}-mode {@link OpenSearchAggregate}. */
    public static boolean containsFinalAggregate(QueryDAG dag) {
        return dag != null && dag.rootStage() != null && containsFinalAggregateRecursive(dag.rootStage());
    }

    private static boolean containsFinalAggregateRecursive(Stage stage) {
        if (containsFinalAggregateInFragment(stage.getFragment())) return true;
        for (Stage child : stage.getChildStages()) {
            if (containsFinalAggregateRecursive(child)) return true;
        }
        return false;
    }

    private static boolean containsFinalAggregateInFragment(RelNode root) {
        if (root == null) return false;
        if (root instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) return true;
        for (RelNode input : root.getInputs()) {
            if (containsFinalAggregateInFragment(input)) return true;
        }
        return false;
    }

    /** Locates the {@link Stage.StageRole#BROADCAST_BUILD} stage anywhere in {@code dag}, or {@code null}. */
    public static Stage findBroadcastBuild(QueryDAG dag) {
        Stage root = dag.rootStage();
        if (root == null) return null;
        return findStageByRole(root, Stage.StageRole.BROADCAST_BUILD);
    }

    private static Stage findStageByRole(Stage stage, Stage.StageRole role) {
        if (stage.getRole() == role) return stage;
        for (Stage child : stage.getChildStages()) {
            Stage found = findStageByRole(child, role);
            if (found != null) return found;
        }
        return null;
    }
}
