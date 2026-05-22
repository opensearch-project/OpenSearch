/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

/**
 * Observation-only walker over a CBO-produced {@link QueryDAG}. Surfaces:
 * <ul>
 *   <li>{@link #containsJoin(QueryDAG)} — whether the DAG contains an {@link OpenSearchJoin},
 *       used as a metrics gate so non-join queries don't pollute join-strategy counters.</li>
 *   <li>{@link #observe(QueryDAG)} — which {@link JoinStrategy} the DAG already encodes,
 *       inferred from the stage roles {@code DAGBuilder} stamped at cut time. Read-only:
 *       no decisions, no rewrites, no role mutations.</li>
 * </ul>
 *
 * <p>The strategy decision itself is made by Calcite's CBO via the cost model on the three
 * split rules ({@link org.opensearch.analytics.planner.rules.OpenSearchJoinSplitRule
 * coord-centric}, {@link org.opensearch.analytics.planner.rules.OpenSearchBroadcastJoinSplitRule
 * broadcast}, and {@link org.opensearch.analytics.planner.rules.OpenSearchHashJoinSplitRule
 * hash-shuffle}). {@code DAGBuilder} cuts at the resulting exchange RelNodes and stamps the
 * appropriate {@link Stage.StageRole}; this class only reports what's already there.
 *
 * @opensearch.internal
 */
public final class JoinStrategyAdvisor {

    private JoinStrategyAdvisor() {}

    /**
     * Returns {@code true} iff the DAG's root-stage fragment contains an {@link OpenSearchJoin}
     * anywhere in its tree. Used by {@code DefaultPlanExecutor} to gate join-strategy metrics
     * so scans, aggregations, and other non-join queries don't get counted.
     */
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

    /**
     * Reads the join strategy that CBO + DAGBuilder produced for this DAG.
     *
     * <p>Resolution priority (in order):
     * <ol>
     *   <li>Any stage tagged {@link Stage.StageRole#BROADCAST_BUILD} → {@link JoinStrategy#BROADCAST}.</li>
     *   <li>Any stage whose exchange-info distribution is HASH_DISTRIBUTED → {@link JoinStrategy#HASH_SHUFFLE}.
     *       DAGBuilder.cutShuffle records the shuffle's hash distribution on the child stage;
     *       we don't role-tag at cut time because cutShuffle doesn't know which side of the
     *       parent join the shuffle feeds.</li>
     *   <li>Otherwise (including non-join queries) → {@link JoinStrategy#COORDINATOR_CENTRIC}.</li>
     * </ol>
     *
     * <p>Callers that need to distinguish "non-join, returned coord-centric by default" from
     * "actual join, CBO picked coord-centric" should gate on {@link #containsJoin} first.
     */
    public static JoinStrategy observe(QueryDAG dag) {
        Stage root = dag.rootStage();
        if (root == null) {
            return JoinStrategy.COORDINATOR_CENTRIC;
        }
        if (anyStageHasRole(root, Stage.StageRole.BROADCAST_BUILD)) {
            return JoinStrategy.BROADCAST;
        }
        if (anyStageHasHashExchange(root)) {
            return JoinStrategy.HASH_SHUFFLE;
        }
        return JoinStrategy.COORDINATOR_CENTRIC;
    }

    /**
     * Locates the {@link Stage.StageRole#BROADCAST_BUILD} stage in the DAG, or {@code null} if
     * absent. Returned reference is the live stage in the DAG — callers should not mutate it.
     * Used by {@code DefaultPlanExecutor} to feed {@code BroadcastDispatch} the build/probe/root
     * triple without re-walking the DAG itself.
     */
    public static Stage findBroadcastBuild(QueryDAG dag) {
        Stage root = dag.rootStage();
        if (root == null) return null;
        return findStageByRole(root, Stage.StageRole.BROADCAST_BUILD);
    }

    /**
     * Locates the {@link Stage.StageRole#BROADCAST_PROBE} stage. With M2 CBO-driven DAGs the
     * probe stage sits between the root and the build stage; {@code DefaultPlanExecutor.dispatchBroadcast}
     * needs it to inject the broadcast instruction onto its plan alternatives at pass-2 dispatch.
     */
    public static Stage findBroadcastProbe(QueryDAG dag) {
        Stage root = dag.rootStage();
        if (root == null) return null;
        return findStageByRole(root, Stage.StageRole.BROADCAST_PROBE);
    }

    private static boolean anyStageHasRole(Stage stage, Stage.StageRole role) {
        if (stage.getRole() == role) return true;
        for (Stage child : stage.getChildStages()) {
            if (anyStageHasRole(child, role)) return true;
        }
        return false;
    }

    private static boolean anyStageHasHashExchange(Stage stage) {
        ExchangeInfo info = stage.getExchangeInfo();
        if (info != null && info.distributionType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            return true;
        }
        for (Stage child : stage.getChildStages()) {
            if (anyStageHasHashExchange(child)) return true;
        }
        return false;
    }

    private static Stage findStageByRole(Stage stage, Stage.StageRole role) {
        if (stage.getRole() == role) return stage;
        for (Stage child : stage.getChildStages()) {
            Stage found = findStageByRole(child, role);
            if (found != null) return found;
        }
        return null;
    }

    /** RelNode-based join lookup retained for {@link #containsJoin} convenience. */
    @SuppressWarnings("unused")
    private static boolean fragmentContainsJoin(RelNode fragment) {
        return RelNodeUtils.findNode(fragment, OpenSearchJoin.class) != null;
    }
}
