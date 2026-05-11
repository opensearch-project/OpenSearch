/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decides the {@link JoinStrategy} for a query DAG and tags stages with the corresponding
 * {@link Stage.StageRole} prior to scheduler dispatch.
 *
 * <p>Detection scope (M1):
 * <ul>
 *   <li>Root of the DAG's root-stage fragment is an {@link OpenSearchJoin} whose left and right
 *       inputs are {@link OpenSearchExchangeReducer}s over a per-side subtree containing exactly
 *       one {@link OpenSearchTableScan}.</li>
 *   <li>Anything else (multi-way joins after Calcite collapsing, joins over derived inputs,
 *       joins beneath aggregates): fall through to {@link JoinStrategy#COORDINATOR_CENTRIC}.</li>
 * </ul>
 *
 * <p>This class does not modify the DAG. It inspects, decides, tags roles, and returns the
 * strategy. Strategy dispatch (what to actually do for BROADCAST / HASH_SHUFFLE) is the
 * caller's responsibility — see {@code DefaultPlanExecutor.executeInternal}.
 *
 * @opensearch.internal
 */
public final class JoinStrategyAdvisor {

    private static final Logger LOGGER = LogManager.getLogger(JoinStrategyAdvisor.class);

    private final int broadcastMaxShards;

    public JoinStrategyAdvisor(int broadcastMaxShards) {
        this.broadcastMaxShards = broadcastMaxShards;
    }

    /**
     * Inspects {@code dag}, picks a strategy, and tags stage roles when applicable. Returns the
     * chosen {@link JoinStrategy}. For non-join queries returns {@link JoinStrategy#COORDINATOR_CENTRIC}
     * (the existing default single-stage or reduce path).
     */
    public JoinStrategy adviseAndTag(QueryDAG dag, ClusterState clusterState) {
        Stage rootStage = dag.rootStage();
        if (rootStage == null || rootStage.getFragment() == null) {
            return JoinStrategy.COORDINATOR_CENTRIC;
        }

        RelNode rootFragment = RelNodeUtils.unwrapHep(rootStage.getFragment());
        if (!(rootFragment instanceof OpenSearchJoin join)) {
            return JoinStrategy.COORDINATOR_CENTRIC;
        }

        // Confirm the M1-admissible shape: both root-fragment inputs are ExchangeReducer +
        // StageInputScan (DAGBuilder's post-cut shape), and the index names come from the
        // child stages' fragments — DAGBuilder places the real scan subtree into a child Stage
        // and replaces the reducer's input with a StageInputScan placeholder in the root.
        RelNode left = RelNodeUtils.unwrapHep(join.getLeft());
        RelNode right = RelNodeUtils.unwrapHep(join.getRight());
        if (!(left instanceof OpenSearchExchangeReducer) || !(right instanceof OpenSearchExchangeReducer)) {
            LOGGER.info("Join has non-reducer inputs, falling back to COORDINATOR_CENTRIC");
            return JoinStrategy.COORDINATOR_CENTRIC;
        }
        List<Stage> children = rootStage.getChildStages();
        if (children.size() != 2) {
            LOGGER.info("Expected 2 child stages for binary join, got {}, falling back to COORDINATOR_CENTRIC", children.size());
            return JoinStrategy.COORDINATOR_CENTRIC;
        }

        String leftIndex = findSoleScanIndex(children.get(0).getFragment());
        String rightIndex = findSoleScanIndex(children.get(1).getFragment());
        if (leftIndex == null || rightIndex == null) {
            LOGGER.info("Join inputs are not simple scans, falling back to COORDINATOR_CENTRIC");
            return JoinStrategy.COORDINATOR_CENTRIC;
        }

        JoinInfo joinInfo = join.analyzeCondition();
        boolean isEqui = !joinInfo.leftKeys.isEmpty();

        Set<String> indexNames = new HashSet<>();
        indexNames.add(leftIndex);
        indexNames.add(rightIndex);
        Map<String, TableStatistics> stats = StatisticsCollector.collect(clusterState, indexNames);

        JoinStrategySelector selector = new JoinStrategySelector(broadcastMaxShards, stats);
        JoinStrategy strategy = selector.selectStrategy(leftIndex, rightIndex, isEqui);

        LOGGER.info(
            "Join strategy: {} for {} ⋈ {} (isEqui={}, broadcastMaxShards={})",
            strategy,
            leftIndex,
            rightIndex,
            isEqui,
            broadcastMaxShards
        );

        // Tag stage roles. For COORDINATOR_CENTRIC the existing roles (SHARD_SOURCE on children,
        // COORDINATOR_REDUCE derived from the sink provider) are already correct — no re-tagging
        // needed. For BROADCAST we mark one build, one probe; for HASH_SHUFFLE we leave tagging
        // to the M2 DAG rewrite (that replaces the shape entirely).
        if (strategy == JoinStrategy.BROADCAST) {
            tagBroadcastRoles(rootStage, selector.selectBuildSide(leftIndex, rightIndex));
        }

        return strategy;
    }

    /**
     * Walks down to the sole {@link OpenSearchTableScan} in {@code subtree}. Returns the scan's
     * qualified table name, or {@code null} if the subtree doesn't look like a simple scan+filter
     * +project pipeline.
     *
     * <p>Only descends through operators whose output rows are a 1:1 projection of a single
     * underlying shard scan — {@link OpenSearchFilter}, {@link OpenSearchProject},
     * {@link OpenSearchExchangeReducer}, {@link OpenSearchShuffleExchange}. Anything else (an
     * aggregate, a nested join, a union, a generic unary op we don't recognise) returns
     * {@code null}, falling back to {@link JoinStrategy#COORDINATOR_CENTRIC}. Otherwise joins
     * over aggregated subqueries ({@code A JOIN (SELECT ... GROUP BY ...) B}) would be
     * mis-classified as "large source" and pushed into MPP — Codex P2.
     */
    private String findSoleScanIndex(RelNode subtree) {
        RelNode node = RelNodeUtils.unwrapHep(subtree);
        while (!(node instanceof OpenSearchTableScan)) {
            if (!(node instanceof OpenSearchFilter
                || node instanceof OpenSearchProject
                || node instanceof OpenSearchExchangeReducer
                || node instanceof OpenSearchShuffleExchange)) {
                // Aggregate, Join, Union, StageInputScan, or any unrecognised op — not a plain
                // scan subtree, strategy selection cannot identify a single source index.
                return null;
            }
            List<RelNode> inputs = node.getInputs();
            if (inputs.size() != 1) {
                return null;
            }
            node = RelNodeUtils.unwrapHep(inputs.get(0));
        }
        List<String> qualified = ((OpenSearchTableScan) node).getTable().getQualifiedName();
        return qualified.isEmpty() ? null : qualified.get(qualified.size() - 1);
    }

    /**
     * Assign {@link Stage.StageRole#BROADCAST_BUILD} to the child stage matching the chosen build
     * side and {@link Stage.StageRole#BROADCAST_PROBE} to the other. The root stage's role stays
     * {@link Stage.StageRole#COORDINATOR_REDUCE} (it's where the join runs in M1 broadcast — the
     * probe-side rewrite that pushes the join down to probe nodes is a follow-up).
     */
    private void tagBroadcastRoles(Stage rootStage, String buildSide) {
        rootStage.setRole(Stage.StageRole.COORDINATOR_REDUCE);
        List<Stage> children = rootStage.getChildStages();
        if (children.size() != 2) {
            LOGGER.warn("Expected exactly 2 child stages for binary join, got {} — skipping role tagging", children.size());
            return;
        }
        int buildIndex = "left".equals(buildSide) ? 0 : 1;
        children.get(buildIndex).setRole(Stage.StageRole.BROADCAST_BUILD);
        children.get(1 - buildIndex).setRole(Stage.StageRole.BROADCAST_PROBE);
    }
}
