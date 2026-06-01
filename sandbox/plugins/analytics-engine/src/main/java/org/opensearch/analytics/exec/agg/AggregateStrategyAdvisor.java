/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.agg;

import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;

/**
 * Sibling of {@code JoinStrategyAdvisor} for aggregate-shuffle dispatch routing. The
 * planner-side {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateShuffleSplitRule}
 * decides whether a shuffle alternative is registered; the CBO race decides whether it wins.
 * If it wins, {@code DAGBuilder.cutShuffle} tags the producer child stage as
 * {@link Stage.StageRole#SHUFFLE_SCAN_AGG}. This advisor reads that tag to drive dispatcher
 * routing — there's no separate strategy enum because aggregate has only two shapes
 * (coord-centric, hash-shuffle) and no broadcast equivalent.
 *
 * @opensearch.internal
 */
public final class AggregateStrategyAdvisor {

    private AggregateStrategyAdvisor() {}

    /** True when {@code dag} contains any {@link OpenSearchAggregate}({@code mode=FINAL}). */
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

    private static boolean containsFinalAggregateInFragment(org.apache.calcite.rel.RelNode root) {
        if (root == null) return false;
        if (root instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) return true;
        for (org.apache.calcite.rel.RelNode input : root.getInputs()) {
            if (containsFinalAggregateInFragment(input)) return true;
        }
        return false;
    }

    /** Returns the first stage tagged {@link Stage.StageRole#SHUFFLE_SCAN_AGG}, or null if none.
     *  CBO/DAG produces at most one such producer per query (single-source aggregate). */
    public static Stage findAggregateShuffleProducer(QueryDAG dag) {
        if (dag == null || dag.rootStage() == null) return null;
        return findStageByRole(dag.rootStage(), Stage.StageRole.SHUFFLE_SCAN_AGG);
    }

    private static Stage findStageByRole(Stage stage, Stage.StageRole role) {
        if (stage == null) return null;
        if (stage.getRole() == role) return stage;
        for (Stage child : stage.getChildStages()) {
            Stage found = findStageByRole(child, role);
            if (found != null) return found;
        }
        return null;
    }
}
