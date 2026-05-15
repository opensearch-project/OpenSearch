/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Join rel carrying viable backends. Both sides are gathered SINGLETON to the
 * coordinator (enforced by {@link #computeSelfCost}). {@code right} is always the
 * build side (matches substrait {@code JoinRel.right}).
 *
 * @opensearch.internal
 */
public class OpenSearchJoin extends Join implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, List.of(), left, right, condition, Set.of(), joinType);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Output field storage is the concatenation of left and right input storage —
     * matches Calcite's join row type ordering (left fields first, then right).
     *
     * <p>SEMI / ANTI joins project only the left side — Calcite's {@code Join#getRowType}
     * exposes left fields only in those cases, so our storage metadata must mirror that or
     * downstream walkers (e.g. {@code OpenSearchJoinRule.collectStorageFormats} on a wrapping
     * outer join) index past the row and pick up phantom formats from the right.
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        List<FieldStorageInfo> result = new ArrayList<>();
        appendChildStorage(getLeft(), result);
        if (getJoinType().projectsRight()) {
            appendChildStorage(getRight(), result);
        }
        return result;
    }

    private static void appendChildStorage(RelNode child, List<FieldStorageInfo> out) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(child);
        if (unwrapped instanceof OpenSearchRelNode os) {
            out.addAll(os.getOutputFieldStorage());
        }
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new OpenSearchJoin(getCluster(), traitSet, left, right, conditionExpr, joinType, viableBackends);
    }

    /**
     * Cost gate. The join's locality must match its inputs' locality:
     * <ul>
     *   <li>If the join is at {@code COORDINATOR+SINGLETON}, every input must also be
     *       {@code COORDINATOR+SINGLETON}. {@code OpenSearchJoinSplitRule} drives this
     *       by calling {@code convert(input, COORDINATOR+SINGLETON)} which inserts an ER
     *       wherever the input doesn't already deliver that.</li>
     *   <li>If the join is at {@code SHARD+SINGLETON} (co-location fast path), every input
     *       must also be {@code SHARD+SINGLETON} with the same {@code tableId} and
     *       {@code shardCount=1}. Anything else is infinite cost.</li>
     * </ul>
     */
    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(
        org.apache.calcite.plan.RelOptPlanner planner,
        org.apache.calcite.rel.metadata.RelMetadataQuery mq
    ) {
        OpenSearchDistribution selfDist = distributionOf(this);
        if (selfDist == null || selfDist.getType() != org.apache.calcite.rel.RelDistribution.Type.SINGLETON) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        for (RelNode input : getInputs()) {
            OpenSearchDistribution inputDist = distributionOf(input);
            if (inputDist == null) continue;
            if (inputDist.getType() == org.apache.calcite.rel.RelDistribution.Type.ANY) continue;
            if (inputDist.getType() != org.apache.calcite.rel.RelDistribution.Type.SINGLETON) {
                return planner.getCostFactory().makeInfiniteCost();
            }
            // Locality must match the join's own locality.
            if (selfDist.getLocality() != inputDist.getLocality()) {
                return planner.getCostFactory().makeInfiniteCost();
            }
            // SHARD case additionally requires the input to share the join's tableId and shardCount=1.
            if (selfDist.getLocality() == OpenSearchDistribution.Locality.SHARD) {
                if (selfDist.getTableId() == null || !selfDist.getTableId().equals(inputDist.getTableId())) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
                if (!Integer.valueOf(1).equals(inputDist.getShardCount())) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
            }
        }
        return planner.getCostFactory().makeTinyCost();
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            org.apache.calcite.plan.RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchJoin(
            getCluster(),
            getTraitSet(),
            children.get(0),
            children.get(1),
            getCondition(),
            getJoinType(),
            List.of(backend)
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalJoin.create(
            strippedChildren.get(0),
            strippedChildren.get(1),
            List.of(),
            getCondition(),
            Set.<CorrelationId>of(),
            getJoinType()
        );
    }
}
