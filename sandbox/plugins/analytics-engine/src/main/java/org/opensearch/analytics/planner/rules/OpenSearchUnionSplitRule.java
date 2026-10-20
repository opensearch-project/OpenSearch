/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives per-arm distribution for {@link OpenSearchUnion}.
 *
 * <p><b>Co-location fast path.</b> When every arm is a SHARD+SINGLETON scan with
 * {@code shardCount=1} and the same {@code tableId}, all arms are already on the same
 * node. The Union runs at that shard node without any ER; its output carries the
 * matched SHARD+SINGLETON+shardCount=1 trait so a downstream operator (or the root)
 * can insert a single gather ER above it.
 *
 * <p><b>General path.</b> Otherwise, request {@code COORDINATOR+SINGLETON} on each
 * arm. Volcano's {@code ExpandConversionRule} +
 * {@link OpenSearchDistributionTraitDef#convert} then materialize an
 * {@link org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer} on any arm
 * not already at {@code COORDINATOR+SINGLETON}.
 *
 * @opensearch.internal
 */
public class OpenSearchUnionSplitRule extends RelOptRule {

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchUnionSplitRule(PlannerContext context) {
        super(operand(OpenSearchUnion.class, any()), "OpenSearchUnionSplitRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchUnion union = call.rel(0);
        // Already satisfied — every arm is COORDINATOR+SINGLETON and the Union's own trait
        // reflects that, or every arm is co-located SHARD+SINGLETON(shardCount=1, tableId) and
        // the Union already carries the shared SHARD trait. Nothing to do.
        if (unionAlreadyResolved(union)) return false;
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchUnion union = call.rel(0);

        Integer commonTableId = commonColocatedTableId(union.getInputs());
        if (commonTableId != null) {
            // Co-location applies: every arm is a 1-shard scan of the same table, so the
            // whole subtree resolves to a single node. Build the Union at SHARD with no ER
            // under any arm, then call convert(shardUnion, COORDINATOR) to register a
            // SHARD→COORDINATOR converter on top — so a parent demanding COORDINATOR sees
            // a single gather ER above the SHARD Union (one transport instead of one-per-arm).
            RelTraitSet shardTraits = union.getTraitSet().replace(distTraitDef.shardSingleton(commonTableId, 1));
            RelNode shardUnion = union.copy(shardTraits, union.getInputs(), union.all);
            RelTraitSet coordTraits = union.getTraitSet().replace(distTraitDef.coordSingleton());
            // Register the SHARD→COORDINATOR converter; downstream consumers can use either.
            convert(shardUnion, coordTraits);
            call.transformTo(shardUnion);
            return;
        }

        // Not co-located: at least one arm originates from a different table or shard layout,
        // so per-arm ERs are unavoidable. Demand COORDINATOR+SINGLETON on each arm; Volcano
        // materializes an ER on any arm not already there via TraitDef.convert.
        RelTraitSet coordTraits = union.getTraitSet().replace(distTraitDef.coordSingleton());
        List<RelNode> gatheredInputs = new ArrayList<>(union.getInputs().size());
        for (RelNode input : union.getInputs()) {
            gatheredInputs.add(convert(input, coordTraits));
        }
        call.transformTo(union.copy(coordTraits, gatheredInputs, union.all));
    }

    /** Returns the shared tableId iff every input is {@code SHARD+SINGLETON+shardCount=1}
     *  and their {@code tableId}s all match. Otherwise null. */
    private static Integer commonColocatedTableId(List<RelNode> inputs) {
        Integer commonId = null;
        for (RelNode input : inputs) {
            OpenSearchDistribution dist = distributionOf(input);
            if (dist == null) return null;
            if (dist.getLocality() != OpenSearchDistribution.Locality.SHARD) return null;
            if (dist.getType() != RelDistribution.Type.SINGLETON) return null;
            if (!Integer.valueOf(1).equals(dist.getShardCount())) return null;
            Integer tid = dist.getTableId();
            if (tid == null) return null;
            if (commonId == null) commonId = tid;
            else if (!commonId.equals(tid)) return null;
        }
        return commonId;
    }

    private static boolean unionAlreadyResolved(OpenSearchUnion union) {
        OpenSearchDistribution unionDist = distributionOf(union);
        if (unionDist == null) return false;
        // Case 1: Union is at COORDINATOR+SINGLETON and every arm is too.
        if (unionDist.getLocality() == OpenSearchDistribution.Locality.COORDINATOR
            && unionDist.getType() == RelDistribution.Type.SINGLETON) {
            for (RelNode input : union.getInputs()) {
                OpenSearchDistribution d = distributionOf(input);
                if (d == null) return false;
                if (d.getLocality() != OpenSearchDistribution.Locality.COORDINATOR) return false;
                if (d.getType() != RelDistribution.Type.SINGLETON) return false;
            }
            return true;
        }
        // Case 2: Union is at SHARD+SINGLETON (co-located) — the fast path already fired.
        if (unionDist.getLocality() == OpenSearchDistribution.Locality.SHARD && unionDist.getType() == RelDistribution.Type.SINGLETON) {
            return true;
        }
        return false;
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }
}
