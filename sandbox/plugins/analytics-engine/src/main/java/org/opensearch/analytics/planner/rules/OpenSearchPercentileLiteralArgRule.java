/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchProject;

import java.util.ArrayList;
import java.util.List;

/**
 * Post-marking rule: when an aggregate's literal CONFIG arg (e.g. percentile's {@code 50}) lives as
 * a column on the input {@link OpenSearchProject}, DUPLICATE that Project into two straddling the
 * (yet-to-be-inserted) ExchangeReducer:
 *
 * <pre>
 *   Aggregate(percentile_approx($0,$1,$2))
 *     OpenSearchProject(ParamPrice=$0, $f1=50, $f2=FLAG)   ← UPPER, pinned: literal stays with agg
 *       [ExchangeReducer inserted here by CBO]
 *         OpenSearchProject(ParamPrice=$59)                ← LOWER, unpinned: physical-only → pushdown
 *           Scan
 * </pre>
 *
 * <p><b>Why.</b> PPL {@code percentile(field, 50)} lowers to {@code percentile_approx($0,$1,$2)}
 * over {@code Project(field, $f1=50, $f2=FLAG)} — the percentile literal {@code 50} (and a
 * type-flag) are materialized as Project columns referenced by ordinal. The ExchangeReducer
 * width-cost pushes that single Project below the gather; the literal then crosses the Exchange as a
 * data column and the DataFusion substrait converter — which recovers the literal from the
 * directly-attached Project — can no longer reach it, so DataFusion errors with
 * "Percentile value ... must be a literal".
 *
 * <p><b>Fix.</b> Keep a copy of the literal-bearing Project directly under the aggregate, flagged
 * {@link OpenSearchProject#isPinAboveExchange()} so its {@code computeSelfCost} forces the ER below
 * it (it stays in the coordinator fragment, literal reachable). A second, unpinned, physical-only
 * Project pushes below the gather and narrows the scan — projection-pushdown preserved. The
 * aggregate's {@code argList} is untouched (literal is still a column on the upper Project), so no
 * operand renumbering and no return-type-inference change.
 *
 * <p><b>Placement.</b> Runs AFTER marking (operates on {@code OpenSearch*} nodes) and before CBO.
 * {@code PROJECT_MERGE} lives in the pre-marking pushdown phase, so it cannot re-fuse the two
 * Projects; the ER inserted by CBO lands between them and the split sticks.
 *
 * <p><b>Scope.</b> Only {@code PERCENTILE_APPROX} (PPL percentile / median / percentile_approx /
 * pNN). Fires only when the percentile call references a literal column AND the Project has a
 * non-literal column to push down — otherwise the split buys nothing.
 *
 * @opensearch.internal
 */
public class OpenSearchPercentileLiteralArgRule extends RelOptRule {

    private static final String PERCENTILE_APPROX = "PERCENTILE_APPROX";

    public OpenSearchPercentileLiteralArgRule() {
        super(operand(OpenSearchAggregate.class, operand(OpenSearchProject.class, none())), "OpenSearchPercentileLiteralArgRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        OpenSearchProject project = call.rel(1);

        // Already split — the upper copy is pinned; don't re-fire on it.
        if (project.isPinAboveExchange()) {
            return;
        }

        List<RexNode> exprs = project.getProjects();

        // Does any percentile call reference a literal column on this Project?
        boolean referencesLiteralArg = false;
        for (AggregateCall ac : aggregate.getAggCallList()) {
            if (!PERCENTILE_APPROX.equalsIgnoreCase(ac.getAggregation().getName())) {
                continue;
            }
            for (int arg : ac.getArgList()) {
                if (arg < exprs.size() && exprs.get(arg) instanceof RexLiteral) {
                    referencesLiteralArg = true;
                    break;
                }
            }
        }
        if (!referencesLiteralArg) {
            return;
        }

        // Lower Project: keep only the non-literal (RexInputRef-style) columns — these are what must
        // cross the gather. Pure literals carry no scan I/O and are re-materialized on the upper copy.
        int[] remap = new int[exprs.size()];
        List<RexNode> lowerExprs = new ArrayList<>();
        List<String> origNames = project.getRowType().getFieldNames();
        List<String> lowerNames = new ArrayList<>();
        int next = 0;
        for (int i = 0; i < exprs.size(); i++) {
            if (exprs.get(i) instanceof RexLiteral) {
                remap[i] = -1;
            } else {
                remap[i] = next++;
                lowerExprs.add(exprs.get(i));
                lowerNames.add(origNames.get(i));
            }
        }
        // Nothing to push down (all columns are literals): the split saves nothing.
        if (lowerExprs.isEmpty()) {
            return;
        }

        RelOptCluster cluster = aggregate.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();

        RelDataType lowerRowType = buildRowType(typeFactory, project.getRowType(), remap);
        OpenSearchProject lower = new OpenSearchProject(
            cluster,
            project.getTraitSet(),
            project.getInput(),
            lowerExprs,
            lowerRowType,
            project.getViableBackends()
        );

        // Upper Project: reconstruct the original row type 1:1. RexInputRefs reindex onto the lower
        // Project's output; literals stay inline. The aggregate above references unchanged ordinals.
        List<RexNode> upperExprs = new ArrayList<>(exprs.size());
        for (int i = 0; i < exprs.size(); i++) {
            RexNode expr = exprs.get(i);
            if (expr instanceof RexLiteral) {
                upperExprs.add(expr);
            } else {
                // Non-literal kept on the lower Project at slot remap[i]; reference it by that slot.
                int slot = remap[i];
                upperExprs.add(rexBuilder.makeInputRef(lowerRowType.getFieldList().get(slot).getType(), slot));
            }
        }
        OpenSearchProject upper = new OpenSearchProject(
            cluster,
            project.getTraitSet(),
            lower,
            upperExprs,
            project.getRowType(),
            project.getViableBackends(),
            true
        );

        call.transformTo(aggregate.copy(aggregate.getTraitSet(), List.of(upper)));
    }

    /** Row type over the columns kept by {@code remap} (entry &gt;= 0), preserving order. */
    private static RelDataType buildRowType(RelDataTypeFactory typeFactory, RelDataType orig, int[] remap) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        List<RelDataTypeField> fields = orig.getFieldList();
        for (int i = 0; i < remap.length; i++) {
            if (remap[i] >= 0) {
                builder.add(fields.get(i).getName(), fields.get(i).getType());
            }
        }
        return builder.build();
    }
}
