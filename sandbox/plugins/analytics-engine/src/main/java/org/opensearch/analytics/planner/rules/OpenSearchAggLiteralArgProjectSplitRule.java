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
 * Post-marking rule: duplicates the input {@link OpenSearchProject} when an aggregate's literal CONFIG
 * arg (percentile's pct, take's N) lives on it as a column, so the literal can't be severed from the
 * aggregate by projection pushdown:
 *
 * <pre>
 *   Aggregate(percentile_approx($0,$1,$2))
 *     OpenSearchProject(ParamPrice=$0, $f1=50, $f2=FLAG)   ← UPPER, pinned: literal stays with agg
 *       [ExchangeReducer inserted here by CBO]
 *         OpenSearchProject(ParamPrice=$59)                ← LOWER, unpinned: physical-only → pushdown
 *           Scan
 * </pre>
 *
 * <p>The DataFusion converter re-inlines the literal from the directly-attached Project. Without the split,
 * width-cost pushes the single literal-bearing Project below the gather, the converter can't reach the
 * literal, and percentile errors "must be a literal" / take returns an empty array. The pinned upper copy
 * ({@link OpenSearchProject#isPinAboveExchange()}, infinite cost unless input is gathered) keeps the literal
 * in the coordinator fragment; the unpinned lower copy narrows the scan. {@code argList} is untouched.
 *
 * <p>Runs after marking, before CBO — {@code PROJECT_MERGE} (pre-marking) can't re-fuse the copies, and the
 * ER lands between them. Scope = {@code PERCENTILE_APPROX} and {@code TAKE}, the only aggregates that
 * materialize a literal config arg as a Project column (FIRST/LAST drop their optional N; others carry no
 * literal). Fires only when such a call references a literal column and the Project also has a non-literal
 * column to push down.
 *
 * @opensearch.internal
 */
public class OpenSearchAggLiteralArgProjectSplitRule extends RelOptRule {

    // Aggregates whose literal config arg the DataFusion converter re-inlines from the attached Project.
    private static final java.util.Set<String> LITERAL_ARG_AGGS = java.util.Set.of("PERCENTILE_APPROX", "TAKE");

    public OpenSearchAggLiteralArgProjectSplitRule() {
        super(operand(OpenSearchAggregate.class, operand(OpenSearchProject.class, none())), "OpenSearchAggLiteralArgProjectSplitRule");
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
            if (!LITERAL_ARG_AGGS.contains(ac.getAggregation().getName().toUpperCase(java.util.Locale.ROOT))) {
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
