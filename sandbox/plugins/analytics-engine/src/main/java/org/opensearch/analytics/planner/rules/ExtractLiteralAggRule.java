/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Rewrites a {@link LogicalAggregate} that contains one or more {@code LITERAL_AGG}
 * calls into a {@code Project(literal)} layered on top of an aggregate without those
 * calls. Handles both the "single LITERAL_AGG" shape and the "mixed" shape where
 * {@code LITERAL_AGG} appears alongside regular aggregates.
 *
 * <p>{@code LITERAL_AGG} is a Calcite-internal aggregate that always returns the
 * same literal value supplied at plan time, regardless of the rows in each group.
 * It carries a stateless accumulator (state size 0; {@code send} is a no-op,
 * {@code end} returns the literal). Calcite's {@link
 * org.apache.calcite.rel.rules.SubQueryRemoveRule} emits it as an indicator column
 * for {@code NOT IN} / {@code SOME} / {@code ALL} subquery rewrites — the inner
 * aggregate's {@code i = LITERAL_AGG(true)} column ends up NULL when the inner
 * relation is empty (no group fired), letting the outer side distinguish "no
 * match" from "matched with a null".
 *
 * <p>Two shapes:
 * <ul>
 *   <li>Single LITERAL_AGG (NOT IN's per-uid indicator):
 *       <pre>
 *       LogicalAggregate(group=[{0}], i=[LITERAL_AGG(true)])
 *       </pre></li>
 *   <li>Mixed (SOME / ALL / NOT EQUALS subqueries):
 *       <pre>
 *       LogicalAggregate(group=[{}],
 *                        c=[COUNT()], d=[COUNT(deptno)],
 *                        m=[MAX(deptno)], i=[LITERAL_AGG(true)])
 *       </pre></li>
 * </ul>
 *
 * <p>Calcite has no rule to remove {@code LITERAL_AGG}; consuming engines are
 * expected to either implement the accumulator natively (Calcite's own Interpreter
 * and Enumerable codegen do; the SQL JDBC implementor inlines the literal directly
 * into the SQL output) or rewrite the call away. Other Calcite-using projects
 * (Druid, Ignite) wire it directly into their accumulator impls; Apache Impala's
 * {@code ExtractLiteralAgg} rule lowers it to {@code Aggregate + Project} but only
 * fires on the single-LITERAL_AGG shape. This rule extends the Impala approach to
 * the mixed shape by partitioning the agg call list and emitting the literal back
 * at the original output position.
 *
 * <p>Rewrite (mixed shape illustrated):
 * <pre>
 * LogicalAggregate(group=[{0}],
 *                  m=[MAX($2)],            -- agg call 0, output position group_count+0=1
 *                  i=[LITERAL_AGG(true)],  -- agg call 1, output position group_count+1=2
 *                  c=[COUNT()])            -- agg call 2, output position group_count+2=3
 *
 * becomes
 *
 * LogicalProject($0, $1, true_literal, $2)
 *   LogicalAggregate(group=[{0}],
 *                    m=[MAX($2)],          -- agg call 0, output position 1
 *                    c=[COUNT()])          -- agg call 1, output position 2
 *                                          --   (renumbered down by 1 because
 *                                          --    LITERAL_AGG was extracted)
 * </pre>
 *
 * <p>The Project re-emits the schema of the original Aggregate. Group keys pass
 * through. For each original agg call, the Project either references the rebuilt
 * Aggregate's column (regular agg) or substitutes the literal RexNode
 * (LITERAL_AGG). The output column order matches the original so any downstream
 * RexInputRef into the aggregate's row type stays valid.
 *
 * <p>This rule is adapted from Apache Impala's {@code ExtractLiteralAgg}
 * ({@code java/calcite-planner/src/main/java/org/apache/impala/calcite/rules/ExtractLiteralAgg.java})
 * — the operand pattern, the choice to emit a {@code Project} on top of a
 * stripped-down {@code Aggregate}, and the literal source ({@code AggregateCall.rexList.get(0)})
 * are the same. Generalised here to the mixed shape.
 *
 * @opensearch.internal
 */
public class ExtractLiteralAggRule extends RelOptRule {

    public ExtractLiteralAggRule() {
        super(operand(LogicalAggregate.class, none()), "ExtractLiteralAggRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final List<AggregateCall> originalCalls = aggregate.getAggCallList();

        // Partition: for each original agg call, either it stays (regular agg) or
        // it is extracted (LITERAL_AGG) and replaced by a literal in the Project.
        List<AggregateCall> keptCalls = new ArrayList<>(originalCalls.size());
        // Per-original-agg-call slot: null → regular agg (use rebuilt aggregate's column),
        // non-null → literal RexNode to substitute at this position.
        List<RexNode> literalAtSlot = new ArrayList<>(originalCalls.size());
        for (AggregateCall ac : originalCalls) {
            if (ac.getAggregation().getName().equals("LITERAL_AGG")) {
                Preconditions.checkState(
                    ac.rexList.size() == 1 && ac.rexList.get(0) instanceof RexLiteral,
                    "LITERAL_AGG must carry exactly one RexLiteral preOperand"
                );
                literalAtSlot.add(ac.rexList.get(0));
            } else {
                literalAtSlot.add(null);
                keptCalls.add(ac);
            }
        }
        // Nothing to do if no LITERAL_AGG present.
        if (literalAtSlot.stream().noneMatch(Objects::nonNull)) {
            return;
        }

        // Build the rebuilt Aggregate without LITERAL_AGG calls. Same group keys.
        LogicalAggregate newAggregate = LogicalAggregate.create(
            aggregate.getInput(),
            aggregate.getHints(),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            keptCalls
        );

        // Build the wrapping Project that reproduces the original Aggregate's row type.
        RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>();

        // Group keys pass through (positions 0 .. groupCount-1 are identical in
        // the rebuilt Aggregate's row type because group set was preserved).
        int groupCount = aggregate.getGroupSet().cardinality();
        for (int i = 0; i < groupCount; i++) {
            projects.add(rexBuilder.makeInputRef(newAggregate, i));
        }

        // For each original agg call, either reference the rebuilt aggregate's
        // corresponding column (regular agg) or substitute the literal.
        // Walking original positions in order maintains the original schema.
        int rebuiltCallIdx = 0;
        for (RexNode literal : literalAtSlot) {
            if (literal != null) {
                projects.add(literal);
            } else {
                projects.add(rexBuilder.makeInputRef(newAggregate, groupCount + rebuiltCallIdx));
                rebuiltCallIdx++;
            }
        }

        RelNode projectRelNode = LogicalProject.create(
            newAggregate,
            new ArrayList<>(),
            projects,
            aggregate.getRowType().getFieldNames(),
            new HashSet<>()
        );
        call.transformTo(projectRelNode);
    }
}
