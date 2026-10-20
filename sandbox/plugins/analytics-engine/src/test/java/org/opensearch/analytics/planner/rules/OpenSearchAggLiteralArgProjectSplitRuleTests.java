/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Direct-call unit tests for {@link OpenSearchAggLiteralArgProjectSplitRule}. Builds marked
 * {@code OpenSearchAggregate -> OpenSearchProject} trees by hand and runs the rule through a
 * {@link HepPlanner}, asserting the literal-bearing Project is (or isn't) duplicated into a
 * pinned-above-exchange copy over an unpinned physical-only copy.
 */
public class OpenSearchAggLiteralArgProjectSplitRuleTests extends BasePlannerRulesTests {

    private static final List<String> BACKENDS = List.of("mock-parquet");

    // ── positive cases ─────────────────────────────────────────────────────────

    public void testPercentileLiteralArgProjectIsSplit() {
        // Project(field, $f1=50) under Aggregate(percentile_approx($0,$1)).
        RelNode result = runRule(buildAggregateOverLiteralProject("PERCENTILE_APPROX", List.of(0, 1)));
        assertSplit(result);
    }

    public void testTakeLiteralArgProjectIsSplit() {
        // Project(field, $f1=10) under Aggregate(take($0,$1)).
        RelNode result = runRule(buildAggregateOverLiteralProject("TAKE", List.of(0, 1)));
        assertSplit(result);
    }

    // ── negative cases ──────────────────────────────────────────────────────────

    public void testNonLiteralArgAggregateNotSplit() {
        // SUM is not a literal-config-arg aggregate — even over a Project with a literal column,
        // the rule must not fire (SUM doesn't reference the literal as a config arg).
        RelNode result = runRule(buildAggregateOverLiteralProject("SUM", List.of(0)));
        assertNotSplit(result);
    }

    public void testAllLiteralProjectNotSplit() {
        // A percentile whose Project has NOTHING but the literal to push down: split buys nothing.
        OpenSearchTableScan scan = (OpenSearchTableScan) stubMarkedScan();
        RexLiteral fifty = literal(50);
        OpenSearchProject project = new OpenSearchProject(
            cluster,
            cluster.traitSet(),
            scan,
            List.of(fifty),
            rowType(List.of("$f0"), List.of(SqlTypeName.INTEGER)),
            BACKENDS
        );
        OpenSearchAggregate agg = aggregate("PERCENTILE_APPROX", List.of(0), project);
        assertNotSplit(runRule(agg));
    }

    public void testAlreadyPinnedProjectNotReSplit() {
        // Upper (pinned) copy must not re-fire the rule.
        OpenSearchTableScan scan = (OpenSearchTableScan) stubMarkedScan();
        RexLiteral fifty = literal(50);
        OpenSearchProject pinned = new OpenSearchProject(
            cluster,
            cluster.traitSet(),
            scan,
            List.of(rexBuilder.makeInputRef(scan, 0), fifty),
            rowType(List.of("field", "$f1"), List.of(SqlTypeName.INTEGER, SqlTypeName.INTEGER)),
            BACKENDS,
            true // pinAboveExchange
        );
        OpenSearchAggregate agg = aggregate("PERCENTILE_APPROX", List.of(0, 1), pinned);
        // Already pinned → the rule's guard returns early; the child stays a single Project over the scan.
        RelNode result = runRule(agg);
        RelNode child = result.getInput(0);
        assertTrue("child stays an OpenSearchProject", child instanceof OpenSearchProject);
        assertTrue("child's input stays the scan (not a duplicated Project)", child.getInput(0) instanceof OpenSearchTableScan);
    }

    // ── assertions ──────────────────────────────────────────────────────────────

    /** Asserts the result is Aggregate -> pinned-upper Project -> unpinned-lower Project, with the
     *  literal kept only on the upper copy and the lower copy carrying just the physical column. */
    private void assertSplit(RelNode result) {
        assertTrue("top should be an aggregate", result instanceof OpenSearchAggregate);
        RelNode upper = result.getInput(0);
        assertTrue("upper should be an OpenSearchProject", upper instanceof OpenSearchProject);
        OpenSearchProject upperProject = (OpenSearchProject) upper;
        assertTrue("upper Project must be pinned above the exchange", upperProject.isPinAboveExchange());
        assertTrue("upper Project keeps the literal column", upperProject.getProjects().stream().anyMatch(e -> e instanceof RexLiteral));

        RelNode lower = upperProject.getInput();
        assertTrue("lower should be an OpenSearchProject", lower instanceof OpenSearchProject);
        OpenSearchProject lowerProject = (OpenSearchProject) lower;
        assertFalse("lower Project must NOT be pinned", lowerProject.isPinAboveExchange());
        assertTrue(
            "lower Project must carry only physical (non-literal) columns",
            lowerProject.getProjects().stream().noneMatch(e -> e instanceof RexLiteral)
        );
        assertFalse("lower Project must keep at least one column to push down", lowerProject.getProjects().isEmpty());
    }

    /** Asserts the rule did NOT split: the aggregate's child is a single Project directly over the scan
     *  (no duplicated Project-over-Project stack, nothing pinned). */
    private void assertNotSplit(RelNode result) {
        assertTrue("top should be an aggregate", result instanceof OpenSearchAggregate);
        RelNode child = result.getInput(0);
        assertTrue("child should be an OpenSearchProject", child instanceof OpenSearchProject);
        assertFalse("child Project must not be pinned", ((OpenSearchProject) child).isPinAboveExchange());
        assertTrue("child Project's input must be the scan, not a duplicated Project", child.getInput(0) instanceof OpenSearchTableScan);
    }

    // ── builders ──────────────────────────────────────────────────────────────

    /** Aggregate({@code aggName}(argList)) over {@code Project(field=$0, $f1=50)}. */
    private OpenSearchAggregate buildAggregateOverLiteralProject(String aggName, List<Integer> argList) {
        OpenSearchTableScan scan = (OpenSearchTableScan) stubMarkedScan();
        RexNode fieldRef = rexBuilder.makeInputRef(scan, 0);
        RexLiteral literal = literal(50);
        OpenSearchProject project = new OpenSearchProject(
            cluster,
            cluster.traitSet(),
            scan,
            List.of(fieldRef, literal),
            rowType(List.of("field", "$f1"), List.of(SqlTypeName.INTEGER, SqlTypeName.INTEGER)),
            BACKENDS
        );
        return aggregate(aggName, argList, project);
    }

    private OpenSearchAggregate aggregate(String aggName, List<Integer> argList, OpenSearchProject input) {
        AggregateCall call = "SUM".equals(aggName)
            ? AggregateCall.create(
                SqlStdOperatorTable.SUM,
                false,
                false,
                false,
                List.of(),
                argList,
                -1,
                null,
                RelCollations.EMPTY,
                0,
                input,
                null,
                aggName
            )
            : AggregateCall.create(
                udaf(aggName),
                false,
                false,
                false,
                List.of(),
                argList,
                -1,
                null,
                RelCollations.EMPTY,
                0,
                input,
                // Matches the UDAF's ReturnTypes.ARG0_FORCE_NULLABLE (arg 0 = field, INTEGER).
                typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
                aggName
            );
        return new OpenSearchAggregate(
            cluster,
            cluster.traitSet(),
            input,
            ImmutableBitSet.of(),
            null,
            List.of(call),
            AggregateMode.SINGLE,
            BACKENDS,
            Map.of(),
            Map.of(),
            Collections.singletonList(null)
        );
    }

    /** A minimal user-defined agg function with the given name (two ANY operands). */
    private static SqlAggFunction udaf(String name) {
        return new SqlAggFunction(
            name,
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_FORCE_NULLABLE,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            Optionality.FORBIDDEN
        ) {
        };
    }

    private RexLiteral literal(int value) {
        return (RexLiteral) rexBuilder.makeLiteral(BigDecimal.valueOf(value), typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    }

    private RelDataType rowType(List<String> names, List<SqlTypeName> types) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (int i = 0; i < names.size(); i++) {
            b.add(names.get(i), typeFactory.createSqlType(types.get(i)));
        }
        return b.build();
    }

    /** A marked OpenSearchTableScan (single INTEGER column "field") usable as a rule leaf. */
    private RelNode stubMarkedScan() {
        return new OpenSearchTableScan(
            cluster,
            cluster.traitSet(),
            mockTable("test_index", new String[] { "field" }, new SqlTypeName[] { SqlTypeName.INTEGER }),
            BACKENDS,
            List.of()
        );
    }

    private RelNode runRule(RelNode input) {
        HepProgram program = new HepProgramBuilder().addRuleInstance(new OpenSearchAggLiteralArgProjectSplitRule()).build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(input);
        return planner.findBestExp();
    }
}
