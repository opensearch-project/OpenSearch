/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Direct-call unit tests for the post-Volcano rewrite path. Constructs FINAL
 * {@link OpenSearchAggregate} trees by hand and asserts how
 * {@link DistributedAggregateRewriter#rewrite} (and its transformer chain) reshape them.
 */
public class DistributedAggregateRewriterTests extends BasePlannerRulesTests {

    /**
     * No StageInputScan, no decompositions, no literals → rewriter returns the same instance.
     * Verifies the orchestrator's fast-path early return.
     */
    public void testRewriteNoOpWhenNothingToAdapt() {
        OpenSearchAggregate finalAgg = buildFinalAggregateOverScan();
        // Wrap an exchange reducer over a plain scan so the orchestrator's prefix check
        // (must have a StageInputScan beneath the reducer) fails — early exit, no work done.
        RelNode result = DistributedAggregateRewriter.rewrite(finalAgg);
        assertSame("rewrite must return finalAgg unchanged when input chain isn't a StageInputScan", finalAgg, result);
    }

    /**
     * APPROX_COUNT_DISTINCT's intermediate field is VARBINARY (sketch), but the StageInputScan
     * was constructed declaring BIGINT (the user-facing return type). The transformer should
     * re-type the StageInputScan column to VARBINARY.
     */
    public void testExchangeTypeOverrideRetypesApproxCountDistinctSketch() {
        // PARTIAL output schema as declared (BIGINT for the agg column).
        RelDataType groupKeyType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType declaredAggType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), false);
        RelDataType stageInputType = typeFactory.builder().add("k", groupKeyType).add("dc", declaredAggType).build();

        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            cluster.traitSet(),
            0,
            stageInputType,
            List.of("mock-parquet"),
            List.of()
        );
        OpenSearchExchangeReducer reducer = new OpenSearchExchangeReducer(cluster, cluster.traitSet(), stageInput, List.of("mock-parquet"));

        // FINAL aggregate sitting on top of the exchange. APPROX_COUNT_DISTINCT keeps function
        // identity (engine-native merge) — its declared return type is BIGINT (user-facing).
        AggregateCall dcCall = AggregateCall.create(
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            false,
            List.of(1),
            -1,
            reducer,
            declaredAggType,
            "dc"
        );
        // intermediateFields is parallel to aggCallList (one entry per agg call, not per row column).
        List<IntermediateField> intermediateFields = List.of(classify(SqlStdOperatorTable.APPROX_COUNT_DISTINCT));
        OpenSearchAggregate finalAgg = new OpenSearchAggregate(
            cluster,
            cluster.traitSet(),
            reducer,
            ImmutableBitSet.of(0),
            null,
            List.of(dcCall),
            AggregateMode.FINAL,
            List.of("mock-parquet"),
            Map.of(),
            Map.of(),
            intermediateFields
        );

        RelNode result = DistributedAggregateRewriter.rewrite(finalAgg);

        // The exchange below FINAL must now wrap a StageInputScan whose 'dc' column is VARBINARY.
        assertNotSame("rewrite should return a new FINAL when the exchange type was overridden", finalAgg, result);
        OpenSearchExchangeReducer newReducer = (OpenSearchExchangeReducer) result.getInput(0);
        OpenSearchStageInputScan newStageInput = (OpenSearchStageInputScan) newReducer.getInput(0);
        RelDataType retypedDc = newStageInput.getRowType().getFieldList().get(1).getType();
        assertEquals(SqlTypeName.VARBINARY, retypedDc.getSqlTypeName());
    }

    /**
     * STATE_EXPANDING aggregates carry literal config args (e.g. {@code take(field, 10)}'s
     * {@code 10}) on FINAL's {@code finalExtraLiteralArgs} stash. The transformer should
     * insert an {@link OpenSearchProject} that re-introduces those literals as columns.
     */
    public void testLiteralProjectInsertsCapturedLiterals() {
        RelDataType stateType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType stageInputType = typeFactory.builder().add("take_state", stateType).build();
        OpenSearchStageInputScan stageInput = new OpenSearchStageInputScan(
            cluster,
            cluster.traitSet(),
            0,
            stageInputType,
            List.of("mock-parquet"),
            List.of()
        );
        OpenSearchExchangeReducer reducer = new OpenSearchExchangeReducer(cluster, cluster.traitSet(), stageInput, List.of("mock-parquet"));

        // We don't need a real TAKE SqlAggFunction for this check — a SUM call will do, since
        // the transformer only reads finalExtraLiteralArgs and projects the literals; FINAL's
        // own aggCall list isn't introspected by the transformer itself. Use the long-form
        // create with null return type so Calcite infers (avoids typeMatchesInferred drift on
        // empty group).
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            0,
            reducer,
            null,
            "take_state"
        );

        // One captured literal for call 0: integer 10.
        RexLiteral ten = (RexLiteral) rexBuilder.makeLiteral(BigDecimal.valueOf(10), typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        Map<Integer, List<RexLiteral>> extras = Map.of(0, List.of(ten));

        OpenSearchAggregate finalAgg = new OpenSearchAggregate(
            cluster,
            cluster.traitSet(),
            reducer,
            ImmutableBitSet.of(),
            null,
            List.of(sumCall),
            AggregateMode.FINAL,
            List.of("mock-parquet"),
            Map.of(),
            extras,
            Collections.singletonList(null)
        );

        RelNode result = DistributedAggregateRewriter.rewrite(finalAgg);

        // FINAL's input should now be an OpenSearchProject (inserted by insertLiteralProject).
        assertNotSame("rewrite should return a new FINAL when literals were re-introduced", finalAgg, result);
        OpenSearchProject project = (OpenSearchProject) result.getInput(0);
        // Project has the original column plus the appended literal column.
        assertEquals(2, project.getRowType().getFieldCount());
        assertEquals("take_state", project.getRowType().getFieldList().get(0).getName());
        assertTrue(
            "literal column name should follow the $lit_call prefix",
            project.getRowType().getFieldList().get(1).getName().startsWith("$lit_call")
        );
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** Small helper: classify a single SqlAggFunction via the SPI; null when no decomposition. */
    private static IntermediateField classify(SqlAggFunction sqlFn) {
        AggregateFunction enumFn = AggregateFunction.fromSqlAggFunction(sqlFn);
        List<IntermediateField> fields = enumFn.intermediateFields();
        if (fields == null) return null;
        return fields.get(0);
    }

    /**
     * Builds a FINAL aggregate over a plain scan (no exchange reducer). Used as a degenerate
     * input shape — the orchestrator's StageInputScan check should reject it and return the
     * input unchanged.
     */
    private OpenSearchAggregate buildFinalAggregateOverScan() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        return new OpenSearchAggregate(
            cluster,
            cluster.traitSet(),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(sumCall(scan)),
            AggregateMode.FINAL,
            List.of("mock-parquet"),
            Map.of(),
            Map.of(),
            Collections.singletonList(null)
        );
    }
}
