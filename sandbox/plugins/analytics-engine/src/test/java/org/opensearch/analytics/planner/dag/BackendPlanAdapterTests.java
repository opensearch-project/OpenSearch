/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link BackendPlanAdapter} — verifies per-function adapters are applied
 * correctly between plan forking and fragment conversion.
 */
public class BackendPlanAdapterTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(BackendPlanAdapterTests.class);

    private static final SqlFunction SIN_FUNCTION = new SqlFunction(
        "SIN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    );

    private final ScalarFunctionAdapter sinCastAdapter = (call, fieldStorage) -> {
        List<RexNode> adaptedOperands = new ArrayList<>(call.getOperands().size());
        boolean changed = false;
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef inputRef) {
                FieldStorageInfo info = FieldStorageInfo.resolve(fieldStorage, inputRef.getIndex());
                if (info.getFieldType() == FieldType.INTEGER || info.getFieldType() == FieldType.LONG) {
                    adaptedOperands.add(rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.DOUBLE), operand));
                    changed = true;
                    continue;
                }
            }
            adaptedOperands.add(operand);
        }
        return changed ? call.clone(call.getType(), adaptedOperands) : call;
    };

    private RexCall adaptSinFilter(SqlTypeName operandType, Map<String, Map<String, Object>> fields) {
        return adaptSinFilter(operandType, fields, fields.keySet().toArray(String[]::new), null);
    }

    private RexCall adaptSinFilter(
        SqlTypeName operandType,
        Map<String, Map<String, Object>> fields,
        String[] fieldNames,
        SqlTypeName[] fieldTypes
    ) {
        MockDataFusionBackend dfWithAdapter = new MockDataFusionBackend() {
            @Override
            protected Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
                return Map.of(ScalarFunction.SIN, sinCastAdapter);
            }
        };

        PlannerContext context = buildContext("parquet", 1, fields, List.of(dfWithAdapter));

        RexNode sinCall = rexBuilder.makeCall(SIN_FUNCTION, rexBuilder.makeInputRef(typeFactory.createSqlType(operandType), 0));
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            sinCall,
            rexBuilder.makeLiteral(0.5, typeFactory.createSqlType(SqlTypeName.DOUBLE), true)
        );
        RelOptTable table = fieldTypes != null ? mockTable("test_index", fieldNames, fieldTypes) : mockTable("test_index", fieldNames);
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);

        RelNode marked = runPlanner(filter, context);
        LOGGER.info("Marked:\n{}", RelOptUtil.toString(marked));

        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());

        StagePlan plan = dag.rootStage().getPlanAlternatives().getFirst();
        OpenSearchFilter adaptedFilter = (OpenSearchFilter) plan.resolvedFragment();
        return findCallByName(adaptedFilter.getCondition(), "SIN");
    }

    /** SIN(integer_column) should be adapted to SIN(CAST(integer_column AS DOUBLE)). */
    public void testSinAdapterInsertsCastForIntegerField() {
        RexCall sinCall = adaptSinFilter(SqlTypeName.INTEGER, intFields());
        assertNotNull("SIN call should exist in adapted condition", sinCall);
        assertEquals("SIN operand should be CAST after adaptation", SqlKind.CAST, sinCall.getOperands().getFirst().getKind());
    }

    /** SIN(double_column) should NOT be adapted — no CAST needed. */
    public void testSinAdapterNoOpForDoubleField() {
        Map<String, Map<String, Object>> doubleFields = Map.of("price", Map.of("type", "double"), "amount", Map.of("type", "double"));
        RexCall sinCall = adaptSinFilter(
            SqlTypeName.DOUBLE,
            doubleFields,
            new String[] { "price", "amount" },
            new SqlTypeName[] { SqlTypeName.DOUBLE, SqlTypeName.DOUBLE }
        );
        assertNotNull("SIN call should exist in adapted condition", sinCall);
        assertNotSame("SIN operand should NOT be CAST for double field", SqlKind.CAST, sinCall.getOperands().getFirst().getKind());
    }

    /** SIN(integer_column) in a project should also get CAST inserted. */
    public void testSinAdapterInProjectInsertsCastForIntegerField() {
        MockDataFusionBackend dfWithAdapter = new MockDataFusionBackend() {
            @Override
            protected Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
                return Map.of(ScalarFunction.SIN, sinCastAdapter);
            }

            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return Set.of(
                    new ProjectCapability.Scalar(
                        ScalarFunction.SIN,
                        Set.of(FieldType.INTEGER, FieldType.DOUBLE),
                        Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT),
                        false
                    )
                );
            }
        };

        PlannerContext context = buildContext("parquet", 1, intFields(), List.of(dfWithAdapter));

        RexNode sinExpr = rexBuilder.makeCall(SIN_FUNCTION, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0));
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(sinExpr), List.of("sin_status"));

        RelNode marked = runPlanner(project, context);
        LOGGER.info("Marked project:\n{}", RelOptUtil.toString(marked));

        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());

        StagePlan plan = dag.rootStage().getPlanAlternatives().getFirst();
        // Find SIN call in the project expressions
        RexCall sinCall = null;
        if (plan.resolvedFragment() instanceof org.opensearch.analytics.planner.rel.OpenSearchProject adaptedProject) {
            for (RexNode expr : adaptedProject.getProjects()) {
                sinCall = findCallByName(expr, "SIN");
                if (sinCall != null) break;
            }
        }
        assertNotNull("SIN call should exist in adapted project", sinCall);
        assertEquals("SIN operand should be CAST after adaptation in project", SqlKind.CAST, sinCall.getOperands().getFirst().getKind());
    }

    /** Filter with SIN (adapted) AND ABS (no adapter) — SIN gets CAST, ABS unchanged. */
    public void testMixedAdaptedAndNonAdaptedFunctions() {
        MockDataFusionBackend dfWithSinAdapterOnly = new MockDataFusionBackend() {
            @Override
            protected Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
                return Map.of(ScalarFunction.SIN, sinCastAdapter);
            }
        };

        PlannerContext context = buildContext("parquet", 1, intFields(), List.of(dfWithSinAdapterOnly));

        RexNode sinGt = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeCall(SIN_FUNCTION, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0)),
            rexBuilder.makeLiteral(0.5, typeFactory.createSqlType(SqlTypeName.DOUBLE), true)
        );
        RexNode absGt = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeCall(SqlStdOperatorTable.ABS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, sinGt, absGt);
        LogicalFilter filter = LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), condition);

        RelNode marked = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());

        StagePlan plan = dag.rootStage().getPlanAlternatives().getFirst();
        OpenSearchFilter adaptedFilter = (OpenSearchFilter) plan.resolvedFragment();
        RexCall sinCall = findCallByName(adaptedFilter.getCondition(), "SIN");
        RexCall absCall = findCallByName(adaptedFilter.getCondition(), "ABS");
        assertNotNull("SIN call should exist in adapted condition", sinCall);
        assertNotNull("ABS call should exist in adapted condition", absCall);
        assertEquals("SIN operand should be CAST after adaptation", SqlKind.CAST, sinCall.getOperands().getFirst().getKind());
        assertEquals("ABS operand should remain INPUT_REF without adapter", SqlKind.INPUT_REF, absCall.getOperands().getFirst().getKind());
    }

    /** No adapters registered — plan should pass through completely unchanged. */
    public void testNoAdaptersRegisteredLeavesEverythingUnchanged() {
        PlannerContext context = buildContext("parquet", 1, intFields());

        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeCall(SIN_FUNCTION, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0)),
            rexBuilder.makeLiteral(0.5, typeFactory.createSqlType(SqlTypeName.DOUBLE), true)
        );
        LogicalFilter filter = LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), condition);

        RelNode marked = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());

        StagePlan plan = dag.rootStage().getPlanAlternatives().getFirst();
        OpenSearchFilter adaptedFilter = (OpenSearchFilter) plan.resolvedFragment();
        RexCall sinCall = findCallByName(adaptedFilter.getCondition(), "SIN");
        assertNotNull("SIN call should exist in condition", sinCall);
        assertEquals(
            "SIN operand should remain INPUT_REF with no adapters registered",
            SqlKind.INPUT_REF,
            sinCall.getOperands().getFirst().getKind()
        );
    }

    private static RexCall findCallByName(RexNode node, String name) {
        if (node instanceof AnnotatedPredicate annotated) return findCallByName(annotated.getOriginal(), name);
        if (node instanceof RexCall call) {
            if (call.getOperator().getName().equalsIgnoreCase(name)) return call;
            for (RexNode operand : call.getOperands()) {
                RexCall found = findCallByName(operand, name);
                if (found != null) return found;
            }
        }
        return null;
    }
}
