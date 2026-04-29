/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for project rule: scalar function validation, opaque operation
 * handling, and painless script delegation.
 */
public class ProjectRuleTests extends BasePlannerRulesTests {

    private static final SqlFunction PAINLESS = new SqlFunction(
        "painless",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction HIGHLIGHT = new SqlFunction(
        "highlight",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    // ---- Simple projection ----

    public void testSimpleFieldProjection() {
        OpenSearchProject result = runProject(
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        for (RexNode expr : result.getProjects()) {
            assertFalse("Field ref should not be annotated", expr instanceof AnnotatedProjectExpression);
        }
    }

    public void testPassthroughProjectionSucceedsWithoutProjectCapability() {
        // A backend that declares NO ProjectCapability should still execute a passthrough
        // projection (only field refs). Verifies the short-circuit in OpenSearchProjectRule.onMatch
        // that skips the backend-refinement gate when no RexCall needs evaluation.
        OpenSearchProject result = runProject(
            MockDataFusionBackend.PARQUET_DATA_FORMAT,
            List.of(new MockDataFusionBackend(), LUCENE),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        for (RexNode expr : result.getProjects()) {
            assertFalse("Passthrough expressions must not be annotated", expr instanceof AnnotatedProjectExpression);
        }
    }

    public void testExpressionProjectionStillRequiresCapabilityWithoutDeclaration() {
        // Negative guard: the short-circuit must apply only to passthrough. If a RexCall is
        // present and the backend declares no matching scalar ProjectCapability, the rule must
        // still throw — otherwise a later refactor could silently loosen the gate too much.
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(castExpr), List.of("casted"));
        PlannerContext context = buildContext("parquet", nameValueFields(), List.of(new MockDataFusionBackend(), LUCENE));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("No backend supports scalar function"));
    }

    // ---- Scalar functions ----

    public void testSupportedScalarFunction() {
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        OpenSearchProject result = runProject(castExpr);
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    public void testUnsupportedScalarFunctionErrors() {
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(castExpr), List.of("casted"));
        PlannerContext context = buildContext("parquet", nameValueFields());

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("No backend supports scalar function"));
    }

    // ---- Delegation ----

    public void testPainlessDelegationFromDataFusionToLucene() {
        OpenSearchProject result = runProject(
            "parquet",
            delegationBackends("painless"),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(result.getViableBackends().contains(MockLuceneBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockLuceneBackend.NAME);
    }

    public void testPainlessErrorsWithoutDelegation() {
        // Lucene supports painless but no delegation configured
        MockLuceneBackend luceneWithPainless = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };
        RelOptTable table = mockTable("test_index", new String[] { "name" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(painlessExpr), List.of("scripted_field"));
        PlannerContext context = buildContext(
            "parquet",
            Map.of("name", Map.of("type", "keyword")),
            List.of(DATAFUSION, luceneWithPainless)
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    public void testMixedFieldAndPainlessWithDelegation() {
        OpenSearchProject result = runProject(
            "parquet",
            delegationBackends("painless"),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Field ref should not be annotated", result.getProjects().get(0) instanceof AnnotatedProjectExpression);
        assertAnnotation(result.getProjects().get(1), MockLuceneBackend.NAME);
    }

    public void testHighlightDelegation() {
        OpenSearchProject result = runProject(
            "parquet",
            delegationBackends("painless", "highlight"),
            rexBuilder.makeCall(HIGHLIGHT, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockLuceneBackend.NAME);
    }

    // ---- Opaque natively supported ----

    public void testOpaqueOperationSupportedNatively() {
        MockDataFusionBackend dfWithPainless = new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return combine(
                    scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class)),
                    opaqueCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), "painless")
                );
            }
        };
        OpenSearchProject result = runProject(
            "parquet",
            List.of(dfWithPainless, LUCENE),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    // ---- Nested expressions ----

    public void testNestedScalarFunctions() {
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0)
        );
        RexNode plusExpr = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            castExpr,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        OpenSearchProject result = runProject(plusExpr);
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    // ---- Mixed backends in one projection ----

    public void testMixedBackendsInProjection() {
        MockDataFusionBackend dfWithScalarsAndDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }

            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };

        RexNode fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );

        OpenSearchProject result = runProject(
            "parquet",
            List.of(dfWithScalarsAndDelegation, luceneAccepting),
            fieldRef,
            painlessExpr,
            castExpr
        );

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Field ref should not be annotated", result.getProjects().get(0) instanceof AnnotatedProjectExpression);
        assertAnnotation(result.getProjects().get(1), MockLuceneBackend.NAME);
        assertAnnotation(result.getProjects().get(2), MockDataFusionBackend.NAME);
    }

    public void testScalarWrappingOpaqueOp() {
        MockDataFusionBackend dfWithScalarsAndDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }

            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };

        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode plusExpr = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.INTEGER), painlessExpr),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );

        OpenSearchProject result = runProject("parquet", List.of(dfWithScalarsAndDelegation, luceneAccepting), plusExpr);

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
        AnnotatedProjectExpression outerAnnotation = (AnnotatedProjectExpression) result.getProjects().get(0);
        RexNode innerPlus = outerAnnotation.getOriginal();
        assertTrue(innerPlus instanceof RexCall);
        RexNode castOperand = ((RexCall) innerPlus).getOperands().get(0);
        assertAnnotation(castOperand, MockDataFusionBackend.NAME);
        RexNode painlessInside = ((AnnotatedProjectExpression) castOperand).getOriginal();
        assertTrue(painlessInside instanceof RexCall);
        RexNode painlessArg = ((RexCall) painlessInside).getOperands().get(0);
        assertAnnotation(painlessArg, MockLuceneBackend.NAME);
    }

    // ---- Delegation edge cases ----

    public void testDelegationFailsWhenAcceptorLacksOpaqueOp() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless", "highlight");
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        // Third backend declares "suggest" so isOpaqueOperation returns true, but no acceptor handles it
        MockLuceneBackend thirdBackend = new MockLuceneBackend() {
            @Override
            public String name() {
                return "mock-third";
            }

            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "suggest");
            }
        };

        SqlFunction suggest = new SqlFunction(
            "suggest",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RelOptTable table = mockTable("test_index", new String[] { "name" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        RexNode suggestExpr = rexBuilder.makeCall(suggest, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(suggestExpr), List.of("sg"));
        PlannerContext context = buildContext(
            "parquet",
            Map.of("name", Map.of("type", "keyword")),
            List.of(dfWithDelegation, luceneAccepting, thirdBackend)
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    public void testDelegationFailsWhenAcceptorRejectsDelegationType() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        // Lucene supports painless but doesn't accept PROJECT delegation
        MockLuceneBackend luceneWithPainlessNoAccept = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };

        RelOptTable table = mockTable("test_index", new String[] { "name" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(painlessExpr), List.of("scripted"));
        PlannerContext context = buildContext(
            "parquet",
            Map.of("name", Map.of("type", "keyword")),
            List.of(dfWithDelegation, luceneWithPainlessNoAccept)
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    // ---- Composed pipeline shapes ----

    /**
     * Project(Filter(Scan)) — verifies annotation propagation through filter→project
     * at every level.
     */
    public void testProjectOnFilteredScan() {
        RelNode filter = makeFilter(
            stubScan(
                mockTable("test_index", new String[] { "name", "value" }, new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER })
            ),
            makeEquals(1, SqlTypeName.INTEGER, 100)
        );
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        List<String> fieldNames = List.of("col_0");
        LogicalProject project = LogicalProject.create(filter, List.of(), List.of(castExpr), fieldNames);
        PlannerContext context = buildContext("parquet", nameValueFields(), List.of(dfWithScalarFunctions(), LUCENE));
        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchProject.class, OpenSearchFilter.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        assertAnnotation(((OpenSearchProject) result).getProjects().get(0), MockDataFusionBackend.NAME);
    }

    /**
     * Project(Agg(Scan)) — single shard: Project → Aggregate(SINGLE) → Scan.
     * Multi shard: Project → Aggregate(FINAL) → ExchangeReducer → Aggregate(PARTIAL) → Scan.
     */
    public void testProjectOnAggregateScanSingleShard() {
        RelNode result = runProjectOnAgg(1);
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchProject.class, OpenSearchAggregate.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        assertAnnotation(((OpenSearchProject) result).getProjects().get(0), MockDataFusionBackend.NAME);
    }

    public void testProjectOnAggregateScanMultiShard() {
        RelNode result = runProjectOnAgg(2);
        assertPipelineViableBackends(
            result,
            List.of(
                OpenSearchProject.class,
                OpenSearchAggregate.class,
                OpenSearchExchangeReducer.class,
                OpenSearchAggregate.class,
                OpenSearchTableScan.class
            ),
            Set.of(MockDataFusionBackend.NAME)
        );
        assertAnnotation(((OpenSearchProject) result).getProjects().get(0), MockDataFusionBackend.NAME);
    }

    private RelNode runProjectOnAgg(int shardCount) {
        RelNode agg = makeAggregate(
            stubScan(
                mockTable("test_index", new String[] { "name", "value" }, new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER })
            ),
            sumCall()
        );
        // Cast SUM result (field 1, INTEGER→VARCHAR) — genuine RexCall that gets annotated
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(agg.getRowType().getFieldList().get(1).getType(), 1)
        );
        LogicalProject project = LogicalProject.create(agg, List.of(), List.of(castExpr), List.of("col_0"));
        PlannerContext context = buildContext("parquet", shardCount, nameValueFields(), List.of(dfWithScalarFunctions(), LUCENE));
        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan ({} shard(s)):\n{}", shardCount, RelOptUtil.toString(result));
        return result;
    }

    // ---- Helpers ----

    private static Map<String, Map<String, Object>> nameValueFields() {
        return Map.of("name", Map.of("type", "keyword"), "value", Map.of("type", "integer"));
    }

    private void assertAnnotation(RexNode expr, String expectedBackend) {
        assertTrue(
            "Expected AnnotatedProjectExpression, got " + expr.getClass().getSimpleName(),
            expr instanceof AnnotatedProjectExpression
        );
        assertTrue(((AnnotatedProjectExpression) expr).getViableBackends().contains(expectedBackend));
    }

    private OpenSearchProject runProject(RexNode... exprs) {
        return runProject("parquet", List.of(dfWithScalarFunctions(), LUCENE), Set.of(MockDataFusionBackend.NAME), exprs);
    }

    private OpenSearchProject runProject(String format, List<AnalyticsSearchBackendPlugin> backends, RexNode... exprs) {
        return runProject(format, backends, Set.of(MockDataFusionBackend.NAME), exprs);
    }

    private OpenSearchProject runProject(
        String format,
        List<AnalyticsSearchBackendPlugin> backends,
        Set<String> expectedViable,
        RexNode... exprs
    ) {
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "name", "value" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < exprs.length; i++)
            fieldNames.add("col_" + i);
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(exprs), fieldNames);
        PlannerContext context = buildContext(format, nameValueFields(), backends);
        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchProject", result instanceof OpenSearchProject);
        assertPipelineViableBackends(result, List.of(OpenSearchProject.class, OpenSearchTableScan.class), expectedViable);
        return (OpenSearchProject) result;
    }

    /** DF backend with all scalar functions declared. */
    private MockDataFusionBackend dfWithScalarFunctions() {
        return new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }
        };
    }

    /** DF (with PROJECT delegation) + Lucene (accepting, with given opaque ops). */
    private List<AnalyticsSearchBackendPlugin> delegationBackends(String... opaqueOps) {
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }

            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), opaqueOps);
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        return List.of(df, lucene);
    }

    @SafeVarargs
    private static Set<ProjectCapability> combine(Set<ProjectCapability>... sets) {
        Set<ProjectCapability> result = new HashSet<>();
        for (Set<ProjectCapability> set : sets)
            result.addAll(set);
        return result;
    }

    private static Set<ProjectCapability> scalarCaps(Set<String> formats, Set<ScalarFunction> functions) {
        Set<ProjectCapability> caps = new HashSet<>();
        for (ScalarFunction func : functions) {
            caps.add(new ProjectCapability.Scalar(func, Set.of(FieldType.values()), formats, true));
        }
        return caps;
    }

    private static Set<ProjectCapability> opaqueCaps(Set<String> formats, String... names) {
        Set<ProjectCapability> caps = new HashSet<>();
        for (String name : names)
            caps.add(new ProjectCapability.Opaque(name, formats));
        return caps;
    }
}
