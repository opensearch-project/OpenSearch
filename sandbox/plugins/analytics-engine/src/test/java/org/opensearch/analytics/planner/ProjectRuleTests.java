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
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.OperatorCapability;
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
        "painless", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000,
        null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction HIGHLIGHT = new SqlFunction(
        "highlight", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000,
        null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION
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
        RelOptTable table = mockTable("test_index",
            new String[]{"name", "value"}, new SqlTypeName[]{SqlTypeName.VARCHAR, SqlTypeName.INTEGER});
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(castExpr), List.of("casted"));
        PlannerContext context = buildContext("parquet",
            Map.of("name", Map.of("type", "keyword"), "value", Map.of("type", "integer")));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("No backend supports scalar function"));
    }

    // ---- Delegation ----

    public void testPainlessDelegationFromDataFusionToLucene() {
        OpenSearchProject result = runProject("parquet", delegationBackends("painless"),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(result.getViableBackends().contains(MockLuceneBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockLuceneBackend.NAME);
    }

    public void testPainlessErrorsWithoutDelegation() {
        BackendCapabilityProvider luceneWithPainless = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return LUCENE.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };
        RelOptTable table = mockTable("test_index", new String[]{"name"}, new SqlTypeName[]{SqlTypeName.VARCHAR});
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(painlessExpr), List.of("scripted_field"));
        PlannerContext context = buildContext("parquet", Map.of("name", Map.of("type", "keyword")),
            List.of(DATAFUSION, LUCENE.withCapabilityProvider(luceneWithPainless)));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    public void testMixedFieldAndPainlessWithDelegation() {
        OpenSearchProject result = runProject("parquet", delegationBackends("painless"),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Field ref should not be annotated", result.getProjects().get(0) instanceof AnnotatedProjectExpression);
        assertAnnotation(result.getProjects().get(1), MockLuceneBackend.NAME);
    }

    public void testHighlightDelegation() {
        OpenSearchProject result = runProject("parquet", delegationBackends("painless", "highlight"),
            rexBuilder.makeCall(HIGHLIGHT, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockLuceneBackend.NAME);
    }

    // ---- Opaque natively supported ----

    public void testOpaqueOperationSupportedNatively() {
        BackendCapabilityProvider dfWithPainless = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() { return DATAFUSION.getCapabilityProvider().aggregateCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return combine(scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class)),
                    opaqueCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), "painless"));
            }
        };
        OpenSearchProject result = runProject("parquet", List.of(DATAFUSION.withCapabilityProvider(dfWithPainless), LUCENE),
            rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    // ---- Nested expressions ----

    public void testNestedScalarFunctions() {
        RexNode castExpr = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.INTEGER),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode plusExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, castExpr,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1));
        OpenSearchProject result = runProject(plusExpr);
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    // ---- Mixed backends in one projection ----

    public void testMixedBackendsInProjection() {
        BackendCapabilityProvider dfWithScalarsAndDelegation = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() { return DATAFUSION.getCapabilityProvider().aggregateCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }
            @Override public Set<DelegationType> supportedDelegations() { return Set.of(DelegationType.PROJECT); }
        };
        BackendCapabilityProvider luceneAccepting = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return LUCENE.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
            @Override public Set<DelegationType> acceptedDelegations() { return Set.of(DelegationType.PROJECT); }
        };

        RexNode fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode castExpr = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1));

        OpenSearchProject result = runProject("parquet",
            List.of(DATAFUSION.withCapabilityProvider(dfWithScalarsAndDelegation), LUCENE.withCapabilityProvider(luceneAccepting)),
            fieldRef, painlessExpr, castExpr);

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Field ref should not be annotated", result.getProjects().get(0) instanceof AnnotatedProjectExpression);
        assertAnnotation(result.getProjects().get(1), MockLuceneBackend.NAME);
        assertAnnotation(result.getProjects().get(2), MockDataFusionBackend.NAME);
    }

    public void testScalarWrappingOpaqueOp() {
        BackendCapabilityProvider dfWithScalarsAndDelegation = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() { return DATAFUSION.getCapabilityProvider().aggregateCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }
            @Override public Set<DelegationType> supportedDelegations() { return Set.of(DelegationType.PROJECT); }
        };
        BackendCapabilityProvider luceneAccepting = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return LUCENE.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
            @Override public Set<DelegationType> acceptedDelegations() { return Set.of(DelegationType.PROJECT); }
        };

        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode plusExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.INTEGER), painlessExpr),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1));

        OpenSearchProject result = runProject("parquet",
            List.of(DATAFUSION.withCapabilityProvider(dfWithScalarsAndDelegation), LUCENE.withCapabilityProvider(luceneAccepting)),
            plusExpr);

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
        BackendCapabilityProvider dfWithDelegation = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() { return DATAFUSION.getCapabilityProvider().aggregateCapabilities(); }
            @Override public Set<DelegationType> supportedDelegations() { return Set.of(DelegationType.PROJECT); }
        };
        BackendCapabilityProvider luceneAccepting = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return LUCENE.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless", "highlight");
            }
            @Override public Set<DelegationType> acceptedDelegations() { return Set.of(DelegationType.PROJECT); }
        };
        SqlFunction suggest = new SqlFunction("suggest", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000,
            null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION);
        // Third backend declares "suggest" so isOpaqueOperation returns true
        BackendCapabilityProvider thirdCaps = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return LUCENE.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "suggest");
            }
        };
        MockLuceneBackend thirdBackend = new MockLuceneBackend() {
            @Override public String name() { return "mock-third"; }
            @Override public BackendCapabilityProvider getCapabilityProvider() { return thirdCaps; }
        };

        RelOptTable table = mockTable("test_index", new String[]{"name"}, new SqlTypeName[]{SqlTypeName.VARCHAR});
        RexNode suggestExpr = rexBuilder.makeCall(suggest, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(suggestExpr), List.of("sg"));
        PlannerContext context = buildContext("parquet", Map.of("name", Map.of("type", "keyword")),
            List.of(DATAFUSION.withCapabilityProvider(dfWithDelegation), LUCENE.withCapabilityProvider(luceneAccepting), thirdBackend));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    public void testDelegationFailsWhenAcceptorRejectsDelegationType() {
        BackendCapabilityProvider dfWithDelegation = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() { return DATAFUSION.getCapabilityProvider().aggregateCapabilities(); }
            @Override public Set<DelegationType> supportedDelegations() { return Set.of(DelegationType.PROJECT); }
        };
        BackendCapabilityProvider luceneWithPainlessNoAccept = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return LUCENE.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
            // acceptedDelegations() throws by default — no delegation accepted
        };

        RelOptTable table = mockTable("test_index", new String[]{"name"}, new SqlTypeName[]{SqlTypeName.VARCHAR});
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(painlessExpr), List.of("scripted"));
        PlannerContext context = buildContext("parquet", Map.of("name", Map.of("type", "keyword")),
            List.of(DATAFUSION.withCapabilityProvider(dfWithDelegation), LUCENE.withCapabilityProvider(luceneWithPainlessNoAccept)));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    // ---- Helpers ----

    private void assertAnnotation(RexNode expr, String expectedBackend) {
        assertTrue("Expected AnnotatedProjectExpression, got " + expr.getClass().getSimpleName(),
            expr instanceof AnnotatedProjectExpression);
        assertTrue(((AnnotatedProjectExpression) expr).getViableBackends().contains(expectedBackend));
    }

    private OpenSearchProject runProject(RexNode... exprs) {
        return runProject("parquet", List.of(dfWithScalarFunctions(), LUCENE), exprs);
    }

    private OpenSearchProject runProject(String format, List<AnalyticsSearchBackendPlugin> backends, RexNode... exprs) {
        RelOptTable table = mockTable("test_index",
            new String[]{"name", "value"}, new SqlTypeName[]{SqlTypeName.VARCHAR, SqlTypeName.INTEGER});
        List<String> fieldNames = new ArrayList<>();
        for (int i = 0; i < exprs.length; i++) fieldNames.add("col_" + i);
        LogicalProject project = LogicalProject.create(stubScan(table), List.of(), List.of(exprs), fieldNames);
        PlannerContext context = buildContext(format,
            Map.of("name", Map.of("type", "keyword"), "value", Map.of("type", "integer")), backends);
        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchProject", result instanceof OpenSearchProject);
        return (OpenSearchProject) result;
    }

    private MockDataFusionBackend dfWithScalarFunctions() {
        BackendCapabilityProvider withScalars = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() { return DATAFUSION.getCapabilityProvider().aggregateCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }
        };
        return DATAFUSION.withCapabilityProvider(withScalars);
    }

    /** Returns DF (with PROJECT delegation) + Lucene (accepting, with given opaque ops). */
    private List<AnalyticsSearchBackendPlugin> delegationBackends(String... opaqueOps) {
        BackendCapabilityProvider dfWithDelegation = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() { return DATAFUSION.getCapabilityProvider().aggregateCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class));
            }
            @Override public Set<DelegationType> supportedDelegations() { return Set.of(DelegationType.PROJECT); }
        };
        BackendCapabilityProvider luceneAccepting = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return LUCENE.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), opaqueOps);
            }
            @Override public Set<DelegationType> acceptedDelegations() { return Set.of(DelegationType.PROJECT); }
        };
        return List.of(DATAFUSION.withCapabilityProvider(dfWithDelegation), LUCENE.withCapabilityProvider(luceneAccepting));
    }

    @SafeVarargs
    private static Set<ProjectCapability> combine(Set<ProjectCapability>... sets) {
        Set<ProjectCapability> result = new HashSet<>();
        for (Set<ProjectCapability> set : sets) result.addAll(set);
        return result;
    }

    private static Set<ProjectCapability> scalarCaps(Set<String> formats, Set<ScalarFunction> functions) {
        Set<ProjectCapability> caps = new HashSet<>();
        for (ScalarFunction func : functions) {
            for (FieldType type : FieldType.values()) {
                caps.add(new ProjectCapability.Scalar(func, type, formats));
            }
        }
        return caps;
    }

    private static Set<ProjectCapability> opaqueCaps(Set<String> formats, String... names) {
        Set<ProjectCapability> caps = new HashSet<>();
        for (String name : names) caps.add(new ProjectCapability.Opaque(name, formats));
        return caps;
    }
}
