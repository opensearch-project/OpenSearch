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
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;

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

    /** Field references only — always valid, no annotations. */
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

    /** CAST supported by backend → annotated with child backend. */
    public void testSupportedScalarFunction() {
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );
        OpenSearchProject result = runProject(castExpr);
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    // ---- Delegation ----

    /** Painless on DataFusion with delegation to Lucene → passes. */
    public void testPainlessDelegationFromDataFusionToLucene() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };

        OpenSearchProject result = runProject("parquet", List.of(dfWithDelegation, luceneAccepting),
            rexBuilder.makeCall(PAINLESS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        // Operator: only DF viable (Lucene not viable for scan)
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(result.getViableBackends().contains(MockLuceneBackend.NAME));
        // painless expression: only Lucene can natively evaluate it
        AnnotatedProjectExpression annotation = (AnnotatedProjectExpression) result.getProjects().get(0);
        assertTrue(annotation.getOriginal().toString().contains("painless"));
        assertTrue(annotation.getViableBackends().contains(MockLuceneBackend.NAME));
        assertFalse(annotation.getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    /** Painless on DataFusion WITHOUT delegation → error. */
    public void testPainlessErrorsWithoutDelegation() {
        // Lucene supports painless but no delegation configured
        MockLuceneBackend luceneWithPainless = new MockLuceneBackend() {
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };

        RelOptTable table = mockTable("test_index", new String[]{"name"}, new SqlTypeName[]{SqlTypeName.VARCHAR});
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(
            stubScan(table), List.of(), List.of(painlessExpr),
            List.of("scripted_field"));

        PlannerContext context = buildContext("parquet", Map.of(
            "name", Map.of("type", "keyword")
        ), List.of(DATAFUSION, luceneWithPainless));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    /** Mixed: field ref + painless with delegation → passes. */
    public void testMixedFieldAndPainlessWithDelegation() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };

        OpenSearchProject result = runProject("parquet", List.of(dfWithDelegation, luceneAccepting),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeCall(PAINLESS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Field ref should not be annotated",
            result.getProjects().get(0) instanceof AnnotatedProjectExpression);
        assertAnnotation(result.getProjects().get(1), MockLuceneBackend.NAME);
    }

    /** Highlight on DataFusion with delegation to Lucene → passes. */
    public void testHighlightDelegation() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless", "highlight");
            }
        };

        OpenSearchProject result = runProject("parquet", List.of(dfWithDelegation, luceneAccepting),
            rexBuilder.makeCall(HIGHLIGHT,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockLuceneBackend.NAME);
    }

    // ---- Unsupported scalar function ----

    /** Backend does not support CAST → error. */
    public void testUnsupportedScalarFunctionErrors() {
        // Default MockDataFusionBackend has empty projectCapabilities
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1)
        );

        RelOptTable table = mockTable("test_index",
            new String[]{"name", "value"}, new SqlTypeName[]{SqlTypeName.VARCHAR, SqlTypeName.INTEGER});
        LogicalProject project = LogicalProject.create(
            stubScan(table), List.of(), List.of(castExpr), List.of("casted"));

        PlannerContext context = buildContext("parquet", Map.of(
            "name", Map.of("type", "keyword"),
            "value", Map.of("type", "integer")
        ));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("No backend supports scalar function"));
    }

    // ---- Opaque operation natively supported ----

    /** Backend directly supports painless (no delegation needed) → passes. */
    public void testOpaqueOperationSupportedNatively() {
        MockDataFusionBackend dfWithPainless = new MockDataFusionBackend() {
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return combine(
                    scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), EnumSet.allOf(ScalarFunction.class)),
                    opaqueCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), "painless"));
            }
        };

        OpenSearchProject result = runProject("parquet", List.of(dfWithPainless, LUCENE),
            rexBuilder.makeCall(PAINLESS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0))
        );
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
    }

    // ---- Nested expressions ----

    /** Nested scalar: CAST inside arithmetic → both annotated with child backend. */
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

    /** Field ref + delegated painless + scalar CAST in one projection. */
    public void testMixedBackendsInProjection() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT),
                    EnumSet.allOf(ScalarFunction.class));
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };

        RexNode fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode castExpr = rexBuilder.makeCast(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1));

        OpenSearchProject result = runProject("parquet", List.of(dfWithDelegation, luceneAccepting),
            fieldRef, painlessExpr, castExpr);

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        // field ref — no annotation
        assertFalse("Field ref should not be annotated",
            result.getProjects().get(0) instanceof AnnotatedProjectExpression);
        // painless — delegated to Lucene
        assertAnnotation(result.getProjects().get(1), MockLuceneBackend.NAME);
        // CAST — scalar on DataFusion
        assertAnnotation(result.getProjects().get(2), MockDataFusionBackend.NAME);
    }

    /** Scalar wrapping an opaque op: +(painless($0), $1) — inner painless annotated with Lucene. */
    public void testScalarWrappingOpaqueOp() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT),
                    EnumSet.allOf(ScalarFunction.class));
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
        };

        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        RexNode intRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
        RexNode plusExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.INTEGER), painlessExpr),
            intRef);

        OpenSearchProject result = runProject("parquet", List.of(dfWithDelegation, luceneAccepting),
            plusExpr);

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        // outer PLUS annotated with DF
        assertAnnotation(result.getProjects().get(0), MockDataFusionBackend.NAME);
        // CAST(painless($0)) — CAST annotated with DF, painless inside annotated with Lucene
        AnnotatedProjectExpression outerAnnotation = (AnnotatedProjectExpression) result.getProjects().get(0);
        RexNode innerPlus = outerAnnotation.getOriginal();
        assertTrue("Expected RexCall for PLUS", innerPlus instanceof org.apache.calcite.rex.RexCall);
        RexNode castOperand = ((org.apache.calcite.rex.RexCall) innerPlus).getOperands().get(0);
        assertAnnotation(castOperand, MockDataFusionBackend.NAME);
        RexNode painlessInside = ((AnnotatedProjectExpression) castOperand).getOriginal();
        assertTrue("Expected RexCall for CAST", painlessInside instanceof org.apache.calcite.rex.RexCall);
        RexNode painlessArg = ((org.apache.calcite.rex.RexCall) painlessInside).getOperands().get(0);
        assertAnnotation(painlessArg, MockLuceneBackend.NAME);
    }

    // ---- Delegation edge cases ----

    /** Delegation configured, acceptor accepts PROJECT but only knows painless — highlight fails. */
    public void testDelegationFailsWhenAcceptorLacksOpaqueOp() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        // Lucene accepts delegation and knows highlight (makes isOpaqueOperation true)
        // but only supports painless for actual evaluation
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless", "highlight");
            }
        };

        // Use a function unknown to any acceptor — "suggest"
        SqlFunction suggest = new SqlFunction(
            "suggest", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000,
            null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION
        );

        RelOptTable table = mockTable("test_index",
            new String[]{"name"}, new SqlTypeName[]{SqlTypeName.VARCHAR});
        RexNode suggestExpr = rexBuilder.makeCall(suggest,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(
            stubScan(table), List.of(), List.of(suggestExpr), List.of("sg"));

        // Add a third backend that declares "suggest" so isOpaqueOperation returns true
        MockLuceneBackend thirdBackend = new MockLuceneBackend() {
            @Override public String name() { return "mock-third"; }
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "suggest");
            }
        };

        PlannerContext context = buildContext("parquet", Map.of(
            "name", Map.of("type", "keyword")
        ), List.of(dfWithDelegation, luceneAccepting, thirdBackend));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(project, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }

    /** Acceptor supports opaque op but doesn't accept PROJECT delegation → error. */
    public void testDelegationFailsWhenAcceptorRejectsDelegationType() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.PROJECT);
            }
        };
        MockLuceneBackend luceneWithPainlessButNoAccept = new MockLuceneBackend() {
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return opaqueCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), "painless");
            }
            // acceptedDelegations() returns empty by default
        };

        RelOptTable table = mockTable("test_index",
            new String[]{"name"}, new SqlTypeName[]{SqlTypeName.VARCHAR});
        RexNode painlessExpr = rexBuilder.makeCall(PAINLESS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0));
        LogicalProject project = LogicalProject.create(
            stubScan(table), List.of(), List.of(painlessExpr), List.of("scripted"));

        PlannerContext context = buildContext("parquet", Map.of(
            "name", Map.of("type", "keyword")
        ), List.of(dfWithDelegation, luceneWithPainlessButNoAccept));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(project, context));
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

    private OpenSearchProject runProject(String format, List<AnalyticsSearchBackendPlugin> backends,
                                         RexNode... exprs) {
        RelOptTable table = mockTable("test_index",
            new String[]{"name", "value"}, new SqlTypeName[]{SqlTypeName.VARCHAR, SqlTypeName.INTEGER});

        List<String> fieldNames = new java.util.ArrayList<>();
        for (int i = 0; i < exprs.length; i++) {
            fieldNames.add("col_" + i);
        }

        LogicalProject project = LogicalProject.create(
            stubScan(table), List.of(), List.of(exprs), fieldNames);

        PlannerContext context = buildContext(format, Map.of(
            "name", Map.of("type", "keyword"),
            "value", Map.of("type", "integer")
        ), backends);

        RelNode result = unwrapExchange(runPlanner(project, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchProject, got " + result.getClass().getSimpleName(),
            result instanceof OpenSearchProject);
        return (OpenSearchProject) result;
    }

    private MockDataFusionBackend dfWithScalarFunctions() {
        return new MockDataFusionBackend() {
            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return scalarCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT),
                    EnumSet.allOf(ScalarFunction.class));
            }
        };
    }

    @SafeVarargs
    private static Set<ProjectCapability> combine(Set<ProjectCapability>... sets) {
        Set<ProjectCapability> result = new HashSet<>();
        for (Set<ProjectCapability> set : sets) {
            result.addAll(set);
        }
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
        for (String name : names) {
            caps.add(new ProjectCapability.Opaque(name, formats));
        }
        return caps;
    }
}
