/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationInstructionNode;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.be.lucene.LuceneAnalyticsBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.SimpleExtensionDeclaration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end delegation test: MATCH predicates flow through the full pipeline
 * (marking → forking → FragmentConversionDriver → Substrait) using the real
 * {@link LuceneAnalyticsBackendPlugin} for query serialization and the real
 * {@link DataFusionFragmentConvertor} for Substrait conversion.
 *
 * <p>Verifies both the delegated query bytes (MatchQueryBuilder round-trip) and
 * the Substrait plan structure (delegated_predicate placeholders with correct annotation IDs
 * and preserved AND/OR/NOT boolean structure).
 */
public class FilterDelegationForIndexFullConversionTests extends OpenSearchTestCase {

    private static final SqlFunction MATCH_FUNCTION = new SqlFunction(
        "MATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchQueryBuilder.NAME, MatchQueryBuilder::new))
    );

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private AnalyticsSearchBackendPlugin dfBackend;
    private AnalyticsSearchBackendPlugin luceneBackend;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);

        // Load Substrait extensions with delegation_functions.yaml merged in,
        // same as DataFusionPlugin.loadSubstraitExtensions() does at startup.
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        SimpleExtension.ExtensionCollection extensions;
        try {
            SimpleExtension.ExtensionCollection delegationExtensions = SimpleExtension.load(List.of("/delegation_functions.yaml"));
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(delegationExtensions);
        } finally {
            thread.setContextClassLoader(previous);
        }

        // Lightweight DF backend wrapping the real DataFusionFragmentConvertor.
        // Avoids instantiating DataFusionPlugin which requires native libraries.
        // Only capabilities and fragment conversion are needed — no execution.
        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor(extensions);
        dfBackend = new StubDfBackend(convertor);
        luceneBackend = new LuceneAnalyticsBackendPlugin(null);
    }

    /**
     * AND(status = 200, MATCH(message, 'hello world')) — mixed native + delegated.
     * Planner assigns id=0 to equals (native), id=1 to MATCH (delegated).
     */
    public void testMixedNativeAndDelegated() throws Exception {
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            makeEquals(0, SqlTypeName.INTEGER, 200),
            makeMatch(1, "hello world")
        );
        StagePlan plan = runPipeline(condition);

        assertEquals("should have 1 delegated query", 1, plan.delegatedExpressions().size());
        assertMatchQueryBuilder(plan.delegatedExpressions(), "message", "hello world");

        SubstraitResult substrait = substraitResult(plan.convertedBytes());
        logger.info("Substrait plan (mixed E2E):\n{}", substrait.plan());
        // Root: AND
        Expression.ScalarFunction andFunc = substrait.filterRel().getCondition().getScalarFunction();
        assertEquals("and", resolveFunctionName(substrait.plan(), andFunc.getFunctionReference()));
        assertEquals("AND must have 2 arguments", 2, andFunc.getArgumentsCount());
        // arg[1]: delegated_predicate(1) — annotation id=1 maps to MATCH 'hello world'
        assertDelegatedPredicate(substrait.plan(), andFunc.getArguments(1).getValue(), 1);
        assertMatchQueryForAnnotation(plan.delegatedExpressions(), 1, "message", "hello world");
    }

    /**
     * AND(status = 200, OR(MATCH(message, 'hello'), NOT(MATCH(message, 'goodbye')))) — complex tree.
     * Planner assigns id=0 to equals (native), id=1 to first MATCH, id=2 to second MATCH.
     */
    public void testComplexBooleanTree() throws Exception {
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            makeEquals(0, SqlTypeName.INTEGER, 200),
            rexBuilder.makeCall(
                SqlStdOperatorTable.OR,
                makeMatch(1, "hello"),
                rexBuilder.makeCall(SqlStdOperatorTable.NOT, makeMatch(1, "goodbye"))
            )
        );
        StagePlan plan = runPipeline(condition);

        assertEquals("should have 2 delegated queries", 2, plan.delegatedExpressions().size());

        SubstraitResult substrait = substraitResult(plan.convertedBytes());
        logger.info("Substrait plan (complex E2E):\n{}", substrait.plan());

        // Root: AND
        Expression.ScalarFunction andFunc = substrait.filterRel().getCondition().getScalarFunction();
        assertEquals("and", resolveFunctionName(substrait.plan(), andFunc.getFunctionReference()));
        assertEquals("AND must have 2 arguments", 2, andFunc.getArgumentsCount());

        // arg[1]: OR
        Expression orExpr = andFunc.getArguments(1).getValue();
        assertTrue("second AND arg must be scalar function", orExpr.hasScalarFunction());
        assertEquals("or", resolveFunctionName(substrait.plan(), orExpr.getScalarFunction().getFunctionReference()));
        Expression.ScalarFunction orFunc = orExpr.getScalarFunction();
        assertEquals("OR must have 2 arguments", 2, orFunc.getArgumentsCount());

        // OR arg[0]: delegated_predicate(1) → MATCH 'hello'
        assertDelegatedPredicate(substrait.plan(), orFunc.getArguments(0).getValue(), 1);
        assertMatchQueryForAnnotation(plan.delegatedExpressions(), 1, "message", "hello");

        // OR arg[1]: NOT(delegated_predicate(2)) → MATCH 'goodbye'
        Expression notExpr = orFunc.getArguments(1).getValue();
        assertTrue("OR second arg must be scalar function", notExpr.hasScalarFunction());
        assertEquals("not", resolveFunctionName(substrait.plan(), notExpr.getScalarFunction().getFunctionReference()));
        assertDelegatedPredicate(substrait.plan(), notExpr.getScalarFunction().getArguments(0).getValue(), 2);
        assertMatchQueryForAnnotation(plan.delegatedExpressions(), 2, "message", "goodbye");
    }

    // ---- Pipeline ----

    private StagePlan runPipeline(RexNode condition) {
        Map<String, Map<String, Object>> fields = Map.of(
            "status",
            Map.of("type", "integer", "index", true),
            "message",
            Map.of("type", "keyword", "index", true)
        );
        PlannerContext context = buildContext("parquet", fields, List.of(dfBackend, luceneBackend));
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "status", "message" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR }
        );
        LogicalFilter filter = LogicalFilter.create(new TableScan(cluster, cluster.traitSet(), List.of(), table) {
        }, condition);

        RelNode marked = PlannerImpl.markAndOptimize(filter, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());

        Stage leaf = dag.rootStage();
        while (!leaf.getChildStages().isEmpty()) {
            leaf = leaf.getChildStages().getFirst();
        }
        return leaf.getPlanAlternatives().getFirst();
    }

    // ---- Helpers ----

    private RexNode makeEquals(int fieldIndex, SqlTypeName fieldType, Object value) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(fieldType), fieldIndex),
            rexBuilder.makeLiteral(value, typeFactory.createSqlType(fieldType), true)
        );
    }

    private RexNode makeMatch(int fieldIndex, String query) {
        return rexBuilder.makeCall(
            MATCH_FUNCTION,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), fieldIndex),
            rexBuilder.makeLiteral(query)
        );
    }

    private void assertMatchQueryBuilder(List<DelegatedExpression> delegatedExpressions, String expectedField, String expectedQuery)
        throws IOException {
        for (DelegatedExpression expr : delegatedExpressions) {
            try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(expr.getExpressionBytes()), WRITEABLE_REGISTRY)) {
                MatchQueryBuilder matchQuery = (MatchQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
                if (matchQuery.fieldName().equals(expectedField) && matchQuery.value().equals(expectedQuery)) {
                    return;
                }
            }
        }
        fail("No MatchQueryBuilder found with field=[" + expectedField + "], query=[" + expectedQuery + "]");
    }

    private record SubstraitResult(Plan plan, FilterRel filterRel) {
    }

    private SubstraitResult substraitResult(byte[] convertedBytes) throws Exception {
        Plan plan = Plan.parseFrom(convertedBytes);
        Rel root = plan.getRelations(0).getRoot().getInput();
        assertTrue("root must be a FilterRel", root.hasFilter());
        return new SubstraitResult(plan, root.getFilter());
    }

    /** Resolves a function_reference to its function name from the plan's extension declarations. */
    private String resolveFunctionName(Plan plan, int functionReference) {
        for (SimpleExtensionDeclaration decl : plan.getExtensionsList()) {
            if (decl.hasExtensionFunction() && decl.getExtensionFunction().getFunctionAnchor() == functionReference) {
                String fullName = decl.getExtensionFunction().getName();
                int colonIndex = fullName.indexOf(':');
                return colonIndex >= 0 ? fullName.substring(0, colonIndex) : fullName;
            }
        }
        fail("No extension function found for reference " + functionReference);
        return null;
    }

    /** Asserts a scalar function expression is delegated_predicate with the expected annotation ID. */
    private void assertDelegatedPredicate(Plan plan, Expression expr, int expectedAnnotationId) {
        assertTrue("expression must be a scalar function", expr.hasScalarFunction());
        Expression.ScalarFunction func = expr.getScalarFunction();
        assertEquals(
            "function must be delegated_predicate",
            DelegatedPredicateFunction.NAME,
            resolveFunctionName(plan, func.getFunctionReference())
        );
        assertEquals("annotation ID must match", expectedAnnotationId, func.getArguments(0).getValue().getLiteral().getI32());
    }

    /** Asserts the delegated query bytes for a specific annotation ID deserialize to the expected MatchQueryBuilder. */
    private void assertMatchQueryForAnnotation(
        List<DelegatedExpression> delegatedExpressions,
        int annotationId,
        String expectedField,
        String expectedQuery
    ) throws IOException {
        DelegatedExpression found = null;
        for (DelegatedExpression expr : delegatedExpressions) {
            if (expr.getAnnotationId() == annotationId) {
                found = expr;
                break;
            }
        }
        assertNotNull("annotation ID " + annotationId + " must be in delegatedExpressions", found);
        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(found.getExpressionBytes()), WRITEABLE_REGISTRY)) {
            MatchQueryBuilder matchQuery = (MatchQueryBuilder) input.readNamedWriteable(QueryBuilder.class);
            assertEquals("field name for annotation " + annotationId, expectedField, matchQuery.fieldName());
            assertEquals("query text for annotation " + annotationId, expectedQuery, matchQuery.value());
        }
    }

    @SuppressWarnings("unchecked")
    private PlannerContext buildContext(
        String primaryFormat,
        Map<String, Map<String, Object>> fieldMappings,
        List<AnalyticsSearchBackendPlugin> backends
    ) {
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", fieldMappings));
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put("index.composite.primary_data_format", primaryFormat).build());
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(indexMetadata.getNumberOfShards()).thenReturn(2);
        Metadata metadata = mock(Metadata.class);
        when(metadata.index("test_index")).thenReturn(indexMetadata);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        return new PlannerContext(new CapabilityRegistry(backends, fieldStorageFactory), clusterState);
    }

    private RelOptTable mockTable(String tableName, String[] fieldNames, SqlTypeName[] fieldTypes) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (int index = 0; index < fieldNames.length; index++) {
            builder.add(fieldNames[index], typeFactory.createSqlType(fieldTypes[index]));
        }
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(tableName));
        when(table.getRowType()).thenReturn(builder.build());
        return table;
    }

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        OperationRouting routing = mock(OperationRouting.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(routing);
        when(routing.searchShards(any(), any(), any(), any())).thenReturn(new GroupShardsIterator<ShardIterator>(List.of()));
        return clusterService;
    }

    /**
     * Lightweight DF backend wrapping the real {@link DataFusionFragmentConvertor}
     * without instantiating {@link DataFusionPlugin} (which requires native libraries).
     * Declares the same capabilities as the real DF backend — only fragment conversion
     * and capability declarations are exercised, not execution.
     */
    private static class StubDfBackend implements AnalyticsSearchBackendPlugin {
        private static final Set<FieldType> TYPES = new HashSet<>();
        static {
            TYPES.addAll(FieldType.numeric());
            TYPES.addAll(FieldType.keyword());
            TYPES.addAll(FieldType.date());
            TYPES.add(FieldType.BOOLEAN);
        }

        private final DataFusionFragmentConvertor convertor;

        StubDfBackend(DataFusionFragmentConvertor convertor) {
            this.convertor = convertor;
        }

        @Override
        public String name() {
            return "mock-parquet";
        }

        @Override
        public BackendCapabilityProvider getCapabilityProvider() {
            return new BackendCapabilityProvider() {
                @Override
                public Set<EngineCapability> supportedEngineCapabilities() {
                    return Set.of(EngineCapability.SORT);
                }

                @Override
                public Set<ScanCapability> scanCapabilities() {
                    return Set.of(new ScanCapability.DocValues(Set.of("parquet"), TYPES));
                }

                @Override
                public Set<FilterCapability> filterCapabilities() {
                    Set<FilterCapability> caps = new HashSet<>();
                    for (ScalarFunction op : Set.of(
                        ScalarFunction.EQUALS,
                        ScalarFunction.NOT_EQUALS,
                        ScalarFunction.GREATER_THAN,
                        ScalarFunction.LESS_THAN
                    )) {
                        caps.add(new FilterCapability.Standard(op, TYPES, Set.of("parquet")));
                    }
                    return caps;
                }

                @Override
                public Set<DelegationType> supportedDelegations() {
                    return Set.of(DelegationType.FILTER);
                }
            };
        }

        @Override
        public ExchangeSinkProvider getExchangeSinkProvider() {
            return context -> null;
        }

        @Override
        public FragmentConvertor getFragmentConvertor() {
            return convertor;
        }

        @Override
        public FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
            return new FragmentInstructionHandlerFactory() {
                @Override
                public Optional<InstructionNode> createShardScanNode() {
                    return Optional.of(new ShardScanInstructionNode());
                }

                @Override
                public Optional<InstructionNode> createFilterDelegationNode(
                    FilterTreeShape treeShape,
                    int delegatedPredicateCount,
                    List<DelegatedExpression> delegatedExpressions
                ) {
                    return Optional.of(new FilterDelegationInstructionNode(treeShape, delegatedPredicateCount, delegatedExpressions));
                }

                @Override
                public Optional<InstructionNode> createPartialAggregateNode() {
                    return Optional.empty();
                }

                @Override
                public Optional<InstructionNode> createFinalAggregateNode() {
                    return Optional.empty();
                }

                @Override
                public FragmentInstructionHandler<?> createHandler(InstructionNode node) {
                    throw new UnsupportedOperationException("stub");
                }
            };
        }
    }
}
