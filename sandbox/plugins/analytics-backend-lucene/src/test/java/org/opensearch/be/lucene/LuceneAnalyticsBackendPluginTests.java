/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

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
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end test: MATCH predicate flows through FragmentConversionDriver with the real
 * {@link LuceneAnalyticsBackendPlugin} serializer, producing valid MatchQueryBuilder bytes.
 */
public class LuceneAnalyticsBackendPluginTests extends OpenSearchTestCase {

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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
    }

    /**
     * MATCH(message, 'hello world') through full pipeline → delegatedQueries contains
     * valid MatchQueryBuilder bytes with correct field name and query text.
     */
    public void testMatchPredicateDelegationEndToEnd() throws IOException {
        // DF backend: drives the plan, supports delegation, has a stub convertor
        AnalyticsSearchBackendPlugin dfBackend = new StubDfBackend();
        // Real Lucene backend: accepts delegation, provides MATCH serializer
        AnalyticsSearchBackendPlugin luceneBackend = new LuceneAnalyticsBackendPlugin(null);

        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        PlannerContext context = buildContext("parquet", fields, List.of(dfBackend, luceneBackend));

        RexNode condition = rexBuilder.makeCall(
            MATCH_FUNCTION,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeLiteral("hello world")
        );
        RelOptTable table = mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        LogicalFilter filter = LogicalFilter.create(new TableScan(cluster, cluster.traitSet(), List.of(), table) {
        }, condition);

        RelNode marked = PlannerImpl.markAndOptimize(filter, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());

        // Find the leaf stage (shard scan with filter)
        Stage leaf = dag.rootStage();
        while (!leaf.getChildStages().isEmpty()) {
            leaf = leaf.getChildStages().getFirst();
        }
        StagePlan plan = leaf.getPlanAlternatives().getFirst();

        // Verify delegation happened
        assertFalse("delegatedExpressions should not be empty", plan.delegatedExpressions().isEmpty());
        assertEquals("should have exactly one delegated expression", 1, plan.delegatedExpressions().size());

        // Deserialize and verify the MatchQueryBuilder
        byte[] queryBytes = plan.delegatedExpressions().getFirst().getExpressionBytes();
        try (StreamInput input = new NamedWriteableAwareStreamInput(StreamInput.wrap(queryBytes), WRITEABLE_REGISTRY)) {
            QueryBuilder deserialized = input.readNamedWriteable(QueryBuilder.class);
            assertTrue("Should be MatchQueryBuilder", deserialized instanceof MatchQueryBuilder);
            MatchQueryBuilder matchQuery = (MatchQueryBuilder) deserialized;
            assertEquals("message", matchQuery.fieldName());
            assertEquals("hello world", matchQuery.value());
        }
    }

    // ---- Minimal infrastructure ----

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

    /** Minimal DF backend that drives the plan with delegation support. */
    private static class StubDfBackend implements AnalyticsSearchBackendPlugin {
        private static final Set<FieldType> TYPES = new HashSet<>();
        static {
            TYPES.addAll(FieldType.numeric());
            TYPES.addAll(FieldType.keyword());
            TYPES.addAll(FieldType.date());
            TYPES.add(FieldType.BOOLEAN);
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
                        ScalarFunction.GREATER_THAN_OR_EQUAL,
                        ScalarFunction.LESS_THAN,
                        ScalarFunction.LESS_THAN_OR_EQUAL
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
            return new FragmentConvertor() {
                @Override
                public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
                    return ("shard:" + tableName).getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public byte[] convertFinalAggFragment(RelNode fragment) {
                    return "reduce".getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
                    return innerBytes;
                }

                @Override
                public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
                    return innerBytes;
                }
            };
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
                    throw new UnsupportedOperationException("mock");
                }
            };
        }
    }
}
