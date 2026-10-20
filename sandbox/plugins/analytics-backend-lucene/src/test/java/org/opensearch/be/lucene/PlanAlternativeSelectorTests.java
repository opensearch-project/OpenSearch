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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
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
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

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
 * Tests for {@link PlanAlternativeSelector} executed against the real {@link LuceneAnalyticsBackendPlugin}.
 *
 * <p>Lives in the lucene module (rather than analytics-engine) so the production capability surface
 * — {@code Index} scan + standard filter + COUNT aggregate, declared only for keyword/text
 * types — is consulted directly. If someone widens or narrows {@code STANDARD_TYPES} in the
 * production plugin, these tests catch the change without any mock to update.
 *
 * <p>Pipeline executed: {@code PlanForker} → {@code PlanAlternativeSelector} (no convertor —
 * selection happens before conversion, mirroring {@code DefaultPlanExecutor.executeInternal}).
 */
public class PlanAlternativeSelectorTests extends OpenSearchTestCase {

    private static final IndexNameExpressionResolver TEST_RESOLVER = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

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
     * Qualified shape: {@code COUNT(*)} over a keyword field with both parquet doc values and a
     * lucene index. Forker emits {@code [lucene, mock-parquet]}; with prefer=true the selector
     * collapses the leaf to {@code [lucene]}.
     */
    public void testCountStarOverIndexedKeyword_selectsLucene() {
        TableScan scan = scanOver("status", SqlTypeName.VARCHAR);
        RelNode plan = aggregate(scan, countStar(scan));
        QueryDAG dag = forkAndSelect(plan, keywordMappings(), true);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals("selector should collapse to a single alternative", 1, alternatives.size());
        assertEquals("lucene", alternatives.getFirst().backendId());
    }

    /**
     * Same qualified shape, prefer=false: selector forces the value-producing backend. The
     * Lucene alternative is dropped, leaving only mock-parquet — the data-node fallback can
     * never silently pick Lucene when the cluster has flipped the setting off.
     */
    public void testCountStarOverIndexedKeyword_preferDisabled_forcesDataFusion() {
        TableScan scan = scanOver("status", SqlTypeName.VARCHAR);
        RelNode plan = aggregate(scan, countStar(scan));
        QueryDAG dag = forkAndSelect(plan, keywordMappings(), false);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals("prefer=false: only the value-producing backend remains", 1, alternatives.size());
        assertEquals("mock-parquet", alternatives.getFirst().backendId());
    }

    /**
     * Disqualified shape: {@code COUNT(*)} over an INTEGER field. Production Lucene's
     * {@code Index(supportedFieldTypes = {KEYWORD, TEXT, MATCH_ONLY_TEXT})} cap excludes
     * numerics, so the table-scan rule never marks Lucene viable — irrespective of the field's
     * {@code index} mapping setting.
     */
    public void testCountStarOverIntegerField_luceneNeverViable() {
        TableScan scan = scanOver("status", SqlTypeName.INTEGER);
        RelNode plan = aggregate(scan, countStar(scan));
        QueryDAG dag = forkAndSelect(plan, integerMappings(), true);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals(1, alternatives.size());
        assertEquals("mock-parquet", alternatives.getFirst().backendId());
    }

    /**
     * Disqualified shape: {@code SUM(status)} over a keyword field. Lucene declares no SUM
     * aggregate, so the SUM operator narrows to parquet regardless of where the field is
     * indexed. Selector is a no-op.
     */
    public void testSumOverIntegerField_luceneNeverDrivesSum() {
        TableScan scan = scanOver("status", SqlTypeName.INTEGER);
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(0),
            -1,
            scan,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "sum_status"
        );
        RelNode plan = aggregate(scan, sum);
        QueryDAG dag = forkAndSelect(plan, integerMappings(), true);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals(1, alternatives.size());
        assertEquals("mock-parquet", alternatives.getFirst().backendId());
    }

    /**
     * Disqualified shape: {@code COUNT(*)} over a TEXT field with {@code index: false}. Even
     * though the field's type is in Lucene's {@code Index.supportedFieldTypes},
     * {@link FieldStorageResolver} leaves {@code indexFormats} empty when {@code index} is
     * explicitly false — Lucene has no inverted-index segment to drive a count against.
     */
    public void testCountStarOverNonIndexedTextField_luceneNeverViable() {
        TableScan scan = scanOver("status", SqlTypeName.VARCHAR);
        RelNode plan = aggregate(scan, countStar(scan));
        QueryDAG dag = forkAndSelect(plan, nonIndexedTextMappings(), true);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals(1, alternatives.size());
        assertEquals("mock-parquet", alternatives.getFirst().backendId());
    }

    /**
     * Qualified-but-narrowed shape: {@code COUNT(*) WHERE status='200'} over a Lucene-indexed
     * keyword field. EQUALS on KEYWORD is in Lucene's filter caps, so end-to-end agreement
     * holds and the selector still picks Lucene with prefer=true.
     */
    public void testCountStarWithIndexedKeywordFilter_selectsLucene() {
        TableScan scan = scanOver("status", SqlTypeName.VARCHAR);
        RexNode equalsLiteral = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeLiteral("200")
        );
        RelNode plan = aggregate(LogicalFilter.create(scan, equalsLiteral), countStar(scan));
        QueryDAG dag = forkAndSelect(plan, keywordMappings(), true);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals(1, alternatives.size());
        assertEquals("lucene", alternatives.getFirst().backendId());
    }

    /**
     * Depth-3: {@code COUNT(*) WHERE tag='a' OR region='eu' OR message MATCH 'x'}. All three
     * leaves are Lucene-delegatable (two keyword EQUALS on distinct fields + one MATCH on
     * text). Distinct fields prevent Calcite's SEARCH/Sarg fold. Lucene drives end-to-end
     * via convertFragment; combiner not invoked.
     */
    public void testCountStarWithDepth3OrAllDelegatable_selectsLucene() {
        TableScan scan = threeFieldScan();
        RexNode tagA = equalsLit(0, SqlTypeName.VARCHAR, "a");
        RexNode regionEu = equalsLit(2, SqlTypeName.VARCHAR, "eu");
        RexNode matchX = matchOn(1, "x");
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.OR, tagA, regionEu, matchX);
        RelNode plan = aggregate(LogicalFilter.create(scan, condition), countStar(scan));
        QueryDAG dag = forkAndSelect(plan, threeFieldMappings(), true);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals(1, alternatives.size());
        assertEquals("lucene", alternatives.getFirst().backendId());
    }

    /**
     * Depth-4: {@code COUNT(*) WHERE (tag='a' AND msg MATCH 'x') OR (region='eu' AND msg MATCH 'y')}.
     * Nested AND inside OR, all four leaves Lucene-delegatable on distinct fields. Lucene
     * drives end-to-end.
     */
    public void testCountStarWithDepth4NestedAndOrAllDelegatable_selectsLucene() {
        TableScan scan = threeFieldScan();
        RexNode left = rexBuilder.makeCall(SqlStdOperatorTable.AND, equalsLit(0, SqlTypeName.VARCHAR, "a"), matchOn(1, "x"));
        RexNode right = rexBuilder.makeCall(SqlStdOperatorTable.AND, equalsLit(2, SqlTypeName.VARCHAR, "eu"), matchOn(1, "y"));
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.OR, left, right);
        RelNode plan = aggregate(LogicalFilter.create(scan, condition), countStar(scan));
        QueryDAG dag = forkAndSelect(plan, threeFieldMappings(), true);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals(1, alternatives.size());
        assertEquals("lucene", alternatives.getFirst().backendId());
    }

    /**
     * 4-arm flat OR: {@code COUNT(*) WHERE tag='a' OR msg MATCH 'x' OR region='eu' OR msg MATCH 'y'}.
     * Calcite flattens OR-of-ORs, so authoring nested ORs and a flat 4-arm OR produce the
     * same Filter — all four leaves Lucene-delegatable on distinct fields. Lucene drives.
     */
    public void testCountStarWithFourArmOrAllDelegatable_selectsLucene() {
        TableScan scan = threeFieldScan();
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            equalsLit(0, SqlTypeName.VARCHAR, "a"),
            matchOn(1, "x"),
            equalsLit(2, SqlTypeName.VARCHAR, "eu"),
            matchOn(1, "y")
        );
        RelNode plan = aggregate(LogicalFilter.create(scan, condition), countStar(scan));
        QueryDAG dag = forkAndSelect(plan, threeFieldMappings(), true);

        List<StagePlan> alternatives = leafOf(dag).getPlanAlternatives();
        assertEquals(1, alternatives.size());
        assertEquals("lucene", alternatives.getFirst().backendId());
    }

    // ---- Plan-execution helpers ----

    private QueryDAG forkAndSelect(RelNode plan, Map<String, Map<String, Object>> fieldMappings, boolean preferMetadataDriver) {
        AnalyticsSearchBackendPlugin dfBackend = new StubDfBackend();
        AnalyticsSearchBackendPlugin luceneBackend = new LuceneAnalyticsBackendPlugin(null);

        PlannerContext context = buildContext(fieldMappings, List.of(dfBackend, luceneBackend), preferMetadataDriver);
        RelNode marked = PlannerImpl.runAllOptimizations(plan, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        BackendPlanAdapter.adaptAll(dag, context.getCapabilityRegistry());
        PlanAlternativeSelector.selectAll(dag, context.getCapabilityRegistry(), preferMetadataDriver);
        return dag;
    }

    private static Stage leafOf(QueryDAG dag) {
        Stage stage = dag.rootStage();
        while (!stage.getChildStages().isEmpty()) {
            stage = stage.getChildStages().getFirst();
        }
        return stage;
    }

    // ---- Calcite helpers ----

    private TableScan scanOver(String fieldName, SqlTypeName type) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add(fieldName, typeFactory.createSqlType(type));
        RelDataType rowType = builder.build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("test_index"));
        when(table.getRowType()).thenReturn(rowType);
        return new TableScan(cluster, cluster.traitSet(), List.of(), table) {
        };
    }

    /** Three-field scan: tag (keyword), message (text), region (keyword). Distinct keyword
     *  fields prevent Calcite's SEARCH/Sarg fold of multi-EQUALS-on-same-field. */
    private TableScan threeFieldScan() {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("tag", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        builder.add("message", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        builder.add("region", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        RelDataType rowType = builder.build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("test_index"));
        when(table.getRowType()).thenReturn(rowType);
        return new TableScan(cluster, cluster.traitSet(), List.of(), table) {
        };
    }

    private RexNode equalsLit(int fieldIndex, SqlTypeName type, String value) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(type), fieldIndex),
            rexBuilder.makeLiteral(value)
        );
    }

    /** MATCH on a text-typed field; mirrors the SqlFunction shape used by other Lucene tests. */
    private static final SqlOperator MATCH_FUNCTION = new SqlFunction(
        "MATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private RexNode matchOn(int fieldIndex, String query) {
        return rexBuilder.makeCall(
            MATCH_FUNCTION,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), fieldIndex),
            rexBuilder.makeLiteral(query)
        );
    }

    private RelNode aggregate(RelNode input, AggregateCall... calls) {
        return LogicalAggregate.create(input, ImmutableBitSet.of(), null, List.of(calls));
    }

    private AggregateCall countStar(TableScan scan) {
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "count_star"
        );
    }

    // ---- Mappings ----

    private static Map<String, Map<String, Object>> keywordMappings() {
        return Map.of("status", Map.of("type", "keyword", "index", true));
    }

    private static Map<String, Map<String, Object>> integerMappings() {
        return Map.of("status", Map.of("type", "integer"));
    }

    private static Map<String, Map<String, Object>> nonIndexedTextMappings() {
        return Map.of("status", Map.of("type", "text", "index", false));
    }

    /** tag (keyword indexed), message (text indexed), region (keyword indexed) — all
     *  Lucene-delegatable. */
    private static Map<String, Map<String, Object>> threeFieldMappings() {
        return Map.of(
            "tag",
            Map.of("type", "keyword", "index", true),
            "message",
            Map.of("type", "text", "index", true),
            "region",
            Map.of("type", "keyword", "index", true)
        );
    }

    // ---- Cluster state / context ----

    @SuppressWarnings("unchecked")
    private PlannerContext buildContext(
        Map<String, Map<String, Object>> fieldMappings,
        List<AnalyticsSearchBackendPlugin> backends,
        boolean preferMetadataDriver
    ) {
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", fieldMappings));

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(
            Settings.builder()
                .put("index.composite.primary_data_format", "parquet")
                .putList("index.composite.secondary_data_formats", "lucene")
                .build()
        );
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(indexMetadata.getNumberOfShards()).thenReturn(2);

        Metadata metadata = mock(Metadata.class);
        when(metadata.index("test_index")).thenReturn(indexMetadata);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        return new PlannerContext(new CapabilityRegistry(backends, fieldStorageFactory), clusterState, null, false, preferMetadataDriver);
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
     * Minimal DataFusion-shaped backend: drives parquet, declares enough cap surface
     * for plan forking, exchange-sinks for the coordinator reduce. No delegation —
     * we don't need it for selector tests.
     */
    private static class StubDfBackend implements AnalyticsSearchBackendPlugin {
        private static final Set<FieldType> TYPES = new HashSet<>();
        static {
            TYPES.addAll(FieldType.numeric());
            TYPES.addAll(FieldType.keyword());
            TYPES.addAll(FieldType.text());
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
                public Set<AggregateCapability> aggregateCapabilities() {
                    return Set.of(
                        new AggregateCapability(AggregateFunction.COUNT, TYPES, Set.of("parquet")),
                        new AggregateCapability(AggregateFunction.SUM, TYPES, Set.of("parquet"))
                    );
                }

                @Override
                public Set<DelegationType> supportedDelegations() {
                    return Set.of(DelegationType.FILTER);
                }
            };
        }

        @Override
        public ExchangeSinkProvider getExchangeSinkProvider() {
            return (context, backendContext) -> null;
        }

        @Override
        public FragmentConvertor getFragmentConvertor() {
            return new FragmentConvertor() {
                @Override
                public byte[] convertFragment(RelNode fragment) {
                    return "fragment".getBytes(StandardCharsets.UTF_8);
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
                public Optional<InstructionNode> createShardScanNode(boolean requestsRowIds, String logicalTableName) {
                    return Optional.of(new ShardScanInstructionNode(requestsRowIds, logicalTableName));
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
                public Optional<InstructionNode> createShardScanWithDelegationNode(
                    FilterTreeShape treeShape,
                    int delegatedPredicateCount,
                    boolean requestsRowIds,
                    String logicalTableName
                ) {
                    return Optional.of(
                        new ShardScanWithDelegationInstructionNode(treeShape, delegatedPredicateCount, requestsRowIds, logicalTableName)
                    );
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
