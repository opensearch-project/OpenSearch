/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Shared test infrastructure for planner rule tests.
 * Subclasses get mock backends, cluster state builders, table builders,
 * and plan execution helpers.
 */
public abstract class BasePlannerRulesTests extends OpenSearchTestCase {

    protected static final MockDataFusionBackend DATAFUSION = new MockDataFusionBackend();
    protected static final MockLuceneBackend LUCENE = new MockLuceneBackend();

    protected RelDataTypeFactory typeFactory;
    protected RelOptCluster cluster;
    protected RexBuilder rexBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    // ---- Plan execution ----

    protected RelNode runPlanner(RelNode input, PlannerContext context) {
        return PlannerImpl.markAndOptimize(input, context);
    }

    protected RelNode unwrapExchange(RelNode node) {
        if (node instanceof OpenSearchExchangeReducer reducer) {
            return reducer.getInput();
        }
        return node;
    }

    // ---- Context builders ----

    protected PlannerContext buildContext(String primaryFormat, Map<String, Map<String, Object>> fieldMappings) {
        return buildContext(primaryFormat, 2, fieldMappings, List.of(DATAFUSION, LUCENE));
    }

    protected PlannerContext buildContext(
        String primaryFormat,
        Map<String, Map<String, Object>> fieldMappings,
        List<AnalyticsSearchBackendPlugin> backends
    ) {
        return buildContext(primaryFormat, 2, fieldMappings, backends);
    }

    /**
     * Builds a PlannerContext with explicit per-field storage — for hybrid index tests
     * where a field has doc values in multiple formats (e.g. parquet + lucene).
     */
    protected PlannerContext buildContextWithExplicitStorage(
        int shardCount,
        Map<String, FieldStorageInfo> fieldStorage,
        List<AnalyticsSearchBackendPlugin> backends
    ) {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "uuid"));
        when(indexMetadata.getNumberOfShards()).thenReturn(shardCount);
        when(metadata.index("test_index")).thenReturn(indexMetadata);
        when(clusterState.metadata()).thenReturn(metadata);

        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = idx -> new FieldStorageResolver(fieldStorage);

        return new PlannerContext(new CapabilityRegistry(backends, fieldStorageFactory), clusterState);
    }

    protected PlannerContext buildContext(String primaryFormat, int shardCount, Map<String, Map<String, Object>> fieldMappings) {
        return buildContext(primaryFormat, shardCount, fieldMappings, List.of(DATAFUSION, LUCENE));
    }

    @SuppressWarnings("unchecked")
    protected PlannerContext buildContext(
        String primaryFormat,
        int shardCount,
        Map<String, Map<String, Object>> fieldMappings,
        List<AnalyticsSearchBackendPlugin> backends
    ) {
        Map<String, Object> mappingSource = Map.of("properties", fieldMappings);

        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(mappingSource);

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put("index.composite.primary_data_format", primaryFormat).build());
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(indexMetadata.getNumberOfShards()).thenReturn(shardCount);

        Metadata metadata = mock(Metadata.class);
        when(metadata.index("test_index")).thenReturn(indexMetadata);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;

        return new PlannerContext(new CapabilityRegistry(backends, fieldStorageFactory), clusterState);
    }

    // ---- Table builders ----

    protected RelOptTable mockTable(String tableName, String... fieldNames) {
        SqlTypeName[] types = Arrays.stream(fieldNames).map(name -> SqlTypeName.INTEGER).toArray(SqlTypeName[]::new);
        return mockTable(tableName, fieldNames, types);
    }

    protected RelOptTable mockTable(String tableName, String[] fieldNames, SqlTypeName[] fieldTypes) {
        RelDataTypeFactory.Builder rowTypeBuilder = typeFactory.builder();
        for (int index = 0; index < fieldNames.length; index++) {
            rowTypeBuilder.add(fieldNames[index], typeFactory.createSqlType(fieldTypes[index]));
        }
        RelDataType rowType = rowTypeBuilder.build();

        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(tableName));
        when(table.getRowType()).thenReturn(rowType);
        return table;
    }

    protected TableScan stubScan(RelOptTable table) {
        return new StubTableScan(cluster, cluster.traitSet(), table);
    }

    // ---- RexNode builders ----

    protected RexNode makeEquals(int fieldIndex, SqlTypeName fieldType, Object value) {
        RelDataType type = typeFactory.createSqlType(fieldType);
        return rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(type, fieldIndex),
            rexBuilder.makeLiteral(value, type, true)
        );
    }

    protected RexNode makeCall(SqlOperator operator, RexNode... operands) {
        return rexBuilder.makeCall(operator, operands);
    }

    protected RexNode makeFullTextCall(SqlOperator function, int fieldIndex, String query) {
        return rexBuilder.makeCall(
            function,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), fieldIndex),
            rexBuilder.makeLiteral(query)
        );
    }

    protected RexNode makeAnd(RexNode... operands) {
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, operands);
    }

    /** Builds a set of AggregateCapability for the given function→fieldTypes mapping. */
    protected static Set<AggregateCapability> aggCaps(Set<String> formats, Map<AggregateFunction, Set<FieldType>> funcToTypes) {
        Set<AggregateCapability> caps = new HashSet<>();
        for (var entry : funcToTypes.entrySet()) {
            caps.add(new AggregateCapability(entry.getKey(), entry.getValue(), formats));
        }
        return caps;
    }

    /**
     * Walks the single-input chain asserting each node matches the expected type
     * and has viableBackends containing all expectedBackends.
     * TODO: extend to per-node expected backends when delegation is implemented.
     */
    protected static void assertPipelineViableBackends(
        RelNode root,
        List<Class<? extends OpenSearchRelNode>> expectedTypes,
        Set<String> expectedBackends
    ) {
        RelNode current = root;
        for (int i = 0; i < expectedTypes.size(); i++) {
            Class<? extends OpenSearchRelNode> expectedType = expectedTypes.get(i);
            assertTrue(
                "Node at depth " + i + " must be " + expectedType.getSimpleName() + " but was " + current.getClass().getSimpleName(),
                expectedType.isInstance(current)
            );
            OpenSearchRelNode osNode = (OpenSearchRelNode) current;
            assertTrue(
                "Node "
                    + expectedType.getSimpleName()
                    + " viableBackends must contain "
                    + expectedBackends
                    + " but was "
                    + osNode.getViableBackends(),
                osNode.getViableBackends().containsAll(expectedBackends)
            );
            if (i < expectedTypes.size() - 1) {
                current = RelNodeUtils.unwrapHep(current.getInputs().get(0));
            }
        }
    }

    // ---- Cluster service ----

    protected ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        OperationRouting routing = mock(OperationRouting.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(routing);
        when(routing.searchShards(any(), any(), any(), any())).thenReturn(new GroupShardsIterator<ShardIterator>(List.of()));
        return clusterService;
    }

    // ---- Shared field helpers ----

    protected static Map<String, Map<String, Object>> intFields() {
        return Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer"));
    }

    /**
     * Integer fields with doc values duplicated in both parquet and lucene formats.
     * Models the "duplicated doc values" persona — both backends can natively scan
     * and aggregate the same field.
     */
    protected Map<String, FieldStorageInfo> duplicatedIntFields() {
        return Map.of(
            "status",
            new FieldStorageInfo(
                "status",
                "integer",
                FieldType.INTEGER,
                List.of(MockDataFusionBackend.PARQUET_DATA_FORMAT, MockLuceneBackend.LUCENE_DATA_FORMAT),
                List.of(),
                List.of(),
                false
            ),
            "size",
            new FieldStorageInfo(
                "size",
                "integer",
                FieldType.INTEGER,
                List.of(MockDataFusionBackend.PARQUET_DATA_FORMAT, MockLuceneBackend.LUCENE_DATA_FORMAT),
                List.of(),
                List.of(),
                false
            )
        );
    }

    // ---- Stub ----

    protected static class StubTableScan extends TableScan {
        StubTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, List.of(), table);
        }
    }

    /** Minimal SearchBackEndPlugin for test FieldStorageResolver construction. */
    static class MockStorageBackend implements SearchBackEndPlugin<Object> {
        private final String formatName;
        private final Set<FieldTypeCapabilities> fieldCaps;

        MockStorageBackend(String formatName, Set<FieldTypeCapabilities> fieldCaps) {
            this.formatName = formatName;
            this.fieldCaps = fieldCaps;
        }

        /** Lucene: POINT_RANGE + STORED_FIELDS for numerics/dates, FULL_TEXT_SEARCH + STORED_FIELDS for text/keyword */
        static MockStorageBackend lucene() {
            var C = FieldTypeCapabilities.Capability.class;
            return new MockStorageBackend(
                MockLuceneBackend.LUCENE_DATA_FORMAT,
                Set.of(
                    new FieldTypeCapabilities(
                        "integer",
                        Set.of(FieldTypeCapabilities.Capability.POINT_RANGE, FieldTypeCapabilities.Capability.STORED_FIELDS)
                    ),
                    new FieldTypeCapabilities(
                        "long",
                        Set.of(FieldTypeCapabilities.Capability.POINT_RANGE, FieldTypeCapabilities.Capability.STORED_FIELDS)
                    ),
                    new FieldTypeCapabilities(
                        "keyword",
                        Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH, FieldTypeCapabilities.Capability.STORED_FIELDS)
                    ),
                    new FieldTypeCapabilities(
                        "text",
                        Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH, FieldTypeCapabilities.Capability.STORED_FIELDS)
                    ),
                    new FieldTypeCapabilities("boolean", Set.of(FieldTypeCapabilities.Capability.STORED_FIELDS)),
                    new FieldTypeCapabilities(
                        "date",
                        Set.of(FieldTypeCapabilities.Capability.POINT_RANGE, FieldTypeCapabilities.Capability.STORED_FIELDS)
                    )
                )
            );
        }

        /** Parquet/DataFusion: COLUMNAR_STORAGE for all types */
        static MockStorageBackend parquet() {
            return new MockStorageBackend(
                MockDataFusionBackend.PARQUET_DATA_FORMAT,
                Set.of(
                    new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("long", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("text", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("boolean", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities("date", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE))
                )
            );
        }

        @Override
        public String name() {
            return formatName;
        }

        @Override
        public List<String> getSupportedFormats() {
            return List.of(formatName);
        }

        @Override
        public EngineReaderManager<Object> createReaderManager(ReaderManagerConfig settings) {
            return null;
        }
    }

    // ---- Shared aggregate helpers ----
    // TODO: AggregateRuleTests has private copies of these — replace with these shared versions.

    protected AggregateCall sumCall() {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total_size"
        );
    }

    protected AggregateCall countStarCall() {
        // COUNT(*) — no field arguments, always gets annotated with aggregateCapableBackends
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
    }

    protected LogicalFilter makeFilter(RelNode input, RexNode condition) {
        return LogicalFilter.create(input, condition);
    }

    /** Sort with fetch (LIMIT) only — no ORDER BY collation, just top-N. */
    protected LogicalSort makeLimit(RelNode input, int fetch) {
        return LogicalSort.create(
            input,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    /** Sort with collation on field 0 ascending. Pass fetch of 0 or less for no limit. */
    protected LogicalSort makeSort(RelNode input, int fetch) {
        RelCollation collation = RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING));
        RexNode fetchRex = fetch > 0 ? rexBuilder.makeLiteral(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER), true) : null;
        return LogicalSort.create(input, collation, null, fetchRex);
    }

    protected LogicalAggregate makeAggregate(AggregateCall aggCall) {
        return makeAggregate(stubScan(mockTable("test_index", "status", "size")), aggCall);
    }

    protected LogicalAggregate makeMultiCallAggregate(AggregateCall... aggCalls) {
        return makeMultiCallAggregate(stubScan(mockTable("test_index", "status", "size")), aggCalls);
    }

    protected LogicalAggregate makeAggregate(RelNode input, AggregateCall... aggCalls) {
        return LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(0), null, List.of(aggCalls));
    }

    protected LogicalAggregate makeMultiCallAggregate(RelNode input, AggregateCall... aggCalls) {
        return LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(0), null, List.of(aggCalls));
    }
}
