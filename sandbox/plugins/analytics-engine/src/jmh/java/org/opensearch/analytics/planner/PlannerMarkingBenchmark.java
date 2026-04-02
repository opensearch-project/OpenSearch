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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.index.Index;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * JMH benchmark for the analytics planner marking phase (HEP + CBO).
 * Measures end-to-end plan creation time for different query shapes.
 */
@Fork(2)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class PlannerMarkingBenchmark {

    private static final SqlFunction PAINLESS = new SqlFunction(
        "painless", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000,
        null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Param({ "scan", "filter", "aggregate", "project_scalar", "project_delegation", "filter_agg" })
    String queryShape;

    private RelNode inputRelNode;
    private PlannerContext plannerContext;

    @Setup(Level.Trial)
    public void setup() {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, rexBuilder);

        String[] fieldNames = { "status", "size", "country", "message" };
        SqlTypeName[] fieldTypes = { SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR };

        RelDataTypeFactory.Builder rowTypeBuilder = typeFactory.builder();
        for (int i = 0; i < fieldNames.length; i++) {
            rowTypeBuilder.add(fieldNames[i], typeFactory.createSqlType(fieldTypes[i]));
        }
        RelDataType rowType = rowTypeBuilder.build();

        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("http_logs"));
        when(table.getRowType()).thenReturn(rowType);

        TableScan scan = new StubTableScan(cluster, cluster.traitSet(), table);

        AnalyticsSearchBackendPlugin dfBackend = new MockDataFusionBackend();
        AnalyticsSearchBackendPlugin luceneBackend = new MockLuceneBackend();

        plannerContext = buildContext(List.of(dfBackend, luceneBackend));
        inputRelNode = buildInput(queryShape, scan, rexBuilder, typeFactory);
    }

    @Benchmark
    public void planCreation(Blackhole blackhole) {
        blackhole.consume(PlannerImpl.createPlan(inputRelNode, plannerContext));
    }

    private RelNode buildInput(String shape, TableScan scan, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        return switch (shape) {
            case "scan" -> scan;
            case "filter" -> LogicalFilter.create(scan,
                rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                    rexBuilder.makeLiteral(200, typeFactory.createSqlType(SqlTypeName.INTEGER), true)));
            case "aggregate" -> LogicalAggregate.create(scan,
                List.of(), ImmutableBitSet.of(0), null,
                List.of(makeAggCall(SqlStdOperatorTable.SUM, 1, typeFactory, scan)));
            case "project_scalar" -> LogicalProject.create(scan, List.of(),
                List.of(
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                    rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1))
                ), List.of("status", "size_str"));
            case "project_delegation" -> LogicalProject.create(scan, List.of(),
                List.of(
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2),
                    rexBuilder.makeCall(PAINLESS,
                        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2))
                ), List.of("country", "scripted"));
            case "filter_agg" -> {
                RelNode filtered = LogicalFilter.create(scan,
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2),
                        rexBuilder.makeLiteral("US")));
                yield LogicalAggregate.create(filtered,
                    List.of(), ImmutableBitSet.of(0), null,
                    List.of(makeAggCall(SqlStdOperatorTable.SUM, 1, typeFactory, filtered)));
            }
            default -> throw new IllegalArgumentException("Unknown shape: " + shape);
        };
    }

    private static AggregateCall makeAggCall(SqlAggFunction func, int argIndex,
                                             RelDataTypeFactory typeFactory, RelNode input) {
        return AggregateCall.create(func, false, List.of(argIndex), 1, input,
            typeFactory.createSqlType(SqlTypeName.BIGINT), "agg_col");
    }

    @SuppressWarnings("unchecked")
    private PlannerContext buildContext(List<AnalyticsSearchBackendPlugin> backends) {
        Map<String, Object> mappingSource = Map.of("properties", Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "long"),
            "country", Map.of("type", "keyword"),
            "message", Map.of("type", "keyword")
        ));

        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(mappingSource);

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("http_logs", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(
            Settings.builder().put("index.composite.primary_data_format", "parquet").build());
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(indexMetadata.getNumberOfShards()).thenReturn(2);

        Metadata metadata = mock(Metadata.class);
        when(metadata.index("http_logs")).thenReturn(indexMetadata);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        return new PlannerContext(new CapabilityRegistry(backends), clusterState);
    }

    static class StubTableScan extends TableScan {
        StubTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, List.of(), table);
        }
    }
}
