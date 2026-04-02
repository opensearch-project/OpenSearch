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
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        return PlannerImpl.createPlan(input, context);
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

    protected PlannerContext buildContext(String primaryFormat, Map<String, Map<String, Object>> fieldMappings,
                                         List<AnalyticsSearchBackendPlugin> backends) {
        return buildContext(primaryFormat, 2, fieldMappings, backends);
    }

    protected PlannerContext buildContext(String primaryFormat, int shardCount,
                                         Map<String, Map<String, Object>> fieldMappings) {
        return buildContext(primaryFormat, shardCount, fieldMappings, List.of(DATAFUSION, LUCENE));
    }

    @SuppressWarnings("unchecked")
    protected PlannerContext buildContext(String primaryFormat, int shardCount,
                                         Map<String, Map<String, Object>> fieldMappings,
                                         List<AnalyticsSearchBackendPlugin> backends) {
        Map<String, Object> mappingSource = Map.of("properties", fieldMappings);

        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(mappingSource);

        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new org.opensearch.core.index.Index("test_index", "uuid"));
        when(indexMetadata.getSettings()).thenReturn(
            Settings.builder().put("index.composite.primary_data_format", primaryFormat).build()
        );
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(indexMetadata.getNumberOfShards()).thenReturn(shardCount);

        Metadata metadata = mock(Metadata.class);
        when(metadata.index("test_index")).thenReturn(indexMetadata);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        return new PlannerContext(new CapabilityRegistry(backends), clusterState);
    }

    // ---- Table builders ----

    protected RelOptTable mockTable(String tableName, String... fieldNames) {
        SqlTypeName[] types = Arrays.stream(fieldNames)
            .map(name -> SqlTypeName.INTEGER)
            .toArray(SqlTypeName[]::new);
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

    // ---- Stub ----

    protected static class StubTableScan extends TableScan {
        StubTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, List.of(), table);
        }
    }
}
