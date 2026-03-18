/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import net.jqwik.api.Example;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.be.lucene.predicate.QueryBuilderSerializer;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link LuceneFilterExecutor}.
 */
class LuceneFilterExecutorTests {

    private final LuceneFilterExecutor bridge = new LuceneFilterExecutor();
    private final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    private RelOptCluster createCluster() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, rexBuilder);
    }

    private RelOptTable createTable(RelOptCluster cluster) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        SchemaPlus schemaPlus = rootSchema.plus();
        schemaPlus.add("test_table", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                return tf.builder()
                    .add("verb", tf.createSqlType(SqlTypeName.VARCHAR))
                    .add("path", tf.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }
        });
        Properties props = new Properties();
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema, Collections.singletonList(""), typeFactory, config
        );
        return catalogReader.getTable(List.of("test_table"));
    }

    private LogicalFilter buildEqualityFilter(String value) {
        RelOptCluster cluster = createCluster();
        RelOptTable table = createTable(cluster);
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());
        RexNode fieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        RexNode literal = rexBuilder.makeLiteral(value);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);
        return LogicalFilter.create(scan, condition);
    }

    // --- convertFragment tests ---

    @Example
    void convertFragmentWithNullThrowsNullPointerException() {
        assertThatThrownBy(() -> bridge.convertFragment(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Example
    void convertFragmentWithEqualityProducesSerializedTermQuery() {
        LogicalFilter filter = buildEqualityFilter("GET");

        byte[] result = bridge.convertFragment(filter);

        assertThat(result).isNotNull().isNotEmpty();

        QueryBuilder deserialized = QueryBuilderSerializer.deserialize(result);
        assertThat(deserialized).isInstanceOf(TermQueryBuilder.class);
        TermQueryBuilder termQuery = (TermQueryBuilder) deserialized;
        assertThat(termQuery.fieldName()).isEqualTo("verb");
        assertThat(termQuery.value()).isEqualTo("GET");
    }

    @Example
    void convertFragmentWithNonFilterThrowsIllegalArgument() {
        RelOptCluster cluster = createCluster();
        RelOptTable table = createTable(cluster);
        LogicalTableScan scan = LogicalTableScan.create(cluster, table, List.of());

        assertThatThrownBy(() -> bridge.convertFragment(scan))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("LogicalFilter");
    }

    // --- execute tests ---

    @Example
    void executeWithNullThrowsIllegalArgumentException() {
        assertThatThrownBy(() -> bridge.execute(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("null or empty");
    }

    @Example
    void executeWithEmptyArrayThrowsIllegalArgumentException() {
        assertThatThrownBy(() -> bridge.execute(new byte[0]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("null or empty");
    }

    @Example
    void executeWithCorruptedBytesThrowsIllegalArgumentException() {
        byte[] garbage = new byte[] { 0x00, 0x01, 0x02, 0x03 };
        assertThatThrownBy(() -> bridge.execute(garbage))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // --- end-to-end test ---

    @Example
    void endToEndEqualityConvertAndExecuteReturnsResult() {
        LogicalFilter filter = buildEqualityFilter("GET");

        // Coordinator side: convert RelNode → byte[]
        byte[] serialized = bridge.convertFragment(filter);
        assertThat(serialized).isNotNull().isNotEmpty();

        // Data node side: byte[] → Iterator<VectorSchemaRoot>
        Iterator<VectorSchemaRoot> results = bridge.execute(serialized);
        assertThat(results).isNotNull();
        assertThat(results.hasNext()).isTrue();

        VectorSchemaRoot root = results.next();
        assertThat(root).isNotNull();
        assertThat(root.getSchema().getFields()).hasSize(1);
        assertThat(root.getSchema().getFields().get(0).getName()).isEqualTo(LuceneFilterExecutor.DOC_IDS_COLUMN);
        // Empty result (no shard context available outside a running node)
        assertThat(root.getRowCount()).isEqualTo(0);

        root.close();
    }

    // --- backward compatibility: no-arg constructor returns empty result from execute ---

    @Example
    void noArgConstructorExecuteReturnsEmptyDocIdsBitSet() {
        LuceneFilterExecutor noArgBridge = new LuceneFilterExecutor();
        byte[] serialized = QueryBuilderSerializer.serialize(new TermQueryBuilder("verb", "GET"));

        Iterator<VectorSchemaRoot> results = noArgBridge.execute(serialized);

        assertThat(results).isNotNull();
        assertThat(results.hasNext()).isTrue();

        VectorSchemaRoot root = results.next();
        assertThat(root.getRowCount()).isEqualTo(0);
        assertThat(root.getSchema().getFields().get(0).getName()).isEqualTo(LuceneFilterExecutor.DOC_IDS_COLUMN);

        root.close();
    }
}
