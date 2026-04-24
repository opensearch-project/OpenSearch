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
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;

/**
 * Tests for {@link DataFusionFragmentConvertor}.
 * Builds Calcite RelNode trees in-process, converts them via the convertor,
 * then decodes the returned Substrait proto bytes and asserts on proto shape.
 */
public class DataFusionFragmentConvertorTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DataFusionFragmentConvertorTests.class.getClassLoader());
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    private RelDataType rowType(String... columns) {
        RelDataTypeFactory.Builder b = typeFactory.builder();
        for (String c : columns) {
            b.add(c, typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        }
        return b.build();
    }

    private RelNode buildTableScan(String tableName, String... columns) {
        return new MinimalTableScan(cluster, cluster.traitSet(), tableName, rowType(columns));
    }

    private DataFusionFragmentConvertor newConvertor() {
        return new DataFusionFragmentConvertor(extensions);
    }

    private Plan decodeSubstrait(byte[] bytes) throws Exception {
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
        return Plan.parseFrom(bytes);
    }

    private Rel rootRel(Plan plan) {
        assertFalse(plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue(planRel.hasRoot());
        return planRel.getRoot().getInput();
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    public void testConvertShardScanFragmentTableScan() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        byte[] bytes = newConvertor().convertShardScanFragment("test_index", scan);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue(root.hasRead());
        ReadRel read = root.getRead();
        assertTrue(read.hasNamedTable());
        assertEquals(List.of("test_index"), read.getNamedTable().getNamesList());
    }

    public void testConvertShardScanFragmentFilterOverScan() throws Exception {
        RelNode scan = buildTableScan("test_index", "A", "B");
        RexNode predicate = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(scan, 0),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode filter = LogicalFilter.create(scan, predicate);

        byte[] bytes = newConvertor().convertShardScanFragment("test_index", filter);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue(root.hasFilter());
        FilterRel filterRel = root.getFilter();
        assertTrue(filterRel.hasCondition());
        Rel inner = filterRel.getInput();
        assertTrue(inner.hasRead());
        assertEquals(List.of("test_index"), inner.getRead().getNamedTable().getNamesList());
    }

    public void testConvertShardScanFragmentProducesNonEmptyBytes() throws Exception {
        RelNode scan = buildTableScan("my_index", "col1");
        byte[] bytes = newConvertor().convertShardScanFragment("my_index", scan);

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }

    public void testConvertShardScanFragmentStripsTableNamePrefix() throws Exception {
        RelNode scan = new MinimalTableScan(cluster, cluster.traitSet(), List.of("opensearch", "schema", "my_table"), rowType("X"));
        byte[] bytes = newConvertor().convertShardScanFragment("my_table", scan);

        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);
        assertTrue(root.hasRead());
        assertEquals(List.of("my_table"), root.getRead().getNamedTable().getNamesList());
    }

    public void testAttachPartialAggOnTopThrowsUnsupported() {
        RelNode scan = buildTableScan("t", "A");
        expectThrows(UnsupportedOperationException.class, () -> newConvertor().attachPartialAggOnTop(scan, new byte[0]));
    }

    public void testConvertFinalAggFragmentThrowsUnsupported() {
        RelNode scan = buildTableScan("t", "A");
        expectThrows(UnsupportedOperationException.class, () -> newConvertor().convertFinalAggFragment(scan));
    }

    public void testAttachFragmentOnTopThrowsUnsupported() {
        RelNode scan = buildTableScan("t", "A");
        expectThrows(UnsupportedOperationException.class, () -> newConvertor().attachFragmentOnTop(scan, new byte[0]));
    }

    public void testExtensionCollectionReuse() throws Exception {
        DataFusionFragmentConvertor convertor = newConvertor();
        RelNode scan = buildTableScan("t", "A");
        byte[] first = convertor.convertShardScanFragment("t", scan);
        byte[] second = convertor.convertShardScanFragment("t", scan);
        assertNotNull(first);
        assertNotNull(second);
        assertArrayEquals(first, second);
    }

    // ── Minimal Calcite stubs for building test RelNodes ─────────────────────────

    static final class MinimalTableScan extends TableScan {
        MinimalTableScan(RelOptCluster cluster, RelTraitSet traitSet, String tableName, RelDataType rowType) {
            this(cluster, traitSet, List.of(tableName), rowType);
        }

        MinimalTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<String> qualifiedName, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new MinimalRelOptTable(qualifiedName, rowType));
        }
    }

    static final class MinimalRelOptTable implements RelOptTable {
        private final List<String> qualifiedName;
        private final RelDataType rowType;

        MinimalRelOptTable(List<String> qualifiedName, RelDataType rowType) {
            this.qualifiedName = qualifiedName;
            this.rowType = rowType;
        }

        @Override
        public List<String> getQualifiedName() {
            return qualifiedName;
        }

        @Override
        public RelDataType getRowType() {
            return rowType;
        }

        @Override
        public double getRowCount() {
            return 100;
        }

        @Override
        public RelOptSchema getRelOptSchema() {
            return null;
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            return List.of();
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        @Override
        public List<ImmutableBitSet> getKeys() {
            return List.of();
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        @Override
        public List<RelCollation> getCollationList() {
            return List.of();
        }

        @Override
        public RelDistribution getDistribution() {
            return RelDistributions.ANY;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
            return null;
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            return this;
        }
    }
}
