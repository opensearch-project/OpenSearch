/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.opensearch.analytics.planner.RelNodeUtils.MAX_EXTRACT_INDICES_DEPTH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RelNodeUtils#extractIndices(RelNode)}.
 */
public class RelNodeUtilsTests extends OpenSearchTestCase {

    private RelBuilder builder() {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("my_index", new MockTable());
        schema.add("orders", new MockTable());
        schema.add("customers", new MockTable());
        schema.add("events", new MockTable());
        schema.add("metrics", new MockTable());
        schema.add("logs_2023", new MockTable());
        schema.add("logs_2024", new MockTable());
        schema.add("index_a", new MockTable());
        schema.add("index_b", new MockTable());
        schema.add("deep_index", new MockTable());
        return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema).build());
    }

    public void testSingleTableScan() {
        RelBuilder b = builder();
        RelNode plan = b.scan("my_index").build();
        assertArrayEquals(new String[] { "my_index" }, RelNodeUtils.extractIndices(plan));
    }

    public void testProjectOverScan() {
        RelBuilder b = builder();
        RelNode plan = b.scan("orders").project(b.field("id")).build();
        assertArrayEquals(new String[] { "orders" }, RelNodeUtils.extractIndices(plan));
    }

    public void testSortOverFilterOverScan() {
        RelBuilder b = builder();
        RelNode plan = b.scan("events").filter(b.literal(true)).sort(b.field("id")).build();
        assertArrayEquals(new String[] { "events" }, RelNodeUtils.extractIndices(plan));
    }

    public void testAggregateOverScan() {
        RelBuilder b = builder();
        RelNode plan = b.scan("metrics").aggregate(b.groupKey("id"), b.count(false, "cnt")).build();
        assertArrayEquals(new String[] { "metrics" }, RelNodeUtils.extractIndices(plan));
    }

    public void testJoinExtractsBothIndices() {
        RelBuilder b = builder();
        RelNode plan = b.scan("customers").scan("orders").join(JoinRelType.INNER, b.literal(true)).build();
        assertArrayEquals(new String[] { "customers", "orders" }, RelNodeUtils.extractIndices(plan));
    }

    public void testUnionExtractsBothIndices() {
        RelBuilder b = builder();
        RelNode plan = b.scan("logs_2023")
            .project(b.field("id"), b.field("name"))
            .scan("logs_2024")
            .project(b.field("id"), b.field("name"))
            .union(true)
            .build();
        assertArrayEquals(new String[] { "logs_2023", "logs_2024" }, RelNodeUtils.extractIndices(plan));
    }

    public void testDeduplicatesRepeatedIndex() {
        RelBuilder b = builder();
        RelNode plan = b.scan("my_index")
            .project(b.field("id"), b.field("name"))
            .scan("my_index")
            .project(b.field("id"), b.field("name"))
            .union(true)
            .build();
        assertArrayEquals(new String[] { "my_index" }, RelNodeUtils.extractIndices(plan));
    }

    public void testComplexJoinWithAggregate() {
        RelBuilder b = builder();
        RelNode plan = b.scan("index_a")
            .scan("index_b")
            .join(JoinRelType.LEFT, b.literal(true))
            .aggregate(b.groupKey(0), b.count(false, "cnt"))
            .sort(b.field(0))
            .build();
        assertArrayEquals(new String[] { "index_a", "index_b" }, RelNodeUtils.extractIndices(plan));
    }

    public void testFindTableNameOnJoinReturnsFirstTable() {
        RelBuilder b = builder();
        RelNode plan = b.scan("customers").scan("orders").join(JoinRelType.INNER, b.literal(true)).build();
        assertEquals("customers", RelNodeUtils.findTableName(plan));
    }

    public void testFindTableNameOnUnionReturnsFirstTable() {
        RelBuilder b = builder();
        RelNode plan = b.scan("logs_2023")
            .project(b.field("id"), b.field("name"))
            .scan("logs_2024")
            .project(b.field("id"), b.field("name"))
            .union(true)
            .build();
        assertEquals("logs_2023", RelNodeUtils.findTableName(plan));
    }

    public void testFindTableNameOnSingleScan() {
        RelBuilder b = builder();
        RelNode plan = b.scan("my_index").filter(b.literal(true)).build();
        assertEquals("my_index", RelNodeUtils.findTableName(plan));
    }

    public void testDepthGuardThrowsOnExcessiveDepth() {
        RelBuilder b = builder();
        RelNode node = b.scan("deep_index").build();
        // Wrap in LogicalFilter nodes directly to avoid RelBuilder optimizations.
        for (int i = 0; i < MAX_EXTRACT_INDICES_DEPTH + 5; i++) {
            RexBuilder rex = node.getCluster().getRexBuilder();
            RexNode condition = rex.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                rex.makeInputRef(node.getRowType().getFieldList().get(0).getType(), 0),
                rex.makeZeroLiteral(node.getRowType().getFieldList().get(0).getType())
            );
            node = LogicalFilter.create(node, condition);
        }
        // The plan exceeds MAX_EXTRACT_INDICES_DEPTH — should throw rather than silently skip
        RelNode deepPlan = node;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RelNodeUtils.extractIndices(deepPlan));
        assertTrue(e.getMessage().contains("maximum depth"));
    }

    // --- Multi-index comma-separated table name tests (FGAC bypass fix) ---

    public void testCommaDelimitedIndicesSplit() {
        RelBuilder b = builderWithTable("logs-2024-01,secrets-2024-01");
        RelNode plan = b.scan("logs-2024-01,secrets-2024-01").build();
        assertArrayEquals(new String[] { "logs-2024-01", "secrets-2024-01" }, RelNodeUtils.extractIndices(plan));
    }

    public void testCommaDelimitedThreeIndices() {
        RelBuilder b = builderWithTable("a,b,c");
        RelNode plan = b.scan("a,b,c").build();
        assertArrayEquals(new String[] { "a", "b", "c" }, RelNodeUtils.extractIndices(plan));
    }

    public void testDoubleCommaProducesEmptyStringFiltered() {
        RelBuilder b = builderWithTable("index1,,index2");
        RelNode plan = b.scan("index1,,index2").build();
        // Strings.splitStringByCommaToArray trims and skips empty tokens
        String[] result = RelNodeUtils.extractIndices(plan);
        for (String idx : result) {
            assertFalse("Should not contain empty string", idx.isEmpty());
        }
        assertTrue("Should contain index1", java.util.Arrays.asList(result).contains("index1"));
        assertTrue("Should contain index2", java.util.Arrays.asList(result).contains("index2"));
    }

    public void testLeadingComma() {
        RelBuilder b = builderWithTable(",index1");
        RelNode plan = b.scan(",index1").build();
        String[] result = RelNodeUtils.extractIndices(plan);
        for (String idx : result) {
            assertFalse("Should not contain empty string", idx.isEmpty());
        }
        assertTrue("Should contain index1", java.util.Arrays.asList(result).contains("index1"));
    }

    public void testDoubleLeadingComma() {
        RelBuilder b = builderWithTable(",,index1");
        RelNode plan = b.scan(",,index1").build();
        String[] result = RelNodeUtils.extractIndices(plan);
        for (String idx : result) {
            assertFalse("Should not contain empty string", idx.isEmpty());
        }
        assertTrue("Should contain index1", java.util.Arrays.asList(result).contains("index1"));
    }

    public void testTrailingComma() {
        RelBuilder b = builderWithTable("index1,");
        RelNode plan = b.scan("index1,").build();
        String[] result = RelNodeUtils.extractIndices(plan);
        for (String idx : result) {
            assertFalse("Should not contain empty string", idx.isEmpty());
        }
        assertTrue("Should contain index1", java.util.Arrays.asList(result).contains("index1"));
    }

    public void testSingleIndexNoComma() {
        RelBuilder b = builderWithTable("plain_index");
        RelNode plan = b.scan("plain_index").build();
        assertArrayEquals(new String[] { "plain_index" }, RelNodeUtils.extractIndices(plan));
    }

    private RelBuilder builderWithTable(String tableName) {
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add(tableName, new MockTable());
        return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema).build());
    }

    // ===== carriedResolution propagation tests =====
    // These exist because dropping the field breaks shard targeting at execution time:
    // ShardTargetResolver throws IllegalStateException when carriedResolution is null.

    /**
     * copyToCluster rebuilds every node in a new RelOptCluster; the resulting OpenSearchTableScan
     * must carry the same IndexResolution instance as the original.
     */
    public void testCopyToClusterPreservesCarriedResolution() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster originalCluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
        RelOptCluster newCluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);

        IndexResolution resolution = stubResolution("my_alias", List.of(mockIndexMetadata("idx_a", "uuid-a")));

        RelDataType rowType = typeFactory.builder().add("v", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("my_alias"));
        when(table.getRowType()).thenReturn(rowType);

        OpenSearchTableScan scan = new OpenSearchTableScan(
            originalCluster,
            originalCluster.traitSet(),
            table,
            List.of("datafusion"),
            List.of(),
            null,
            resolution
        );

        OpenSearchDistributionTraitDef distTraitDef = mock(OpenSearchDistributionTraitDef.class);
        RelNode copied = RelNodeUtils.copyToCluster(scan, newCluster, distTraitDef);

        assertTrue("copyToCluster must return OpenSearchTableScan", copied instanceof OpenSearchTableScan);
        OpenSearchTableScan copiedScan = (OpenSearchTableScan) copied;
        assertSame("copyToCluster must preserve the carried IndexResolution instance", resolution, copiedScan.getCarriedResolution());
        assertSame("copied scan must belong to the new cluster", newCluster, copiedScan.getCluster());
    }

    /**
     * The QTF (late materialization) narrowed-scan path constructs a new OpenSearchTableScan
     * with an overrideRowType and forwards origScan.getCarriedResolution(). This test verifies
     * the 7-arg constructor (the path buildNarrowedScan uses) preserves the resolution when an
     * override rowType is provided.
     */
    public void testNarrowedScanPreservesCarriedResolution() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);

        IndexResolution resolution = stubResolution(
            "test_alias",
            List.of(mockIndexMetadata("backing_a", "uuid-a"), mockIndexMetadata("backing_b", "uuid-b"))
        );

        RelDataType fullRowType = typeFactory.builder()
            .add("col_a", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("col_b", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("test_alias"));
        when(table.getRowType()).thenReturn(fullRowType);

        // Narrowed rowType — fewer columns, simulating the QTF narrowing that retains only
        // sort/filter columns plus ___row_id.
        RelDataType narrowedRowType = typeFactory.builder()
            .add("col_a", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("___row_id", typeFactory.createSqlType(SqlTypeName.BIGINT))
            .build();

        List<FieldStorageInfo> narrowedStorage = List.of(
            FieldStorageInfo.derivedColumn("col_a", SqlTypeName.INTEGER),
            FieldStorageInfo.derivedColumn("___row_id", SqlTypeName.BIGINT)
        );

        OpenSearchTableScan narrowedScan = new OpenSearchTableScan(
            cluster,
            cluster.traitSet(),
            table,
            List.of("datafusion"),
            narrowedStorage,
            narrowedRowType,
            resolution
        );

        assertSame(
            "Narrowed scan (override rowType path) must preserve the carried IndexResolution",
            resolution,
            narrowedScan.getCarriedResolution()
        );
        assertEquals("Narrowed scan must use the override rowType", 2, narrowedScan.getRowType().getFieldCount());
        assertEquals("___row_id", narrowedScan.getRowType().getFieldList().get(1).getName());
    }

    // ---- Helpers for carriedResolution tests ----

    private IndexResolution stubResolution(String requestedName, List<IndexMetadata> indices) {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(state.metadata()).thenReturn(metadata);

        if (indices.size() == 1) {
            IndexMetadata imd = indices.get(0);
            IndexAbstraction abstraction = mock(IndexAbstraction.class);
            when(abstraction.getType()).thenReturn(IndexAbstraction.Type.CONCRETE_INDEX);
            when(abstraction.getIndices()).thenReturn(List.of(imd));
            when(imd.getState()).thenReturn(IndexMetadata.State.OPEN);
            TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
            lookup.put(requestedName, abstraction);
            when(metadata.getIndicesLookup()).thenReturn(lookup);
        } else {
            for (IndexMetadata imd : indices) {
                when(imd.getState()).thenReturn(IndexMetadata.State.OPEN);
                AliasMetadata aliasMd = mock(AliasMetadata.class);
                when(aliasMd.filteringRequired()).thenReturn(false);
                when(imd.getAliases()).thenReturn(Map.of(requestedName, aliasMd));
            }
            IndexAbstraction aliasAbstraction = mock(IndexAbstraction.class);
            when(aliasAbstraction.getType()).thenReturn(IndexAbstraction.Type.ALIAS);
            when(aliasAbstraction.getIndices()).thenReturn(indices);
            TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
            lookup.put(requestedName, aliasAbstraction);
            when(metadata.getIndicesLookup()).thenReturn(lookup);
        }

        return IndexResolution.resolve(requestedName, state);
    }

    private IndexMetadata mockIndexMetadata(String name, String uuid) {
        IndexMetadata imd = mock(IndexMetadata.class);
        when(imd.getIndex()).thenReturn(new Index(name, uuid));
        when(imd.getNumberOfShards()).thenReturn(1);
        when(imd.getState()).thenReturn(IndexMetadata.State.OPEN);
        return imd;
    }

    /** Minimal table implementation for RelBuilder schema registration. */
    private static class MockTable extends AbstractTable {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("id", SqlTypeName.INTEGER).add("name", SqlTypeName.VARCHAR).build();
        }
    }
}
