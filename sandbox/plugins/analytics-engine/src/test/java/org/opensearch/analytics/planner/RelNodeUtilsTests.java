/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.jdbc.CalciteSchema;
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
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.analytics.planner.RelNodeUtils.MAX_EXTRACT_INDICES_DEPTH;

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

    /** Minimal table implementation for RelBuilder schema registration. */
    private static class MockTable extends AbstractTable {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("id", SqlTypeName.INTEGER).add("name", SqlTypeName.VARCHAR).build();
        }
    }
}
