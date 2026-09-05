/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * End-to-end plan-shape coverage for {@link ObjectStructMaterializer}: an OpenSearch
 * {@code object} field is re-assembled from its flat dotted leaf columns into a
 * {@code make_struct} call in a project directly above the scan.
 *
 * <p>The scan carries <em>only</em> leaf columns — an object parent has no physical storage
 * ({@code FieldStorageResolver.populateFromProperties} recurses past object parents), so the
 * struct is appended by the project, never read from the scan:
 *
 * <pre>
 * id                               INTEGER
 * nested_metadata.top              VARCHAR
 * nested_metadata.properties.name  VARCHAR
 * nested_metadata.properties.value VARCHAR
 * </pre>
 */
public class ObjectStructPlanShapeTests extends PlanShapeTestBase {

    /**
     * Table as the schema builder now produces it for an {@code object} mapping: flat dotted leaf
     * columns PLUS the struct-typed parent column.
     */
    private RelOptTable objectTable() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType properties = typeFactory.createStructType(List.of(varchar, varchar), List.of("name", "value"));
        RelDataType meta = typeFactory.createStructType(List.of(varchar, properties), List.of("top", "properties"));

        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("nested_metadata.top", varchar);
        builder.add("nested_metadata.properties.name", varchar);
        builder.add("nested_metadata.properties.value", varchar);
        builder.add("nested_metadata", meta);
        return mockTable("test_index", builder.build());
    }

    /** Field mappings for the leaf columns — the object parent is intentionally absent. */
    private Map<String, Map<String, Object>> leafFieldMappings() {
        return Map.of(
            "id",
            Map.of("type", "integer"),
            "nested_metadata.top",
            Map.of("type", "keyword"),
            "nested_metadata.properties.name",
            Map.of("type", "keyword"),
            "nested_metadata.properties.value",
            Map.of("type", "keyword")
        );
    }

    /**
     * The core rewrite: a project above the scan passes the leaves through and appends the
     * object, nesting a second {@code make_struct} for the {@code properties} sub-object.
     */
    public void testMaterializerAppendsNestedStructProjectAboveScan() {
        RelNode scan = stubScan(objectTable());

        Optional<RelNode> rewritten = ObjectStructMaterializer.rewrite(scan);

        assertTrue("materializer should fire when an object's leaves are present", rewritten.isPresent());
        assertPlanShape(
            """
                LogicalProject(id=[$0], nested_metadata.top=[$1], nested_metadata.properties.name=[$2], nested_metadata.properties.value=[$3], nested_metadata=[make_struct('top':VARCHAR, $1, 'properties':VARCHAR, make_struct('name':VARCHAR, $2, 'value':VARCHAR, $3))])
                  LogicalTableScan(table=[[test_index]])
                """,
            rewritten.get()
        );
    }

    /** No object spec ⇒ no rewrite, so plans without objects are untouched. */
    public void testMaterializerNoOpWithoutObjectSpec() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        assertFalse(ObjectStructMaterializer.rewrite(scan).isPresent());
    }

    /**
     * A struct column whose backing leaves are absent from the scan (e.g. an unsupported sub-field
     * type was dropped from the schema) yields a typed NULL, never a partially-filled struct.
     */
    public void testMaterializerEmitsNullForObjectWithMissingLeaf() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType meta = typeFactory.createStructType(List.of(varchar), List.of("top"));
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        // NOTE: no "nested_metadata.top" leaf column — the struct cannot be assembled.
        builder.add("nested_metadata", meta);
        RelNode scan = stubScan(mockTable("test_index", builder.build()));

        RelNode rewritten = ObjectStructMaterializer.rewrite(scan).orElseThrow();

        String shape = RelOptUtil.toString(rewritten);
        assertFalse("no partial struct should be emitted, got:\n" + shape, shape.contains("make_struct"));
        assertTrue("expected a typed NULL for the unassemblable object, got:\n" + shape, shape.contains("null:RecordType"));
    }

    /**
     * Projecting the object returns the whole object: the materialized struct survives the full
     * planner (marking, CBO) and sits above the scan.
     */
    public void testProjectOnObjectReturnsWholeObjectThroughPlanner() {
        RelNode scan = stubScan(objectTable());
        RelNode materialized = ObjectStructMaterializer.rewrite(scan).orElseThrow();
        int objectIndex = materialized.getRowType().getFieldCount() - 1;
        RelNode plan = LogicalProject.create(
            materialized,
            List.of(),
            List.of(rexBuilder.makeInputRef(materialized, 0), rexBuilder.makeInputRef(materialized, objectIndex)),
            List.of("id", "nested_metadata")
        );

        RelNode result = runPlanner(plan, buildContext("parquet", 1, leafFieldMappings()));

        String shape = RelOptUtil.toString(result);
        assertTrue("expected a materialized struct, got:\n" + shape, shape.contains("make_struct"));
        assertTrue(
            "sub-object must nest a second make_struct, got:\n" + shape,
            shape.indexOf("make_struct") != shape.lastIndexOf("make_struct")
        );
        assertTrue("struct assembly must sit above the scan, got:\n" + shape, shape.indexOf("make_struct") < shape.indexOf("TableScan"));
    }

    /**
     * {@code stats count() by nested_metadata} — the struct is materialized in a project below the
     * aggregate, so the aggregate receives an already-assembled object, not raw leaves.
     *
     * <p>Grouping (rather than {@code count(nested_metadata)}) is the meaningful probe: counting a
     * non-nullable column is equivalent to {@code count(*)}, so Calcite drops the column reference
     * and trims the project away — correctly, but it proves nothing about materialization. A group
     * key genuinely needs the struct's value.
     */
    public void testAggregateOnObjectMaterializesStructBeforeAgg() {
        RelNode scan = stubScan(objectTable());
        RelNode materialized = ObjectStructMaterializer.rewrite(scan).orElseThrow();
        int objectIndex = materialized.getRowType().getFieldCount() - 1;
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            materialized,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode plan = makeAggregate(materialized, ImmutableBitSet.of(objectIndex), count);

        RelNode result = runPlanner(plan, buildContext("parquet", 1, leafFieldMappings()));

        String shape = RelOptUtil.toString(result);
        assertTrue("expected a materialized struct, got:\n" + shape, shape.contains("make_struct"));
        int aggAt = shape.indexOf("Aggregate");
        int structAt = shape.indexOf("make_struct");
        assertTrue("aggregate must sit above the struct-materializing project, got:\n" + shape, aggAt >= 0 && aggAt < structAt);
        assertTrue("struct assembly must sit above the scan, got:\n" + shape, structAt < shape.indexOf("TableScan"));
    }

    /**
     * Multi-shard: grouping on the materialized object still splits into PARTIAL/FINAL. The group set
     * is {@code {0}} over a single-column input, so it satisfies
     * {@code OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit}'s prefix check and the
     * aggregate distributes normally — the struct as a group key costs throughput (DataFusion has no
     * {@code Struct} specialization in its columnar group-values path, so it row-encodes) but does
     * not cost distribution. Pinned because the reduce path is unreachable at one shard.
     */
    public void testAggregateOnObject_2shard() {
        RelNode scan = stubScan(objectTable());
        RelNode materialized = ObjectStructMaterializer.rewrite(scan).orElseThrow();
        int objectIndex = materialized.getRowType().getFieldCount() - 1;
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            materialized,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode plan = makeAggregate(materialized, ImmutableBitSet.of(objectIndex), count);

        RelNode result = runPlanner(plan, buildContext("parquet", 2, leafFieldMappings()));

        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchProject(nested_metadata=[ANNOTATED_PROJECT_EXPR(id=1, backends=[mock-parquet], make_struct('top':VARCHAR, $1, 'properties':VARCHAR, ANNOTATED_PROJECT_EXPR(id=0, backends=[mock-parquet], make_struct('name':VARCHAR, $2, 'value':VARCHAR, $3))))], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }
}
