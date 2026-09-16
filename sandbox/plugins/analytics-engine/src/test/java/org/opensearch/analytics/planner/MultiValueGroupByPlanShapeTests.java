/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Full-planner ({@link PlannerImpl#runAllOptimizations}) coverage for LIST-valued GROUP BY
 * keys. The unit tests in {@code OpenSearchMultiValueGroupByRewriterTests} call the rewriter
 * directly; these go through every phase (subquery-remove, trim, pushdown, the multi-value
 * expansion, aggregate decomposition, marking, CBO) so a gap anywhere in that chain — e.g. a
 * {@code LogicalCorrelate} that marking does not know how to lower — surfaces here.
 *
 * <p>The load-bearing assertion is the 2-shard one: after {@code OpenSearchAggregateSplitRule}
 * both the PARTIAL and FINAL aggregates must group on the SCALAR element key. That is the whole
 * point of running the expansion before {@code decomposeAggregates} — the pre-existing
 * backend-side rewrite left the FINAL fragment grouping on a {@code List(Utf8)} key and failed
 * at execution with a {@code List(Utf8)} vs {@code Utf8View} mismatch.
 */
public class MultiValueGroupByPlanShapeTests extends PlanShapeTestBase {

    private static Map<String, Map<String, Object>> listFields() {
        return Map.of(
            "tags",
            Map.of("type", "keyword", "multi_value", true),
            "colors",
            Map.of("type", "keyword", "multi_value", true),
            "status",
            Map.of("type", "integer")
        );
    }

    private PlannerContext listContext(int shards) {
        return buildContext("parquet", shards, listFields());
    }

    private RelDataType nullableStringList() {
        RelDataType element = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
        return typeFactory.createTypeWithNullability(typeFactory.createArrayType(element, -1), true);
    }

    /** {@code test_index(tags: LIST<VARCHAR>, colors: LIST<VARCHAR>, status: INTEGER)}. */
    private TableScan listScan() {
        RelDataType rowType = typeFactory.builder()
            .add("tags", nullableStringList())
            .add("colors", nullableStringList())
            .add("status", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("test_index"));
        when(table.getRowType()).thenReturn(rowType);
        return stubScan(table);
    }

    private AggregateCall count(RelNode input) {
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
    }

    // ---- stats count() by tags ---------------------------------------------------------------

    public void testCountByListKey_1shard_planSucceeds() {
        TableScan scan = listScan();
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(0), count(scan));

        RelNode result = runPlanner(plan, listContext(1));

        List<OpenSearchAggregate> aggs = RelNodeUtils.findNodes(result, OpenSearchAggregate.class);
        assertEquals("single-shard: exactly one SINGLE aggregate", 1, aggs.size());
        assertEquals(AggregateMode.SINGLE, aggs.get(0).getMode());
        assertGroupKeyIsScalar(aggs.get(0));
    }

    /**
     * The regression this whole change targets. Before: FINAL grouped on {@code List(Utf8)}
     * while PARTIAL emitted {@code Utf8View} rows → runtime type mismatch on multi-shard.
     */
    public void testCountByListKey_2shard_bothFragmentsGroupOnScalar() {
        TableScan scan = listScan();
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(0), count(scan));

        RelNode result = runPlanner(plan, listContext(2));

        // The load-bearing shape: expansion runs SHARD-side under the PARTIAL; the ER carries
        // pre-aggregated (element, count) partials — not raw expanded rows — to the FINAL.
        assertPlanShape(
            """
                OpenSearchProject(tags=[$0], cnt=[$1], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
                      OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchProject(___mvexpand_0=[$1], tags=[$0], viableBackends=[[mock-parquet]])
                          OpenSearchMultiValueExpand(field=[tags], output=[___mvexpand_0], distinct=[true], viableBackends=[[mock-parquet]])
                            OpenSearchProject(tags=[$0], viableBackends=[[mock-parquet]])
                              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );

        List<OpenSearchAggregate> aggs = RelNodeUtils.findNodes(result, OpenSearchAggregate.class);
        assertEquals("multi-shard: PARTIAL + FINAL", 2, aggs.size());
        OpenSearchAggregate finalAgg = aggs.stream().filter(a -> a.getMode() == AggregateMode.FINAL).findFirst().orElseThrow();
        OpenSearchAggregate partialAgg = aggs.stream().filter(a -> a.getMode() == AggregateMode.PARTIAL).findFirst().orElseThrow();
        assertGroupKeyIsScalar(partialAgg);
        assertGroupKeyIsScalar(finalAgg);
        // The expansion must survive marking + CBO, be assigned a backend, and run SHARD-side
        // (under the PARTIAL) — not gathered to the coordinator.
        List<org.opensearch.analytics.planner.rel.OpenSearchMultiValueExpand> expands = RelNodeUtils.findNodes(
            result,
            org.opensearch.analytics.planner.rel.OpenSearchMultiValueExpand.class
        );
        assertEquals(1, expands.size());
        assertFalse("expand must be marked with a viable backend", expands.get(0).getViableBackends().isEmpty());
        assertFalse(
            "expand must run shard-side beneath the PARTIAL aggregate",
            RelNodeUtils.findNodes(partialAgg, org.opensearch.analytics.planner.rel.OpenSearchMultiValueExpand.class).isEmpty()
        );
        // Root output still exposes the user-facing name and the scalar element type.
        assertEquals(List.of("tags", "cnt"), result.getRowType().getFieldNames());
        assertNull("root group key must be the scalar element", result.getRowType().getFieldList().get(0).getType().getComponentType());
    }

    // ---- stats list(tags) by tags — same field as BOTH agg arg and group key ------------------

    /**
     * The reason the expansion APPENDS rather than replaces: {@code list(tags)} must aggregate
     * the source LIST (via {@code mv_collect} downstream) while {@code by tags} groups on the
     * expanded scalar element. LIST is STATE_EXPANDING, so on multi-shard this does NOT split —
     * the expansion still runs shard-side and the gather ships expanded rows to a SINGLE
     * aggregate; the point here is that both references land on the right columns.
     */
    public void testListOfSameFieldGroupedByThatField_2shard_aggArgIsSourceList() {
        TableScan scan = listScan();
        AggregateCall list = AggregateCall.create(
            listAggFunction(),
            false,
            false,
            false,
            List.of(),
            List.of(0),
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            1,
            scan,
            scan.getRowType().getFieldList().get(0).getType(),
            "values"
        );
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(0), list);

        RelNode result = runPlanner(plan, listContext(2));

        List<OpenSearchAggregate> aggs = RelNodeUtils.findNodes(result, OpenSearchAggregate.class);
        assertEquals("STATE_EXPANDING LIST does not split", 1, aggs.size());
        OpenSearchAggregate agg = aggs.get(0);
        assertEquals(AggregateMode.SINGLE, agg.getMode());
        assertGroupKeyIsScalar(agg);
        RelDataType inputType = agg.getInput().getRowType();
        int argIndex = agg.getAggCallList().get(0).getArgList().get(0);
        assertNotNull(
            "list(tags) must aggregate the SOURCE LIST column, not the expanded element",
            inputType.getFieldList().get(argIndex).getType().getComponentType()
        );
        assertEquals(List.of("tags", "values"), result.getRowType().getFieldNames());
        assertNull(result.getRowType().getFieldList().get(0).getType().getComponentType());
        assertNotNull("list() output is itself a LIST", result.getRowType().getFieldList().get(1).getType().getComponentType());
        assertEquals(1, RelNodeUtils.findNodes(result, org.opensearch.analytics.planner.rel.OpenSearchMultiValueExpand.class).size());
    }

    // ---- stats count() by tags, colors (two LIST keys → sequential expansion) ---------------

    public void testCountByTwoListKeys_2shard() {
        TableScan scan = listScan();
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(0, 1), count(scan));

        RelNode result = runPlanner(plan, listContext(2));

        List<OpenSearchAggregate> aggs = RelNodeUtils.findNodes(result, OpenSearchAggregate.class);
        assertEquals(2, aggs.size());
        for (OpenSearchAggregate agg : aggs) {
            assertEquals(2, agg.getGroupSet().cardinality());
            assertGroupKeyIsScalar(agg);
        }
        assertEquals(List.of("tags", "colors", "cnt"), result.getRowType().getFieldNames());
    }

    // ---- stats count() by status (scalar key) — expansion must be a strict no-op -------------

    public void testCountByScalarKey_2shard_noCorrelateIntroduced() {
        TableScan scan = listScan();
        RelNode plan = makeAggregate(scan, ImmutableBitSet.of(2), count(scan));

        RelNode result = runPlanner(plan, listContext(2));

        assertTrue(
            "scalar GROUP BY must not introduce any multi-value expansion",
            RelNodeUtils.findNodes(result, org.opensearch.analytics.planner.rel.OpenSearchMultiValueExpand.class).isEmpty()
        );
        List<OpenSearchAggregate> aggs = RelNodeUtils.findNodes(result, OpenSearchAggregate.class);
        assertEquals(2, aggs.size());
    }

    // ---- helpers -----------------------------------------------------------------------------

    /** PPL's {@code LIST} aggregate — resolved by name to {@code AggregateFunction.LIST} (STATE_EXPANDING). */
    private static org.apache.calcite.sql.SqlAggFunction listAggFunction() {
        return new org.apache.calcite.sql.SqlAggFunction(
            "LIST",
            null,
            org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
            org.apache.calcite.sql.type.ReturnTypes.ARG0,
            null,
            org.apache.calcite.sql.type.OperandTypes.ANY,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            org.apache.calcite.util.Optionality.FORBIDDEN
        ) {
        };
    }

    private static void assertGroupKeyIsScalar(OpenSearchAggregate agg) {
        RelDataType inputType = agg.getInput().getRowType();
        for (int key : agg.getGroupSet()) {
            assertNull(
                agg.getMode() + " aggregate must group on a scalar element, not the LIST (key " + key + ")",
                inputType.getFieldList().get(key).getType().getComponentType()
            );
        }
    }
}
