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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins {@link LuceneFragmentConvertor#isCountFastPath}: drivable iff top is an Aggregate
 * with empty group-set and every call is {@code SqlKind.COUNT}. Read by
 * {@link LuceneShardPreference} to score this fragment for the count-fast-path. Guards
 * capability-declaration drift — PlanForker already narrows by declared caps, this is the
 * second line.
 */
public class LuceneCanDriveFragmentTests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), new RexBuilder(typeFactory));
    }

    public void testCountStarOverEmptyGroupSet_drivable() {
        TableScan scan = stubScan("status", SqlTypeName.VARCHAR);
        RelNode agg = aggregate(scan, ImmutableBitSet.of(), countStar(scan));
        assertTrue("COUNT(*) with empty group-set is the canonical Lucene-driver shape", LuceneFragmentConvertor.isCountFastPath(agg));
    }

    public void testCountFieldOverEmptyGroupSet_drivable() {
        // count(field) — same SqlKind.COUNT, just with a field arg.
        TableScan scan = stubScan("status", SqlTypeName.VARCHAR);
        AggregateCall countField = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(0),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt_status"
        );
        assertTrue(
            "count(field) is also drivable — same SqlKind.COUNT",
            LuceneFragmentConvertor.isCountFastPath(aggregate(scan, ImmutableBitSet.of(), countField))
        );
    }

    public void testSumOverEmptyGroupSet_notDrivable() {
        TableScan scan = stubScan("size", SqlTypeName.INTEGER);
        assertFalse(
            "SUM needs column values Lucene can't materialise — must be rejected",
            LuceneFragmentConvertor.isCountFastPath(
                aggregate(scan, ImmutableBitSet.of(), nullableNumeric(SqlStdOperatorTable.SUM, scan, "total_size"))
            )
        );
    }

    public void testMinOverEmptyGroupSet_notDrivable() {
        TableScan scan = stubScan("size", SqlTypeName.INTEGER);
        assertFalse(
            "MIN must be rejected",
            LuceneFragmentConvertor.isCountFastPath(
                aggregate(scan, ImmutableBitSet.of(), nullableNumeric(SqlStdOperatorTable.MIN, scan, "min_size"))
            )
        );
    }

    public void testMaxOverEmptyGroupSet_notDrivable() {
        TableScan scan = stubScan("size", SqlTypeName.INTEGER);
        assertFalse(
            "MAX must be rejected",
            LuceneFragmentConvertor.isCountFastPath(
                aggregate(scan, ImmutableBitSet.of(), nullableNumeric(SqlStdOperatorTable.MAX, scan, "max_size"))
            )
        );
    }

    public void testCountPlusSum_mixedAggregate_notDrivable() {
        // Even one non-COUNT call disqualifies the whole aggregate — every call must be COUNT.
        TableScan scan = stubScan("size", SqlTypeName.INTEGER);
        RelNode agg = aggregate(scan, ImmutableBitSet.of(), countStar(scan), nullableNumeric(SqlStdOperatorTable.SUM, scan, "total_size"));
        assertFalse("COUNT(*) + SUM mixed must be rejected", LuceneFragmentConvertor.isCountFastPath(agg));
    }

    public void testCountWithGroupBy_notDrivable() {
        // group-by COUNT — Lucene has no per-bucket count primitive.
        TableScan scan = stubScan("status", SqlTypeName.VARCHAR);
        assertFalse(
            "COUNT(*) GROUP BY status must be rejected — Lucene has no per-group count",
            LuceneFragmentConvertor.isCountFastPath(aggregate(scan, ImmutableBitSet.of(0), countStar(scan)))
        );
    }

    public void testNonAggregateTop_notDrivable() {
        TableScan scan = stubScan("status", SqlTypeName.VARCHAR);
        RelNode project = LogicalProject.create(
            scan,
            List.of(),
            List.of(new RexBuilder(typeFactory).makeInputRef(scan, 0)),
            List.of("status")
        );
        assertFalse("Project (no aggregate above) must be rejected", LuceneFragmentConvertor.isCountFastPath(project));
        assertFalse("Bare TableScan must be rejected", LuceneFragmentConvertor.isCountFastPath(scan));
    }

    // ---- Helpers ----

    private TableScan stubScan(String fieldName, SqlTypeName type) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add(fieldName, typeFactory.createSqlType(type));
        RelDataType rowType = builder.build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("test_index"));
        when(table.getRowType()).thenReturn(rowType);
        return new TableScan(cluster, cluster.traitSet(), List.of(), table) {
        };
    }

    private RelNode aggregate(RelNode input, ImmutableBitSet groupSet, AggregateCall... calls) {
        return LogicalAggregate.create(input, List.of(), groupSet, null, List.of(calls));
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

    /**
     * SUM/MIN/MAX over an INTEGER column with empty group-set are nullable in Calcite's
     * inferred type. Use the long-form create with null returnType so Calcite infers —
     * short-form would trip {@code typeMatchesInferred}.
     */
    private AggregateCall nullableNumeric(SqlAggFunction fn, RelNode input, String alias) {
        return AggregateCall.create(fn, false, false, false, List.of(), List.of(0), -1, null, RelCollations.EMPTY, 0, input, null, alias);
    }
}
