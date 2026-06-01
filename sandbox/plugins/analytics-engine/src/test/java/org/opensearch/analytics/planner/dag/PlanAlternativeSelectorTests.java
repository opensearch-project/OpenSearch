/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;

import java.util.List;
import java.util.Map;

/**
 * Direct-call tests for {@link PlanAlternativeSelector#canMetadataDriverExecute(RelNode)} —
 * the defense-in-depth predicate that determines whether the metadata-only driver (Lucene
 * today) is allowed to drive a stage's resolved fragment end-to-end.
 *
 * <p>The selector is the second line of defense. The first is {@code PlanForker}'s
 * chain-agreement filter, which already narrows aggregate alternatives to backends that
 * declare the aggregate function as a capability — and prod Lucene declares only COUNT.
 * These tests pin the predicate's behaviour so a future capability-declaration drift can't
 * silently route a SUM/MIN/MAX/grouped fragment through the Lucene-driver path.
 *
 * <p>For end-to-end coverage that exercises the prod Lucene plugin's actual capability
 * surface through PlanForker → BackendPlanAdapter → selector, see
 * {@code org.opensearch.be.lucene.PlanAlternativeSelectorTests} in the
 * {@code analytics-backend-lucene} module.
 */
public class PlanAlternativeSelectorTests extends BasePlannerRulesTests {

    public void testCountStarOverEmptyGroupSet_drivable() {
        OpenSearchAggregate agg = countAggregate();
        assertTrue(
            "COUNT(*) with empty group-set is the canonical Lucene-driver shape",
            PlanAlternativeSelector.canMetadataDriverExecute(agg)
        );
    }

    public void testCountFieldOverEmptyGroupSet_drivable() {
        // count(field) — same SqlKind.COUNT, just with a field arg. Lucene's count() returns
        // the matching-doc count regardless; non-null filtering is an upstream concern.
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall countField = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(0),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt_status"
        );
        OpenSearchAggregate agg = aggregateOver(scan, ImmutableBitSet.of(), List.of(countField));
        assertTrue("count(field) is also drivable — same SqlKind.COUNT", PlanAlternativeSelector.canMetadataDriverExecute(agg));
    }

    public void testSumOverEmptyGroupSet_notDrivable() {
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        OpenSearchAggregate agg = aggregateOver(
            scan,
            ImmutableBitSet.of(),
            List.of(numericCall(SqlStdOperatorTable.SUM, scan, "total_size"))
        );
        assertFalse(
            "SUM needs column values Lucene can't materialise — must be rejected",
            PlanAlternativeSelector.canMetadataDriverExecute(agg)
        );
    }

    public void testMinOverEmptyGroupSet_notDrivable() {
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        OpenSearchAggregate agg = aggregateOver(
            scan,
            ImmutableBitSet.of(),
            List.of(numericCall(SqlStdOperatorTable.MIN, scan, "min_size"))
        );
        assertFalse("MIN must be rejected", PlanAlternativeSelector.canMetadataDriverExecute(agg));
    }

    public void testMaxOverEmptyGroupSet_notDrivable() {
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        OpenSearchAggregate agg = aggregateOver(
            scan,
            ImmutableBitSet.of(),
            List.of(numericCall(SqlStdOperatorTable.MAX, scan, "max_size"))
        );
        assertFalse("MAX must be rejected", PlanAlternativeSelector.canMetadataDriverExecute(agg));
    }

    public void testCountPlusSum_mixedAggregate_notDrivable() {
        // Even one non-COUNT call in the list disqualifies the whole aggregate — the
        // gate is "every call is COUNT", not "any call is COUNT".
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        OpenSearchAggregate agg = aggregateOver(
            scan,
            ImmutableBitSet.of(),
            List.of(countStarCall(scan), numericCall(SqlStdOperatorTable.SUM, scan, "total_size"))
        );
        assertFalse(
            "COUNT(*) + SUM in the same aggregate must be rejected — Lucene can't produce SUM",
            PlanAlternativeSelector.canMetadataDriverExecute(agg)
        );
    }

    public void testCountWithGroupBy_notDrivable() {
        // group-by COUNT — Lucene has no per-bucket count primitive. Reject regardless of
        // the agg-call shape.
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        OpenSearchAggregate agg = aggregateOver(scan, ImmutableBitSet.of(0), List.of(countStarCall(scan)));
        assertFalse(
            "COUNT(*) GROUP BY status must be rejected — Lucene has no per-group count",
            PlanAlternativeSelector.canMetadataDriverExecute(agg)
        );
    }

    public void testNonAggregateTop_notDrivable() {
        // A bare project (or any non-aggregate top) is never drivable as a metadata-only
        // count — Lucene's only emit shape is a single count value.
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        LogicalProject project = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0)), List.of("status"));
        assertFalse("Project (no aggregate above) must be rejected", PlanAlternativeSelector.canMetadataDriverExecute(project));
        assertFalse("Bare TableScan must be rejected", PlanAlternativeSelector.canMetadataDriverExecute(scan));
    }

    public void testLogicalAggregateNotMarked_notDrivable() {
        // The selector inspects OpenSearchAggregate specifically. A vanilla LogicalAggregate
        // (pre-marking, or one that escaped the marker) is not the contract we drive on —
        // reject it so a stray logical agg can't slip through.
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        LogicalAggregate logicalAgg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(countStarCall(scan)));
        assertFalse(
            "Bare LogicalAggregate (not OpenSearchAggregate) must be rejected",
            PlanAlternativeSelector.canMetadataDriverExecute(logicalAgg)
        );
    }

    // ---- Helpers ----

    private OpenSearchAggregate countAggregate() {
        TableScan scan = stubScan(mockTable("test_index", "status", "size"));
        return aggregateOver(scan, ImmutableBitSet.of(), List.of(countStarCall(scan)));
    }

    /**
     * SUM/MIN/MAX over an INTEGER column with an empty group-set are nullable in Calcite's
     * inferred type even when the input column is NOT NULL. Use the long-form create with a
     * null returnType so Calcite infers — short-form would trip {@code typeMatchesInferred}.
     */
    private AggregateCall numericCall(SqlAggFunction fn, RelNode input, String alias) {
        return AggregateCall.create(fn, false, false, false, List.of(), List.of(1), -1, null, RelCollations.EMPTY, 0, input, null, alias);
    }

    private OpenSearchAggregate aggregateOver(RelNode input, ImmutableBitSet groupSet, List<AggregateCall> calls) {
        return new OpenSearchAggregate(
            cluster,
            cluster.traitSet(),
            input,
            groupSet,
            null,
            calls,
            AggregateMode.SINGLE,
            List.of("lucene", "mock-parquet"),
            Map.of()
        );
    }
}
