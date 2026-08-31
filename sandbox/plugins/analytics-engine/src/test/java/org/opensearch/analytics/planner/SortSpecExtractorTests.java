/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.exec.canmatch.SortSpec;
import org.opensearch.analytics.exec.canmatch.SortSpecExtractor;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;

import java.util.List;

/**
 * Tests {@link SortSpecExtractor} against post-CBO, post-DAG-cut shard fragments — the real input it
 * sees at runtime — rather than hand-assembled Sort nodes. Confirms the pushed-down collated Sort
 * lands inside the shard fragment where extraction finds it, so no new planner plumbing is needed.
 */
public class SortSpecExtractorTests extends PlanShapeTestBase {

    // ---- shapes that yield a spec ----

    public void testAscendingSortWithLimit() {
        SortSpec spec = shardSortSpec(sortLimit(0, RelFieldCollation.Direction.ASCENDING, null, 10));

        assertNotNull("a pushed-down collated Sort with a fetch must yield a spec", spec);
        assertEquals("status", spec.column());
        assertFalse(spec.descending());
        assertEquals("no offset → limit is the fetch", 10, spec.limit());
    }

    public void testDescendingSortWithLimit() {
        SortSpec spec = shardSortSpec(sortLimit(0, RelFieldCollation.Direction.DESCENDING, null, 5));

        assertNotNull(spec);
        assertTrue("DESC must be reported so the coordinator orders by max", spec.descending());
    }

    /** The sort column need not be field 0 — the name must come from the collation's index. */
    public void testResolvesColumnNameFromCollationIndex() {
        SortSpec spec = shardSortSpec(sortLimit(1, RelFieldCollation.Direction.ASCENDING, null, 3));

        assertNotNull(spec);
        assertEquals("second collation index must resolve to the second field", "size", spec.column());
    }

    /** A Sort carrying an offset still yields a spec — offset must not defeat extraction. */
    public void testSortWithOffsetStillYieldsSpec() {
        SortSpec spec = shardSortSpec(sortLimit(0, RelFieldCollation.Direction.ASCENDING, 20, 10));

        assertNotNull(spec);
        assertEquals("status", spec.column());
    }

    /** The limit is offset + fetch: it must count the offset rows the coordinator collects then discards. */
    public void testLimitIncludesOffset() {
        SortSpec spec = shardSortSpec(sortLimit(0, RelFieldCollation.Direction.ASCENDING, 20, 10));

        assertNotNull(spec);
        assertEquals("limit must be offset + fetch, not fetch", 30, spec.limit());
    }

    // ---- shapes that must NOT yield a spec ----

    /** A bare {@code head N} has no ordering, so there is no key to order shards by. */
    public void testBareLimitWithoutCollationYieldsNoSpec() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode sort = LogicalSort.create(scan, RelCollations.EMPTY, null, intLiteral(10));

        assertNull(shardSortSpec(sort));
    }

    /** An unbounded {@code ORDER BY} has no top-N to reason about. */
    public void testSortWithoutFetchYieldsNoSpec() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode sort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );

        assertNull(shardSortSpec(sort));
    }

    public void testFragmentWithoutSortYieldsNoSpec() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));

        assertNull(SortSpecExtractor.extract(scan));
    }

    public void testNullFragmentYieldsNoSpec() {
        assertNull(SortSpecExtractor.extract(null));
    }

    // ---- helpers ----

    private RexNode intLiteral(int value) {
        return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    }

    private RelNode sortLimit(int fieldIndex, RelFieldCollation.Direction direction, Integer offset, int fetch) {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        return LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(fieldIndex, direction)),
            offset == null ? null : intLiteral(offset),
            intLiteral(fetch)
        );
    }

    /** Runs the planner + DAG cut and returns the leaf (shard) stage's extracted spec. */
    private SortSpec shardSortSpec(RelNode logicalPlan) {
        PlannerContext context = multiShardContext();
        RelNode planned = runPlanner(logicalPlan, context);
        QueryDAG dag = DAGBuilder.build(planned, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        return leafStage(dag.rootStage()).getSortSpec();
    }

    /** Walks to the bottom-most stage — the shard fragment. */
    private static Stage leafStage(Stage stage) {
        List<Stage> children = stage.getChildStages();
        return children.isEmpty() ? stage : leafStage(children.get(0));
    }
}
