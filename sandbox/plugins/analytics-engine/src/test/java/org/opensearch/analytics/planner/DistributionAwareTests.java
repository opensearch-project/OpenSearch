/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.DistributionAware;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;

import java.util.List;

/**
 * Unit tests for the {@link DistributionAware} algebra. Each operator's
 * {@code requiredInputDistribution} / {@code deriveOutputDistribution} is a pure function over
 * {@link OpenSearchDistribution}, so it is tested in isolation — no planner run, no DAG. These are the
 * building blocks the post-CBO enforcement pass composes; getting them right per-operator is the
 * keystone of shape-complete scheduling.
 */
public class DistributionAwareTests extends BasePlannerRulesTests {

    private static final int N = 3; // partition count
    private static final List<String> DF = List.of("datafusion");

    private OpenSearchDistributionTraitDef traitDef;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        PlannerContext ctx = buildContext("parquet", Map_of_intFields());
        traitDef = ctx.getDistributionTraitDef();
        // Register the distribution trait def on the test cluster's planner so traitSet().replace(dist)
        // retains the trait (RelTraitSet.replace silently drops a trait whose def isn't registered).
        cluster.getPlanner().addRelTraitDef(traitDef);
    }

    /** A trait set carrying {@code dist} — built from a set that has the dist def registered. */
    private org.apache.calcite.plan.RelTraitSet distTraits(OpenSearchDistribution dist) {
        return cluster.traitSet().replace(dist);
    }

    // intFields() helper from the base declares status,size — reuse via a small map builder.
    private static java.util.Map<String, java.util.Map<String, Object>> Map_of_intFields() {
        return java.util.Map.of("status", java.util.Map.of("type", "integer"), "size", java.util.Map.of("type", "integer"));
    }

    // ── OpenSearchJoin ──────────────────────────────────────────────────────────

    public void testInnerEquiJoin_requiresHashOnEquiKeysPerSide() {
        OpenSearchJoin join = equiJoin(JoinRelType.INNER, /* leftKey */ 0, /* rightKeyOffset */ 0);
        OpenSearchDistribution left = join.requiredInputDistribution(0, N, traitDef);
        OpenSearchDistribution right = join.requiredInputDistribution(1, N, traitDef);
        assertNotNull("left input must be required HASH", left);
        assertNotNull("right input must be required HASH", right);
        assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, left.getType());
        assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, right.getType());
        assertEquals("left hash key = left equi key", List.of(0), left.getKeys());
        assertEquals("partition count threaded through", Integer.valueOf(N), left.getPartitionCount());
    }

    public void testPureThetaJoin_imposesNoRequirement() {
        OpenSearchJoin join = thetaJoin(JoinRelType.INNER);
        assertNull(
            "pure theta join must impose no input requirement (stays coord-gathered)",
            join.requiredInputDistribution(0, N, traitDef)
        );
        assertNull(join.requiredInputDistribution(1, N, traitDef));
    }

    public void testOuterJoin_stillCoPartitionsOnEquiKeys() {
        // The key generalization vs the current cascade (INNER-only): outer joins co-partition too.
        for (JoinRelType type : List.of(JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL)) {
            OpenSearchJoin join = equiJoin(type, 0, 0);
            OpenSearchDistribution left = join.requiredInputDistribution(0, N, traitDef);
            assertNotNull(type + " outer join must co-partition on equi keys", left);
            assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, left.getType());
        }
    }

    public void testJoin_derivesHashOutputWhenLeftHashedOnEquiKeys() {
        OpenSearchJoin join = equiJoin(JoinRelType.INNER, 0, 0);
        OpenSearchDistribution leftActual = traitDef.hash(List.of(0), N);
        OpenSearchDistribution rightActual = traitDef.hash(List.of(0), N);
        OpenSearchDistribution out = join.deriveOutputDistribution(List.of(leftActual, rightActual), traitDef);
        assertNotNull("join output co-partitionable when left is hashed on left equi keys", out);
        assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, out.getType());
        assertEquals("output anchored on left equi keys", List.of(0), out.getKeys());
        assertEquals(Integer.valueOf(N), out.getPartitionCount());
    }

    public void testJoin_noDerivedOutputWhenLeftKeyMismatch() {
        OpenSearchJoin join = equiJoin(JoinRelType.INNER, 0, 0);
        // Left actually hashed on a DIFFERENT key than the join's left equi key → output not co-partitionable.
        OpenSearchDistribution leftActual = traitDef.hash(List.of(1), N);
        OpenSearchDistribution rightActual = traitDef.hash(List.of(0), N);
        assertNull(
            "key mismatch → no co-partitioned output (pass enforces a re-shuffle)",
            join.deriveOutputDistribution(List.of(leftActual, rightActual), traitDef)
        );
    }

    // ── OpenSearchAggregate ─────────────────────────────────────────────────────

    public void testGroupedAggregate_requiresHashOnGroupKeys() {
        OpenSearchAggregate agg = sumByStatus(); // group by col 0
        OpenSearchDistribution req = agg.requiredInputDistribution(0, N, traitDef);
        assertNotNull("grouped SINGLE agg requires hash on group keys", req);
        assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, req.getType());
        assertEquals(List.of(0), req.getKeys());
    }

    public void testEmptyGroupAggregate_requiresSingleton() {
        OpenSearchAggregate agg = sumNoGroup();
        OpenSearchDistribution req = agg.requiredInputDistribution(0, N, traitDef);
        assertNotNull(req);
        assertEquals(
            "empty-group agg requires SINGLETON (PARTIAL distributes, FINAL gathers ≤N partials)",
            RelDistribution.Type.SINGLETON,
            req.getType()
        );
    }

    // ── OpenSearchFilter (transparent) ──────────────────────────────────────────

    public void testFilter_isTransparent() {
        OpenSearchFilter filter = filterOverScan();
        assertNull("filter imposes no input requirement", filter.requiredInputDistribution(0, N, traitDef));
        OpenSearchDistribution childHash = traitDef.hash(List.of(0), N);
        assertEquals(
            "filter passes child distribution through verbatim",
            childHash,
            filter.deriveOutputDistribution(List.of(childHash), traitDef)
        );
    }

    // ── OpenSearchProject (transparent + remap) ─────────────────────────────────

    public void testPlainProject_imposesNoRequirement() {
        OpenSearchProject project = identityProjectOverScan();
        assertNull("plain row-wise project imposes no input requirement", project.requiredInputDistribution(0, N, traitDef));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private OpenSearchJoin equiJoin(JoinRelType type, int leftKey, int rightKeyOffset) {
        return buildJoin(type, /* equi */ true, leftKey, rightKeyOffset);
    }

    private OpenSearchJoin thetaJoin(JoinRelType type) {
        return buildJoin(type, /* equi */ false, 0, 0);
    }

    private OpenSearchJoin buildJoin(JoinRelType type, boolean equi, int leftKey, int rightKeyOffset) {
        RelNode left = stubScan(mockTable("a_idx", "status", "size"));
        RelNode right = stubScan(mockTable("b_idx", "status", "size"));
        int leftCols = left.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = equi
            ? rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(intType, leftKey),
                rexBuilder.makeInputRef(intType, leftCols + rightKeyOffset)
            )
            : rexBuilder.makeCall(
                SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(intType, 0),
                rexBuilder.makeInputRef(intType, leftCols)
            );
        return new OpenSearchJoin(cluster, distTraits(traitDef.coordSingleton()), left, right, condition, type, DF);
    }

    private OpenSearchAggregate sumByStatus() {
        TableScan scan = stubScan(mockTable("a_idx", "status", "size"));
        return new OpenSearchAggregate(
            cluster,
            distTraits(traitDef.coordSingleton()),
            scan,
            ImmutableBitSet.of(0),
            null,
            List.of(sumCall(scan)),
            AggregateMode.SINGLE,
            DF,
            java.util.Map.of()
        );
    }

    private OpenSearchAggregate sumNoGroup() {
        TableScan scan = stubScan(mockTable("a_idx", "status", "size"));
        // Empty-group SUM infers a NULLABLE result (no rows → null), unlike the grouped case — declare it
        // explicitly so Calcite's Aggregate ctor type-check passes.
        org.apache.calcite.rel.core.AggregateCall sumCall = org.apache.calcite.rel.core.AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "total"
        );
        return new OpenSearchAggregate(
            cluster,
            distTraits(traitDef.coordSingleton()),
            scan,
            ImmutableBitSet.of(),
            null,
            List.of(sumCall),
            AggregateMode.SINGLE,
            DF,
            java.util.Map.of()
        );
    }

    private OpenSearchFilter filterOverScan() {
        TableScan scan = stubScan(mockTable("a_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeLiteral(5, intType, false)
        );
        return new OpenSearchFilter(cluster, distTraits(traitDef.coordSingleton()), scan, cond, DF);
    }

    private OpenSearchProject identityProjectOverScan() {
        TableScan scan = stubScan(mockTable("a_idx", "status", "size"));
        List<RexNode> projects = List.of(rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1));
        return new OpenSearchProject(cluster, distTraits(traitDef.coordSingleton()), scan, projects, scan.getRowType(), DF);
    }
}
