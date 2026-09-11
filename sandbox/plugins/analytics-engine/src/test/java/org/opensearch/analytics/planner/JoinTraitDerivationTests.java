/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link OpenSearchJoin}'s distribution algebra as the top-down planner sees it —
 * {@code deriveTraits} (bottom-up: what a hash-partitioned child lets this join deliver) and
 * {@code passThroughTraits} (top-down: what a SINGLETON demand costs its inputs). Pure functions over
 * {@link OpenSearchDistribution}, so they are tested in isolation with no planner run and no DAG.
 *
 * <p>{@code deriveTraits} answers BOTH halves of the old algebra at once: the pair's left is the join's
 * own output distribution, and the pair's right is the per-side demand on its inputs.
 */
public class JoinTraitDerivationTests extends BasePlannerRulesTests {

    private static final int N = 3; // partition count
    private static final List<String> DF = List.of("datafusion");

    private OpenSearchDistributionTraitDef traitDef;
    private RelOptCluster volcanoCluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // The base class's cluster is backed by HepPlanner, which ignores addRelTraitDef — a RelTraitSet
        // built from it silently DROPS the distribution, so every hook would read null and each assertion
        // would pass vacuously. These hooks read the distribution OUT of a trait set (their own and their
        // inputs'), so both the trait sets and the nodes need a real VolcanoPlanner with both defs
        // registered. Same pattern as TopDownTraitHookTests.
        PlannerContext ctx = buildContext("parquet", statusAndSizeFields());
        traitDef = ctx.getDistributionTraitDef();
        VolcanoPlanner volcano = new VolcanoPlanner();
        volcano.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcano.addRelTraitDef(traitDef);
        volcanoCluster = RelOptCluster.create(volcano, rexBuilder);
    }

    private RelTraitSet distTraits(OpenSearchDistribution dist) {
        return RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(dist);
    }

    private static Map<String, Map<String, Object>> statusAndSizeFields() {
        return Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer"));
    }

    public void testInnerEquiJoin_demandsHashOnEquiKeysPerSide() {
        OpenSearchJoin join = equiJoin(JoinRelType.INNER, /* leftKey */ 0, /* rightKeyOffset */ 0);
        Pair<RelTraitSet, List<RelTraitSet>> derived = join.deriveTraits(distTraits(traitDef.hash(List.of(0), N)), 0);
        assertNotNull("a left child hashed on the left equi key must yield a distributed alternative", derived);
        OpenSearchDistribution left = OpenSearchRelNode.distributionOf(derived.right.get(0));
        OpenSearchDistribution right = OpenSearchRelNode.distributionOf(derived.right.get(1));
        assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, left.getType());
        assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, right.getType());
        assertEquals("left hash key = left equi key", List.of(0), left.getKeys());
        assertEquals("right hash key = right equi key, in RIGHT input column space", List.of(0), right.getKeys());
        assertEquals("partition count threaded through", Integer.valueOf(N), left.getPartitionCount());
        assertEquals("both sides must agree on the partition count", Integer.valueOf(N), right.getPartitionCount());
    }

    public void testInnerEquiJoin_advertisesTheLeftKeyHashAsItsOutput() {
        OpenSearchJoin join = equiJoin(JoinRelType.INNER, 0, 0);
        Pair<RelTraitSet, List<RelTraitSet>> derived = join.deriveTraits(distTraits(traitDef.hash(List.of(0), N)), 0);
        OpenSearchDistribution out = OpenSearchRelNode.distributionOf(derived.left);
        assertEquals("INNER join output stays co-partitionable", RelDistribution.Type.HASH_DISTRIBUTED, out.getType());
        assertEquals("output anchored on the left equi keys", List.of(0), out.getKeys());
        assertEquals(Integer.valueOf(N), out.getPartitionCount());
    }

    /**
     * A RIGHT or FULL join emits null-extended rows for unmatched right rows. Those rows never passed
     * through the left-key hash, so their left-key columns are NULL and they sit in whichever partition
     * their RIGHT key landed in. Advertising {@code HASH(leftKeys)} would let a parent keyed on the same
     * column skip its exchange and silently miss matches.
     *
     * <p>Their INPUTS could still legally co-partition — it is only the OUTPUT that is undescribable — but
     * the derive path declines the alternative outright, because {@link OpenSearchDistribution} has no
     * "partitioned, specification unknown" value to report ({@code Type.ANY} means UNRESOLVED, and a parent
     * would treat it as satisfied-by-anything). Losing one alternative is the conservative trade; the split
     * rules can still build these shapes explicitly.
     */
    public void testRightAndFullJoins_deriveNoAlternativeAtAll() {
        for (JoinRelType type : List.of(JoinRelType.RIGHT, JoinRelType.FULL)) {
            OpenSearchJoin join = equiJoin(type, 0, 0);
            assertNull(
                type + " must not derive a hash shape — its output cannot be described as HASH(leftKeys)",
                join.deriveTraits(distTraits(traitDef.hash(List.of(0), N)), 0)
            );
        }
    }

    /** The join types whose output genuinely IS hash-partitioned on the left keys still derive one. */
    public void testInnerSemiAndAntiJoins_doAdvertiseTheHash() {
        for (JoinRelType type : List.of(JoinRelType.INNER, JoinRelType.SEMI, JoinRelType.ANTI)) {
            OpenSearchJoin join = equiJoin(type, 0, 0);
            Pair<RelTraitSet, List<RelTraitSet>> derived = join.deriveTraits(distTraits(traitDef.hash(List.of(0), N)), 0);
            assertNotNull(type + " must derive a co-partitioned alternative", derived);
            assertEquals(
                type + " advertises HASH(leftKeys)",
                RelDistribution.Type.HASH_DISTRIBUTED,
                OpenSearchRelNode.distributionOf(derived.left).getType()
            );
        }
    }

    public void testLeftJoin_stillCoPartitionsOnEquiKeys() {
        OpenSearchJoin join = equiJoin(JoinRelType.LEFT, 0, 0);
        Pair<RelTraitSet, List<RelTraitSet>> derived = join.deriveTraits(distTraits(traitDef.hash(List.of(0), N)), 0);
        assertNotNull("LEFT outer join must co-partition on equi keys", derived);
        assertEquals(RelDistribution.Type.HASH_DISTRIBUTED, OpenSearchRelNode.distributionOf(derived.right.get(0)).getType());
    }

    public void testPureThetaJoin_hasNoDistributedAlternative() {
        OpenSearchJoin join = thetaJoin(JoinRelType.INNER);
        assertNull(
            "a pure theta join has no key to partition on, so it stays coordinator-gathered",
            join.deriveTraits(distTraits(traitDef.hash(List.of(0), N)), 0)
        );
    }

    public void testKeyMismatch_yieldsNoAlternative() {
        OpenSearchJoin join = equiJoin(JoinRelType.INNER, 0, 0);
        // The child is hashed on a DIFFERENT column than the join's left equi key. Shuffling on the wrong
        // column is type-correct but silently produces wrong results, so declining is mandatory.
        assertNull("key mismatch must not derive a co-partitioned shape", join.deriveTraits(distTraits(traitDef.hash(List.of(1), N)), 0));
    }

    public void testSingletonDemand_gathersBothInputs() {
        OpenSearchJoin join = equiJoin(JoinRelType.INNER, 0, 0);
        Pair<RelTraitSet, List<RelTraitSet>> passed = join.passThroughTraits(distTraits(traitDef.coordSingleton()));
        assertNotNull("a SINGLETON demand is satisfiable by gathering both sides", passed);
        for (int side = 0; side < 2; side++) {
            OpenSearchDistribution demand = OpenSearchRelNode.distributionOf(passed.right.get(side));
            assertEquals("input " + side + " gathered", RelDistribution.Type.SINGLETON, demand.getType());
            assertEquals(OpenSearchDistribution.Locality.COORDINATOR, demand.getLocality());
        }
    }

    public void testHashDemand_isDeclinedTopDown() {
        OpenSearchJoin join = equiJoin(JoinRelType.INNER, 0, 0);
        assertNull(
            "a HASH demand is answered by deriveTraits from what a child can deliver, not passed down",
            join.passThroughTraits(distTraits(traitDef.hash(List.of(0), N)))
        );
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private OpenSearchJoin equiJoin(JoinRelType type, int leftKey, int rightKeyOffset) {
        return buildJoin(type, /* equi */ true, leftKey, rightKeyOffset);
    }

    private OpenSearchJoin thetaJoin(JoinRelType type) {
        return buildJoin(type, /* equi */ false, 0, 0);
    }

    private OpenSearchJoin buildJoin(JoinRelType type, boolean equi, int leftKey, int rightKeyOffset) {
        RelNode left = shardScan("a_idx");
        RelNode right = shardScan("b_idx");
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
        return new OpenSearchJoin(volcanoCluster, distTraits(traitDef.coordSingleton()), left, right, condition, type, DF);
    }

    private OpenSearchTableScan shardScan(String table) {
        return new OpenSearchTableScan(
            volcanoCluster,
            distTraits(traitDef.shardRandom(1, N)),
            mockTable(table, "status", "size"),
            DF,
            List.of()
        );
    }
}
