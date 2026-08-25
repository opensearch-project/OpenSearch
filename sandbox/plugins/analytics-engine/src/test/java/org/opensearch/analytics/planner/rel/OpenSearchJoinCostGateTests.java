/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Cost-gate matrix for {@link OpenSearchJoin#computeSelfCost}. Each test constructs a join
 * with explicit (selfTrait, leftInputTrait, rightInputTrait) traits and asserts the cost
 * gate either accepts (returns a finite cost) or rejects (returns infinite). The gate is
 * the load-bearing constraint that prevents Volcano from producing nonsensical plans like
 * "HASH-localized join with one SINGLETON input."
 *
 * <p>The three legal join shapes:
 * <ol>
 *   <li>SINGLETON: COORDINATOR+SINGLETON or SHARD+SINGLETON, inputs match self.</li>
 *   <li>HASH+WORKER (hash-shuffle): both inputs HASH+WORKER with same partition count.</li>
 *   <li>RANDOM+SHARD (broadcast): one BROADCAST+REPLICATED build, one SHARD probe matching
 *       the join's tableId.</li>
 * </ol>
 */
public class OpenSearchJoinCostGateTests extends BasePlannerRulesTests {

    private VolcanoPlanner volcano;
    private RelOptCluster volcanoCluster;
    private OpenSearchDistributionTraitDef traitDef;
    private RelOptTable testTable;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        PlannerContext context = buildContext("parquet", /* shardCount */ 3, intFields());
        traitDef = context.getDistributionTraitDef();
        volcano = new VolcanoPlanner();
        volcano.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcano.addRelTraitDef(traitDef);
        volcanoCluster = RelOptCluster.create(volcano, new RexBuilder(typeFactory));
        testTable = mockTable("test_index", "status", "size");
    }

    // ── SINGLETON shape ───────────────────────────────────────────────────

    public void testCoordSingletonAcceptsCoordSingletonInputs() {
        RelNode left = scanWith(traitDef.coordSingleton());
        RelNode right = scanWith(traitDef.coordSingleton());
        OpenSearchJoin join = makeJoin(left, right, traitDef.coordSingleton());
        assertNotInfinite("COORD+SINGLETON join with COORD+SINGLETON inputs must be accepted", costOf(join));
    }

    public void testCoordSingletonRejectsShardSingletonInputs() {
        // Locality mismatch: SHARD-localized inputs into a COORD-localized join.
        int tableId = testTable.getQualifiedName().hashCode();
        RelNode left = scanWith(traitDef.shardSingleton(tableId, 1));
        RelNode right = scanWith(traitDef.shardSingleton(tableId, 1));
        OpenSearchJoin join = makeJoin(left, right, traitDef.coordSingleton());
        assertInfinite("COORD+SINGLETON join with SHARD inputs must be rejected", costOf(join));
    }

    // ── HASH shape ────────────────────────────────────────────────────────

    public void testHashWorkerAcceptsMatchingHashInputs() {
        RelNode left = scanWith(traitDef.hash(List.of(0), 4));
        RelNode right = scanWith(traitDef.hash(List.of(2), 4));
        OpenSearchJoin join = makeJoin(left, right, traitDef.hash(List.of(0), 4));
        assertNotInfinite("HASH+WORKER join with matching-N HASH inputs must be accepted", costOf(join));
    }

    public void testHashWorkerRejectsMismatchedPartitionCounts() {
        // Different partition counts on the two sides — the actual rows can't co-locate.
        RelNode left = scanWith(traitDef.hash(List.of(0), 4));
        RelNode right = scanWith(traitDef.hash(List.of(2), 8));
        OpenSearchJoin join = makeJoin(left, right, traitDef.hash(List.of(0), 4));
        assertInfinite("HASH+WORKER join with mismatched partition counts must be rejected", costOf(join));
    }

    public void testHashWorkerRejectsSingletonInput() {
        // Type mismatch: SINGLETON input into HASH-localized join.
        RelNode left = scanWith(traitDef.coordSingleton());
        RelNode right = scanWith(traitDef.hash(List.of(2), 4));
        OpenSearchJoin join = makeJoin(left, right, traitDef.hash(List.of(0), 4));
        assertInfinite("HASH+WORKER join with mixed SINGLETON/HASH inputs must be rejected", costOf(join));
    }

    // ── BROADCAST shape ───────────────────────────────────────────────────

    public void testBroadcastShapeAcceptsBroadcastBuildPlusShardProbe() {
        // Probe-side trait drives the join's own trait (tableId/shardCount carry over).
        int tableId = testTable.getQualifiedName().hashCode();
        OpenSearchDistribution probeTrait = traitDef.shardRandom(tableId, /* shardCount */ 3);
        RelNode build = scanWith(traitDef.broadcast(/* probeNodes */ 3));
        RelNode probe = scanWith(probeTrait);
        OpenSearchJoin join = makeJoin(build, probe, traitDef.from(probeTrait));
        assertNotInfinite("Broadcast shape (build=REPLICATED, probe=SHARD+RANDOM) must be accepted", costOf(join));
    }

    public void testBroadcastShapeAcceptsLeftAsProbeRightAsBuild() {
        // Build can be on either side; the gate doesn't care which.
        int tableId = testTable.getQualifiedName().hashCode();
        OpenSearchDistribution probeTrait = traitDef.shardRandom(tableId, /* shardCount */ 3);
        RelNode probe = scanWith(probeTrait);
        RelNode build = scanWith(traitDef.broadcast(/* probeNodes */ 3));
        OpenSearchJoin join = makeJoin(probe, build, traitDef.from(probeTrait));
        assertNotInfinite("Broadcast shape with left=probe, right=build must be accepted", costOf(join));
    }

    public void testBroadcastShapeRejectsTwoBroadcastInputs() {
        // Both inputs broadcast → no probe → no localized execution location for the join.
        RelNode b1 = scanWith(traitDef.broadcast(3));
        RelNode b2 = scanWith(traitDef.broadcast(3));
        // Need a self trait that's broadcast-shape (RANDOM+SHARD); use any tableId.
        int tableId = testTable.getQualifiedName().hashCode();
        OpenSearchJoin join = makeJoin(b1, b2, traitDef.shardRandom(tableId, 3));
        assertInfinite("Broadcast shape with two REPLICATED inputs must be rejected (no probe)", costOf(join));
    }

    public void testBroadcastShapeRejectsTwoShardProbeInputs() {
        // Both shards, no broadcast — that's the regular SHARD+SINGLETON co-location, but
        // RANDOM doesn't co-locate by key. The gate doesn't recognize this combo as broadcast.
        int tableId = testTable.getQualifiedName().hashCode();
        OpenSearchDistribution probeTrait = traitDef.shardRandom(tableId, 3);
        RelNode left = scanWith(probeTrait);
        RelNode right = scanWith(probeTrait);
        OpenSearchJoin join = makeJoin(left, right, traitDef.from(probeTrait));
        assertInfinite("Broadcast shape requires exactly one REPLICATED build", costOf(join));
    }

    public void testBroadcastShapeRejectsWrongTableIdProbe() {
        // Probe's tableId doesn't match the join's self tableId → the probe is from a
        // different index, can't be the join's local probe.
        int joinTableId = testTable.getQualifiedName().hashCode();
        int wrongTableId = joinTableId + 1;
        OpenSearchDistribution wrongProbe = traitDef.shardRandom(wrongTableId, 3);
        RelNode build = scanWith(traitDef.broadcast(3));
        RelNode probe = scanWith(wrongProbe);
        OpenSearchJoin join = makeJoin(build, probe, traitDef.shardRandom(joinTableId, 3));
        assertInfinite("Broadcast shape with mismatched probe tableId must be rejected", costOf(join));
    }

    // ── helpers ───────────────────────────────────────────────────────────

    /** Build an OpenSearchTableScan stub carrying the requested distribution trait. We don't
     *  go through the full table-scan rule pipeline — that triggers planner registration and
     *  changes the trait we care about. Direct construction with a custom trait set lets each
     *  cost-gate case isolate the trait combination under test. */
    private OpenSearchTableScan scanWith(OpenSearchDistribution dist) {
        RelTraitSet traits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(dist);
        return new OpenSearchTableScan(volcanoCluster, traits, testTable, List.of("mock-parquet"), List.<FieldStorageInfo>of());
    }

    /** Build an OpenSearchJoin over two inputs at the requested self trait. */
    private OpenSearchJoin makeJoin(RelNode left, RelNode right, OpenSearchDistribution selfTrait) {
        RelTraitSet joinTraits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(selfTrait);
        // Equi condition on the first column of each side. The cost gate ignores the
        // condition; it operates on traits only.
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), left.getRowType().getFieldCount())
        );
        return new OpenSearchJoin(volcanoCluster, joinTraits, left, right, condition, JoinRelType.INNER, List.of("mock-parquet"));
    }

    private RelOptCost costOf(OpenSearchJoin join) {
        return join.computeSelfCost(volcano, RelMetadataQuery.instance());
    }

    private static void assertInfinite(String message, RelOptCost cost) {
        assertTrue(message + " (got " + cost + ")", cost.isInfinite());
    }

    private static void assertNotInfinite(String message, RelOptCost cost) {
        assertFalse(message + " (got " + cost + ")", cost.isInfinite());
    }
}
