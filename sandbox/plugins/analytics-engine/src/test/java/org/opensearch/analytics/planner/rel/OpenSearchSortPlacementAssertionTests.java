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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.math.BigDecimal;
import java.util.List;

/**
 * Placement matrix for {@link OpenSearchSort#computeSelfCost}, the negative half of the migration from an
 * infinite-cost shape gate to an assertion.
 *
 * <p>Two different mechanisms are exercised, and the distinction is the whole point of the migration:
 * <ul>
 *   <li>An UNRESOLVED INPUT is still PRICED at infinity. It is not a shape rule — an input whose placement
 *       is undecided has no defined cost — and the marking phase legitimately registers such seeds, so this
 *       one cannot become an assertion.</li>
 *   <li>A committed Sort over input it cannot correctly consume is ASSERTED. Both trait-propagation
 *       directions now refuse to build that pair, so reaching it means a builder is wrong, and the failure
 *       mode is silently wrong results (concatenated partition-local order, {@code fetch}×partitions rows)
 *       rather than a slow plan.</li>
 * </ul>
 */
public class OpenSearchSortPlacementAssertionTests extends BasePlannerRulesTests {

    private static final int SHARD_COUNT = 3;

    private VolcanoPlanner volcano;
    private RelOptCluster volcanoCluster;
    private OpenSearchDistributionTraitDef traitDef;
    private RelOptTable testTable;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        PlannerContext context = buildContext("parquet", SHARD_COUNT, intFields());
        traitDef = context.getDistributionTraitDef();
        volcano = new VolcanoPlanner();
        volcano.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcano.addRelTraitDef(traitDef);
        volcanoCluster = RelOptCluster.create(volcano, new RexBuilder(typeFactory));
        testTable = mockTable("test_index", "status", "size");
    }

    /**
     * Guards every other test in this class. An {@code assert} that the JVM never evaluates cannot fail, so
     * without {@code -ea} the assertion cases below would pass while proving nothing at all.
     */
    public void testAssertionsAreEnabledInThisJvm() {
        boolean enabled = false;
        assert enabled = true;
        assertTrue("Assertions must be enabled (-ea) for the placement assertions to mean anything", enabled);
    }

    // ── asserted: a committed global sort/limit needs gathered input ───────

    public void testLimitOverShardPartitionedInputFails() {
        // fetch=10 per shard over 3 shards concatenates to 30 rows, not 10.
        OpenSearchSort sort = limitSort(scanWith(traitDef.shardRandom(tableId(), SHARD_COUNT)), traitDef.coordSingleton());
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> costOf(sort));
        assertTrue("message should name the offending input distribution, got: " + e.getMessage(), e.getMessage().contains("RANDOM"));
    }

    public void testCollatedSortOverShardPartitionedInputFails() {
        // The reducer is a concat gather, so per-shard order arrives interleaved, not merged.
        OpenSearchSort sort = collatedSort(scanWith(traitDef.shardRandom(tableId(), SHARD_COUNT)), traitDef.coordSingleton());
        expectThrows(IllegalStateException.class, () -> costOf(sort));
    }

    public void testCollatedSortOverHashPartitionedInputFails() {
        OpenSearchSort sort = collatedSort(scanWith(traitDef.hash(List.of(0), 4)), traitDef.coordSingleton());
        expectThrows(IllegalStateException.class, () -> costOf(sort));
    }

    // ── accepted: the legal shapes ────────────────────────────────────────

    public void testCollatedSortOverGatheredInputAccepted() {
        OpenSearchSort sort = collatedSort(scanWith(traitDef.coordSingleton()), traitDef.coordSingleton());
        assertNotInfinite("A global sort over COORDINATOR+SINGLETON input is the legal gathered shape", costOf(sort));
    }

    public void testCollatedSortOverSingleShardSingletonAccepted() {
        // SHARD+SINGLETON is already "all rows on one node", so no gather is needed. Narrowing this to
        // COORDINATOR would insert a reducer on every single-shard query for data already on one node.
        OpenSearchSort sort = collatedSort(scanWith(traitDef.shardSingleton(tableId(), 1)), traitDef.shardSingleton(tableId(), 1));
        assertNotInfinite("A global sort over SHARD+SINGLETON input needs no gather", costOf(sort));
    }

    public void testPerPartitionLimitOverShardPartitionedInputAccepted() {
        // The shard-local top-N deliberately placed BELOW the gather: it RIDES its child's partitioning.
        OpenSearchSort sort = limitSort(
            scanWith(traitDef.shardRandom(tableId(), SHARD_COUNT)),
            traitDef.shardRandom(tableId(), SHARD_COUNT),
            /* perPartition */ true
        );
        assertNotInfinite("A perPartition top-N rides its child's partitioning", costOf(sort));
    }

    /**
     * The other rider carve-out — a Sort that constrains nothing — is UNCONSTRUCTIBLE, so {@code perPartition}
     * is the only one that can occur. Calcite's own {@code Sort} constructor asserts {@code "trivial sort"}
     * against exactly the shape {@code OpenSearchSort.ridesChildDistribution()} tests for (no collation, no
     * fetch, no offset). Pinned here so nobody spends time reasoning about how that branch behaves, and so
     * this test starts failing if a Calcite upgrade relaxes the constructor.
     */
    public void testTrivialSortCannotBeConstructedSoPerPartitionIsTheOnlyRider() {
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> sort(
                scanWith(traitDef.shardRandom(tableId(), SHARD_COUNT)),
                RelCollations.EMPTY,
                /* fetch */ null,
                traitDef.shardRandom(tableId(), SHARD_COUNT),
                /* perPartition */ false
            )
        );
        assertEquals("trivial sort", e.getMessage());
    }

    public void testUnresolvedSelfOverPartitionedInputAccepted() {
        // The marking phase's seed. It makes no placement claim, so it cannot contradict its input — and it
        // must NOT trip the assertion, or HEP marking would fail before the search ever runs.
        OpenSearchSort sort = collatedSort(scanWith(traitDef.shardRandom(tableId(), SHARD_COUNT)), traitDef.any());
        assertNotInfinite("An UNRESOLVED seed makes no claim and must not trip the assertion", costOf(sort));
    }

    // ── still priced, not asserted: an unresolved INPUT ────────────────────

    public void testUnresolvedInputIsPricedInfinite() {
        OpenSearchSort sort = collatedSort(scanWith(traitDef.any()), traitDef.coordSingleton());
        assertTrue("An input whose placement is undecided has no defined cost", costOf(sort).isInfinite());
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private int tableId() {
        return testTable.getQualifiedName().hashCode();
    }

    /** A scan stub carrying exactly the requested distribution, bypassing the marking rule so each case can
     *  isolate one (self, input) trait pair. */
    private OpenSearchTableScan scanWith(OpenSearchDistribution dist) {
        RelTraitSet traits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(dist);
        return new OpenSearchTableScan(volcanoCluster, traits, testTable, List.of("mock-parquet"), List.<FieldStorageInfo>of());
    }

    private OpenSearchSort limitSort(RelNode input, OpenSearchDistribution selfTrait) {
        return limitSort(input, selfTrait, false);
    }

    private OpenSearchSort limitSort(RelNode input, OpenSearchDistribution selfTrait, boolean perPartition) {
        return sort(input, RelCollations.EMPTY, literal(10), selfTrait, perPartition);
    }

    private OpenSearchSort collatedSort(RelNode input, OpenSearchDistribution selfTrait) {
        return sort(input, RelCollations.of(0), null, selfTrait, false);
    }

    private OpenSearchSort sort(
        RelNode input,
        RelCollation collation,
        RexNode fetch,
        OpenSearchDistribution selfTrait,
        boolean perPartition
    ) {
        // plus(collation): Calcite's Sort constructor asserts the trait set carries the collation.
        RelTraitSet traits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(selfTrait).plus(collation);
        return new OpenSearchSort(volcanoCluster, traits, input, collation, null, fetch, List.of("mock-parquet"), perPartition);
    }

    private RexNode literal(int value) {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value));
    }

    private RelOptCost costOf(OpenSearchSort sort) {
        return sort.computeSelfCost(volcano, RelMetadataQuery.instance());
    }

    private static void assertNotInfinite(String message, RelOptCost cost) {
        assertFalse(message + " (got " + cost + ")", cost.isInfinite());
    }
}
