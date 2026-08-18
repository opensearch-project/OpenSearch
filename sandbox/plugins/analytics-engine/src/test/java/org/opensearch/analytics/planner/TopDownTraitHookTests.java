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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for the Calcite {@code PhysicalNode} hooks that carry distribution traits TOP-DOWN
 * ({@code passThroughTraits} / {@code deriveTraits}), plus {@code OpenSearchConvention.enforce}.
 *
 * <p>These are pure functions over {@link OpenSearchDistribution} — no planner run — and are the
 * top-down mirror of {@link DistributionAwareTests}, which covers the same algebra in the bottom-up
 * direction. Both directions must agree, so each transparent operator is asserted to round-trip: the
 * distribution it demands downward is the one it reports upward.
 */
public class TopDownTraitHookTests extends BasePlannerRulesTests {

    private static final int N = 3;
    private static final List<String> DF = List.of("mock-parquet");

    private OpenSearchDistributionTraitDef traitDef;
    private RelOptCluster volcanoCluster;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // The base class's cluster is backed by HepPlanner, which ignores addRelTraitDef — a
        // RelTraitSet built from it silently DROPS the distribution, so every hook would see null and
        // each assertion would pass vacuously. These hooks read the distribution OUT of a trait set, so
        // they need a real VolcanoPlanner with both trait defs registered (same pattern as
        // SplitRuleContractTests).
        PlannerContext ctx = buildContext("parquet", intFieldMap());
        traitDef = ctx.getDistributionTraitDef();
        VolcanoPlanner volcano = new VolcanoPlanner();
        volcano.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcano.addRelTraitDef(traitDef);
        volcanoCluster = RelOptCluster.create(volcano, rexBuilder);
    }

    private static Map<String, Map<String, Object>> intFieldMap() {
        return Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer"));
    }

    private RelTraitSet distTraits(OpenSearchDistribution dist) {
        return RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(dist);
    }

    // ── the null-default contract ────────────────────────────────────────────────

    /**
     * A node that has NOT opted in must return {@code null} from both hooks — Calcite's contract for
     * "no alternative". This default is what lets the ~10 rel nodes with no distribution algebra stay
     * untouched when top-down mode is switched on; if it ever returned a bogus Pair, Volcano would
     * plan against a distribution the operator cannot actually deliver.
     */
    public void testUnopinionatedNodeYieldsNoAlternative() {
        OpenSearchTableScan scan = shardScan();
        assertNull("scan declares no pass-through alternative", scan.passThroughTraits(distTraits(traitDef.coordSingleton())));
        assertNull("scan declares no derive alternative", scan.deriveTraits(distTraits(traitDef.hash(List.of(0), N)), 0));
    }

    // ── OpenSearchFilter: rides any distribution ────────────────────────────────

    public void testFilter_passesHashDemandDownUnchanged() {
        OpenSearchFilter filter = filterOverScan();
        OpenSearchDistribution hash = traitDef.hash(List.of(0), N);
        Pair<RelTraitSet, List<RelTraitSet>> pass = filter.passThroughTraits(distTraits(hash));
        assertNotNull("filter rides a hash demand", pass);
        assertEquals("filter delivers the demanded distribution", hash, OpenSearchRelNode.distributionOf(pass.left));
        assertEquals("filter demands the SAME distribution of its input", hash, OpenSearchRelNode.distributionOf(pass.right.get(0)));
    }

    public void testFilter_derivesChildDistributionUpward() {
        OpenSearchFilter filter = filterOverScan();
        OpenSearchDistribution hash = traitDef.hash(List.of(1), N);
        Pair<RelTraitSet, List<RelTraitSet>> derived = filter.deriveTraits(distTraits(hash), 0);
        assertNotNull("filter derives from its child", derived);
        assertEquals("filter outputs its child's distribution", hash, OpenSearchRelNode.distributionOf(derived.left));
    }

    /** Both directions must agree, or top-down and bottom-up planning would disagree on the same tree. */
    public void testFilter_roundTripsBetweenBothDirections() {
        OpenSearchFilter filter = filterOverScan();
        OpenSearchDistribution hash = traitDef.hash(List.of(0), N);
        OpenSearchDistribution demandedOfChild = OpenSearchRelNode.distributionOf(filter.passThroughTraits(distTraits(hash)).right.get(0));
        OpenSearchDistribution deliveredUpward = OpenSearchRelNode.distributionOf(filter.deriveTraits(distTraits(demandedOfChild), 0).left);
        assertEquals("what the filter demands down is what it delivers up", hash, deliveredUpward);
    }

    public void testFilter_declinesWhenRequestCarriesNoDistribution() {
        OpenSearchFilter filter = filterOverScan();
        assertNull("no distribution in the request → no alternative", filter.passThroughTraits(RelTraitSet.createEmpty()));
    }

    // ── OpenSearchProject: rides, but must REMAP keys ───────────────────────────

    /**
     * The demand is expressed in the project's OUTPUT column space; the child speaks INPUT column
     * space. An identity projection makes them coincide, so the key is unchanged.
     */
    public void testIdentityProject_passesKeyThroughUnchanged() {
        OpenSearchProject project = identityProjectOverScan();
        OpenSearchDistribution hash = traitDef.hash(List.of(0), N);
        Pair<RelTraitSet, List<RelTraitSet>> pass = project.passThroughTraits(distTraits(hash));
        assertNotNull("identity project rides a hash demand", pass);
        assertEquals(
            "identity projection leaves the key index alone",
            List.of(0),
            OpenSearchRelNode.distributionOf(pass.right.get(0)).getKeys()
        );
    }

    /**
     * A REORDERING projection must remap: demanding hash on output column 0 (which reads input
     * column 1) has to become a demand for hash on input column 1. Getting this backwards would
     * shuffle on the wrong column — a silent wrong-results bug, since the plan stays type-correct.
     */
    public void testSwappedProject_remapsKeyToInputColumnSpace() {
        OpenSearchProject project = swappedProjectOverScan();
        Pair<RelTraitSet, List<RelTraitSet>> pass = project.passThroughTraits(distTraits(traitDef.hash(List.of(0), N)));
        assertNotNull("swapped project still rides the demand", pass);
        assertEquals(
            "hash on output col 0 becomes hash on input col 1",
            List.of(1),
            OpenSearchRelNode.distributionOf(pass.right.get(0)).getKeys()
        );
    }

    /** A SINGLETON demand carries no keys, so there is nothing to remap and it rides as-is. */
    public void testProject_ridesLocalityOnlyDemandWithoutRemapping() {
        OpenSearchProject project = swappedProjectOverScan();
        OpenSearchDistribution singleton = traitDef.coordSingleton();
        Pair<RelTraitSet, List<RelTraitSet>> pass = project.passThroughTraits(distTraits(singleton));
        assertNotNull("project rides a singleton demand", pass);
        assertEquals("singleton passes down untouched", singleton, OpenSearchRelNode.distributionOf(pass.right.get(0)));
    }

    /**
     * A hash demand with no concrete partition count is NOT enforceable — buildShuffleExchange throws
     * on a null count. Declining is the safe answer; claiming the alternative would surface as an
     * IllegalStateException deep inside Volcano.
     */
    public void testProject_declinesHashDemandWithoutPartitionCount() {
        OpenSearchProject project = identityProjectOverScan();
        assertNull(
            "un-counted hash demand is not enforceable → decline",
            project.passThroughTraits(distTraits(traitDef.hashAny(List.of(0))))
        );
    }

    /** A window/pinned project imposes its OWN singleton requirement and must not claim to ride. */
    public void testWindowProject_declinesToRide() {
        OpenSearchProject windowed = pinnedProjectOverScan();
        assertNull(
            "a pinned/window project does not ride an arbitrary partitioning",
            windowed.passThroughTraits(distTraits(traitDef.hash(List.of(0), N)))
        );
    }

    // ── OpenSearchConvention.enforce ────────────────────────────────────────────

    public void testEnforce_buildsReducerForSingletonDemand() {
        RelNode scan = shardScan();
        RelNode enforced = OpenSearchConvention.INSTANCE.enforce(scan, distTraits(traitDef.coordSingleton()));
        assertTrue(
            "a SINGLETON demand over a shard scan materializes an ExchangeReducer, got " + enforced,
            enforced instanceof OpenSearchExchangeReducer
        );
    }

    public void testEnforce_buildsShuffleForHashDemand() {
        RelNode scan = shardScan();
        RelNode enforced = OpenSearchConvention.INSTANCE.enforce(scan, distTraits(traitDef.hash(List.of(0), N)));
        assertTrue("a HASH demand materializes a ShuffleExchange, got " + enforced, enforced instanceof OpenSearchShuffleExchange);
    }

    /** Only the distribution is a physical trait here, so a request without one has nothing to enforce. */
    public void testEnforce_declinesRequestWithoutDistribution() {
        RelNode scan = shardScan();
        assertNull(
            "no distribution in the request → nothing to enforce",
            OpenSearchConvention.INSTANCE.enforce(scan, RelTraitSet.createEmpty())
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** A 3-shard scan on the volcano cluster carrying a real SHARD+RANDOM distribution. */
    private OpenSearchTableScan shardScan() {
        return new OpenSearchTableScan(
            volcanoCluster,
            distTraits(traitDef.shardRandom(1, 3)),
            mockTable("a_idx", "status", "size"),
            DF,
            List.of()
        );
    }

    private OpenSearchFilter filterOverScan() {
        RelNode scan = shardScan();
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(scan, 0),
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true)
        );
        return new OpenSearchFilter(volcanoCluster, distTraits(traitDef.coordSingleton()), scan, cond, DF);
    }

    private OpenSearchProject identityProjectOverScan() {
        RelNode scan = shardScan();
        List<RexNode> projects = List.of(rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1));
        return new OpenSearchProject(volcanoCluster, distTraits(traitDef.coordSingleton()), scan, projects, scan.getRowType(), DF);
    }

    /** Projects (size, status) — i.e. output col 0 reads input col 1 and vice versa. */
    private OpenSearchProject swappedProjectOverScan() {
        RelNode scan = shardScan();
        List<RexNode> projects = List.of(rexBuilder.makeInputRef(scan, 1), rexBuilder.makeInputRef(scan, 0));
        RelDataTypeSwap swap = new RelDataTypeSwap(scan);
        return new OpenSearchProject(volcanoCluster, distTraits(traitDef.coordSingleton()), scan, projects, swap.rowType(), DF);
    }

    private OpenSearchProject pinnedProjectOverScan() {
        RelNode scan = shardScan();
        List<RexNode> projects = List.of(rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1));
        return new OpenSearchProject(
            volcanoCluster,
            distTraits(traitDef.coordSingleton()),
            scan,
            projects,
            scan.getRowType(),
            DF,
            /* pinAboveExchange */ true
        );
    }

    /** Builds the swapped (size, status) row type without hand-writing a RelDataType literal. */
    private final class RelDataTypeSwap {
        private final RelNode scan;

        RelDataTypeSwap(RelNode scan) {
            this.scan = scan;
        }

        org.apache.calcite.rel.type.RelDataType rowType() {
            return typeFactory.builder().add(scan.getRowType().getFieldList().get(1)).add(scan.getRowType().getFieldList().get(0)).build();
        }
    }
}
