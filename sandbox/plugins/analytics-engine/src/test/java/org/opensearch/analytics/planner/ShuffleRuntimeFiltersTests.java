/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.exec.join.DistributionEnforcementPass;
import org.opensearch.analytics.exec.join.RuntimeFilterEligibility;
import org.opensearch.analytics.exec.join.ShuffleRuntimeFilterPayload;
import org.opensearch.analytics.exec.join.ShuffleRuntimeFilters;
import org.opensearch.analytics.planner.dag.DAGBuilder;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.RuntimeFilterFunction;
import org.opensearch.analytics.spi.RuntimeFilterInstructionNode;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the shuffle family: {@link ShuffleRuntimeFilters#plan}'s eligibility and
 * column-resolution rules, the plan surgery that follows, and where the resulting payload lands.
 *
 * <p>Driven through the real planner rather than hand-built stages: the input is a DAG whose join
 * fragments must have the shape {@code Join(ShuffleExchange(StageInputScan), ShuffleExchange(...))},
 * and asserting against a shape the planner actually produces is what makes these tests worth having.
 * A hand-assembled fragment could drift from the real one without any test noticing.
 *
 * <p>1 MiB is passed as the filter size throughout; sizing is not what these tests are about.
 */
public class ShuffleRuntimeFiltersTests extends BasePlannerRulesTests {

    private static final int CLUSTER_DATA_NODES = 3;
    private static final long LARGE = 10_000_000L;
    private static final int BLOOM_BYTES = 1024 * 1024;
    private static final long GENEROUS_GATE = Long.MAX_VALUE;
    /** These tests are about eligibility and column resolution, not about whether the probe side is big enough. */
    private static final long NO_PROBE_FLOOR = 0L;
    /** A floor no fixture's estimated scan can reach, for asserting the gate refuses. */
    private static final long UNREACHABLE_PROBE_FLOOR = Long.MAX_VALUE;

    public void testPlansOneFilterForATwoWayShuffleJoin() {
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER);
        List<ShuffleRuntimeFilters.Descriptor> planned = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR);

        assertEquals("one join, one filter", 1, planned.size());
        ShuffleRuntimeFilters.Descriptor d = planned.get(0);
        // The two producer stages must differ: a filter has to flow from one side to the other, and a
        // descriptor naming the same stage twice would mean summarising the rows it then filters.
        assertNotEquals("build and probe must be distinct producer stages", d.buildStageId(), d.probeStageId());
        assertEquals("the join key column on both sides of this fixture", "status", d.buildKeyColumn());
        assertEquals("status", d.probeKeyColumn());
        assertEquals(BLOOM_BYTES, d.bloomBytes());
        assertEquals("ids start at 0", 0, d.filterId());
    }

    public void testNoJoinTypeMayFilterASidePreservedByTheJoin() {
        // The rule is about the side being filtered, not about a fixed input position: the filter is
        // applied to whichever side was NOT chosen to be summarised. With equal-sized inputs that choice
        // is the left one, so the filtered side here is input 1.
        //
        // LEFT does not preserve input 1, so filtering it is sound and a filter is planted. RIGHT
        // preserves input 1, and FULL preserves both, so neither may be filtered at all.
        assertEquals(
            "LEFT may filter its non-preserved (right) input",
            1,
            ShuffleRuntimeFilters.plan(twoWayShuffleJoinDag(JoinRelType.LEFT), BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).size()
        );
        assertTrue(
            "RIGHT preserves the side that would be filtered here",
            ShuffleRuntimeFilters.plan(twoWayShuffleJoinDag(JoinRelType.RIGHT), BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).isEmpty()
        );
        assertTrue(
            "FULL preserves both sides",
            ShuffleRuntimeFilters.plan(twoWayShuffleJoinDag(JoinRelType.FULL), BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).isEmpty()
        );
    }

    public void testTheSmallerSideIsSummarisedAndTheLargerFiltered() {
        // Deriving the direction from an input's position instead put the fact table on the summarising side
        // for every multi-way join, so the row gate refused all of them and the feature fired on nothing.
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER, /* leftRows */ LARGE, /* rightRows */ LARGE / 100);
        ShuffleRuntimeFilters.Descriptor d = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).get(0);

        assertEquals("the small side is summarised", stageScanning(dag, "b_idx"), d.buildStageId());
        assertEquals("the large side is filtered", stageScanning(dag, "a_idx"), d.probeStageId());
    }

    public void testTheFilterReachesTheFactTablesOwnLeafThroughAnIntermediate() {
        // fact ⋈ dim1, then (fact ⋈ dim1) ⋈ dim2. The upper join's probe side is an intermediate with no
        // scan, so the predicate must descend to the fact table's leaf — which is also where it helps most,
        // since rows dropped there enter no shuffle at all.
        QueryDAG dag = factWithTwoDimensionsDag(JoinRelType.INNER);
        List<ShuffleRuntimeFilters.Descriptor> planned = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR);

        assertEquals("both joins offer a filter", 2, planned.size());
        int factStage = stageScanning(dag, "fact_idx");
        for (ShuffleRuntimeFilters.Descriptor d : planned) {
            assertEquals("every filter must land on the fact table's own leaf", factStage, d.probeStageId());
        }
        assertEquals(
            "and each is keyed on the column that join actually equates",
            Set.of("dim1_key", "dim2_key"),
            planned.stream().map(ShuffleRuntimeFilters.Descriptor::probeKeyColumn).collect(java.util.stream.Collectors.toSet())
        );
    }

    public void testTwoFiltersOnOneLeafAreBothPlanted() {
        // Keying the plant step by stage id alone would keep whichever descriptor came last. Both have to
        // survive: a fact table joined to two dimensions is filtered by both, and losing one silently
        // halves the benefit.
        QueryDAG dag = factWithTwoDimensionsDag(JoinRelType.INNER);
        List<ShuffleRuntimeFilters.Descriptor> planned = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR);
        ShuffleRuntimeFilters.Planted result = ShuffleRuntimeFilters.plantProbePredicates(dag, planned);

        assertEquals("both descriptors planted", 2, result.descriptors().size());
        // Counted per payload so the funnel stays comparable: two filters on one leaf is two payloads
        // delivered, not one. Counting stages here made a working delivery look like a lost pre-pass.
        givePlanAlternatives(result.dag().rootStage());
        assertEquals(
            "two payloads attached, even though they share a stage",
            2,
            ShuffleRuntimeFilterPayload.attach(result.dag().rootStage(), Map.of(0, new byte[] { 1 }, 1, new byte[] { 2 }))
        );
        assertEquals(
            "both predicates present on the fact leaf",
            Set.of(0, 1),
            ShuffleRuntimeFilterPayload.plantedFilterIds(stageById(result.dag(), stageScanning(dag, "fact_idx")).getFragment())
        );
    }

    public void testAnAmbiguousColumnNameBlocksTheDescent() {
        // Descent resolves the application point by column NAME. In this fixture every table has a column
        // called `status`, so the name does not identify which scan the join key physically comes from —
        // and filtering the wrong table by the right name drops rows that belong in the result.
        QueryDAG dag = threeWayShuffleJoinDag();
        List<ShuffleRuntimeFilters.Descriptor> planned = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR);

        for (ShuffleRuntimeFilters.Descriptor d : planned) {
            assertEquals(
                "a filter may only target a stage whose own scan binds the key unambiguously",
                1,
                (int) countScansExposing(stageById(dag, d.probeStageId()).getFragment(), d.probeKeyColumn())
            );
        }
    }

    public void testTheShippedDefaultsAdmitTheMeasuredWinsAndRefuseTheMeasuredNulls() {
        // Pins the calibrated defaults against silent drift. In this estimator's units: no effect up to
        // ~0.40 GB of probe scan, +24% at ~0.80 GB, +75% at ~3.60 GB; on the build side 22.76M emitted rows
        // paid 75%, while a 600M-row fact table is what the ceiling exists to refuse.
        long floor = AnalyticsSettings.RUNTIME_FILTER_PROBE_SIDE_MIN_SCAN_BYTES.getDefault(Settings.EMPTY).getBytes();
        long ceiling = AnalyticsSettings.RUNTIME_FILTER_BUILD_SIDE_MAX_ROWS.getDefault(Settings.EMPTY);

        long largestMeasuredNull = 400_000_000L;
        long smallestMeasuredWin = 800_000_000L;
        assertTrue("the floor must refuse the largest probe side measured to gain nothing", floor >= largestMeasuredNull);
        assertTrue("and admit the smallest measured to gain", floor < smallestMeasuredWin);
        assertTrue("the ceiling must admit the build side measured to pay 75%", ceiling > 22_760_819L);
        assertTrue("and refuse a fact table as the summarised side", ceiling < 600_037_902L);
    }

    public void testTheProbeSideFloorSuppressesAFilterOnSomethingCheap() {
        // Removing rows only pays when moving them was expensive: the query that gained 24% gained nothing
        // at a tenth of the data. The gate is in BYTES because no row count separates those cases — 80M rows
        // gained, 60M narrower rows gained nothing.
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER, /* leftRows */ LARGE, /* rightRows */ LARGE / 100);

        assertEquals(
            "with no floor the filter is planned",
            1,
            ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).size()
        );
        // Asserting the two ends rather than a fitted number: how many bytes a fixture is estimated to scan
        // is Calcite's row-size metadata, not this test's contract. What is the contract is that the gate
        // reads that estimate at all — a floor nothing can reach must refuse, and a trivial one must admit.
        assertEquals(
            "a floor far below the fixture's scan admits it",
            1,
            ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, 1024L).size()
        );
        assertTrue(
            "a probe side below the floor is not worth filtering",
            ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, UNREACHABLE_PROBE_FLOOR).isEmpty()
        );
    }

    public void testTheBuildGateMeasuresWhatTheSideEmitsNotWhatItScans() {
        // A dimension that scans many rows and emits few is cheap to summarise and a useful filter, so the
        // gate reads estimated output rather than scan count. Here the build side scans LARGE/100 rows and a
        // gate below that must still admit it.
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER, /* leftRows */ LARGE, /* rightRows */ LARGE / 100);
        long buildScanRows = LARGE / 100;

        assertEquals(
            "the gate is compared against the estimate, which never exceeds the scan count",
            1,
            ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, buildScanRows, NO_PROBE_FLOOR).size()
        );
    }

    public void testTheFilterIsSizedFromTheBuildSideNotFixed() {
        // A fixed size fails silently in one direction: 22.7M keys in 1 MiB is ~2.7 bits per key, where the
        // false-positive rate approaches 1 and the query gained 3.9% instead of 78%. Sized at ~10 bits per
        // key, then rounded up to the power of two the backend allocates — 1M keys request 1_250_000 bytes
        // and get 2 MiB.
        assertEquals("~10 bits per key", 2 * 1024 * 1024, ShuffleRuntimeFilters.bloomBytesFor(1_000_000L, Integer.MAX_VALUE));
        assertEquals("and it scales", 32 * 1024 * 1024, ShuffleRuntimeFilters.bloomBytesFor(22_750_000L, Integer.MAX_VALUE));

        // The ceiling is what the setting means now. Above it accuracy is traded for payload, because the
        // payload is shipped once per probe-side shard task.
        assertEquals("capped by the ceiling", 1024, ShuffleRuntimeFilters.bloomBytesFor(22_750_000L, 1024));

        // A floor, so a tiny build side still gets something worth probing rather than a few blocks.
        assertEquals("floored", 64 * 1024, ShuffleRuntimeFilters.bloomBytesFor(1L, Integer.MAX_VALUE));
        assertEquals("floored even at zero rows", 64 * 1024, ShuffleRuntimeFilters.bloomBytesFor(0L, Integer.MAX_VALUE));
    }

    public void testAnUnfilterableKeyTypePlansNothing() {
        // The backend's signatures cover the signed integral widths and the two date widths only. Any other
        // type yields an unresolvable predicate in the MAIN plan, so a join that works with the setting off
        // would FAIL with it on — the only refusal here whose absence costs a result, hence per-type.
        for (SqlTypeName unfilterable : new SqlTypeName[] {
            SqlTypeName.VARCHAR,
            SqlTypeName.CHAR,
            SqlTypeName.DECIMAL,
            SqlTypeName.DOUBLE,
            SqlTypeName.BOOLEAN,
            SqlTypeName.TIMESTAMP }) {
            assertTrue(
                unfilterable + " is not a filterable key type and must plan nothing",
                ShuffleRuntimeFilters.plan(twoWayShuffleJoinDagKeyedOn(unfilterable), BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR)
                    .isEmpty()
            );
        }

        // And the accepted ones still plan, so the guard is a type check rather than a blanket refusal.
        for (SqlTypeName filterable : new SqlTypeName[] {
            SqlTypeName.TINYINT,
            SqlTypeName.SMALLINT,
            SqlTypeName.INTEGER,
            SqlTypeName.BIGINT,
            SqlTypeName.DATE }) {
            assertFalse(
                filterable + " is a filterable key type and must still plan",
                ShuffleRuntimeFilters.plan(twoWayShuffleJoinDagKeyedOn(filterable), BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).isEmpty()
            );
        }
    }

    public void testNoFilterIsBuiltFromAnIntermediateBuildSide() {
        // A pre-pass mini-DAG runs fork / adapt / select / convert only, so a copied subtree still holding a
        // distributed join reaches a data node without its shuffle instructions and computes nothing.
        //
        // A bushy tree reaches the case: an intermediate producer reports unbounded rows, so two
        // intermediates still resolve to input 0. Every filter here must come from a lower tier whose
        // producers are leaf scans, none from the top join.
        QueryDAG bushy = bushyShuffleJoinDag();
        List<ShuffleRuntimeFilters.Descriptor> planned = ShuffleRuntimeFilters.plan(
            bushy,
            BLOOM_BYTES,
            GENEROUS_GATE,
            NO_PROBE_FLOOR
        );
        Map<Integer, Stage> byId = new HashMap<>();
        indexStages(bushy.rootStage(), byId);
        for (ShuffleRuntimeFilters.Descriptor d : planned) {
            Stage buildStage = byId.get(d.buildStageId());
            assertNotNull("every planned build stage is in the DAG", buildStage);
            assertFalse(
                "no filter is built from a stage with no scan of its own (stage " + d.buildStageId() + ")",
                RelNodeUtils.findNodes(buildStage.getFragment(), OpenSearchTableScan.class).isEmpty()
            );
        }
    }

    private static void indexStages(Stage stage, Map<Integer, Stage> out) {
        out.put(stage.getStageId(), stage);
        for (Stage child : stage.getChildStages()) {
            indexStages(child, out);
        }
    }

    public void testAKeyRedefinedByAProjectHasNoApplicationPoint() {
        // The application point is found by NAME, so a project keeping the name while changing the value is
        // where name-matching is actively wrong: the summary holds the join's key, the predicate would test
        // the scan's raw column. A rename is harmless — the name stops resolving and no filter is applied.
        RelNode scan = stubScan(mockTable("a_idx", "id", "amount"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        assertTrue(
            "a bare pass-through of the same column keeps the name meaningful",
            RuntimeFilterEligibility.keyPassesThroughProjectsUnchanged(projectOver(scan, "id", rexBuilder.makeInputRef(intType, 0)), "id")
        );
        assertFalse(
            "a computed key must block the descent",
            RuntimeFilterEligibility.keyPassesThroughProjectsUnchanged(
                projectOver(
                    scan,
                    "id",
                    rexBuilder.makeCall(SqlStdOperatorTable.PLUS, rexBuilder.makeInputRef(intType, 0), rexBuilder.makeLiteral(1, intType))
                ),
                "id"
            )
        );
        // An identity reference is not sufficient on its own: this one is bare, and still leaves a field
        // called `id` whose values come from a different column than the scan's `id`.
        assertFalse(
            "a bare reference to a DIFFERENT column must also block the descent",
            RuntimeFilterEligibility.keyPassesThroughProjectsUnchanged(projectOver(scan, "id", rexBuilder.makeInputRef(intType, 1)), "id")
        );
        assertTrue(
            "a project that does not mention the key is irrelevant to it",
            RuntimeFilterEligibility.keyPassesThroughProjectsUnchanged(
                projectOver(scan, "other", rexBuilder.makeInputRef(intType, 1)),
                "id"
            )
        );
        assertTrue("no project at all is trivially unchanged", RuntimeFilterEligibility.keyPassesThroughProjectsUnchanged(scan, "id"));
    }

    /**
     * A single-column {@link OpenSearchProject} over {@code input}, naming its one output {@code name}.
     *
     * <p>The backend list is empty: the rule under test reads only names and expressions, and the fixture's
     * stub scan is not an {@code OpenSearchRelNode} to inherit one from.
     */
    private RelNode projectOver(RelNode input, String name, RexNode expr) {
        RelDataType rowType = typeFactory.builder().add(name, expr.getType()).build();
        return new OpenSearchProject(input.getCluster(), input.getTraitSet(), input, List.of(expr), rowType, List.of());
    }

    public void testTheSizeReturnedIsTheSizeTheBackendAllocates() {
        // The backend rounds a request up to a power of two. Returning the raw request instead meant a
        // ceiling that is not itself a power of two was exceeded by up to 2x, so the setting bounded neither
        // the per-filter size nor the per-query budget built on top of it.
        int twentyMiB = 20 * 1024 * 1024;
        assertEquals(
            "a non-power-of-two ceiling binds to the largest power of two below it",
            16 * 1024 * 1024,
            ShuffleRuntimeFilters.bloomBytesFor(22_750_000L, twentyMiB)
        );
        assertTrue("and is never exceeded", ShuffleRuntimeFilters.bloomBytesFor(22_750_000L, twentyMiB) <= twentyMiB);

        // Rounding is UP whenever the rounded size still fits, because rounding down would halve the bits
        // per key — the one quantity that decides whether the filter removes rows or admits everything.
        assertEquals(
            "a fitting request rounds up, not down",
            2 * 1024 * 1024,
            ShuffleRuntimeFilters.bloomBytesFor(1_000_000L, 32 * 1024 * 1024)
        );

        // Every size handed to the backend is one it can represent, so it never silently enlarges one.
        for (long rows : new long[] { 0L, 1L, 1_000L, 1_000_000L, 22_750_000L, 500_000_000L }) {
            int size = ShuffleRuntimeFilters.bloomBytesFor(rows, 32 * 1024 * 1024);
            assertEquals("power of two for " + rows + " rows", Integer.highestOneBit(size), size);
            assertTrue("at or above the backend minimum for " + rows + " rows", size >= 32);
        }
    }

    public void testASmallBuildSideStillGetsASmallFilter() {
        // Raising the ceiling must not pad every filter to it: the derivation caps, it does not inflate.
        // This is what keeps the query measured at 24% — whose build side is a few hundred thousand keys —
        // from paying a 32 MiB payload per shard task for no accuracy it can use.
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER, /* leftRows */ LARGE, /* rightRows */ LARGE / 100);
        int ceiling = 32 * 1024 * 1024;
        ShuffleRuntimeFilters.Descriptor d = ShuffleRuntimeFilters.plan(dag, ceiling, GENEROUS_GATE, NO_PROBE_FLOOR).get(0);

        assertTrue("sized well under the ceiling for a 100k-row build side", d.bloomBytes() < ceiling);
        assertTrue("but not below the floor", d.bloomBytes() >= 64 * 1024);
    }

    public void testAMultiLevelBuildSideGetsItsOwnCopyOfEveryStage() {
        // A mini-DAG sharing a Stage object with the query's DAG would put two concurrent executions behind
        // one object and race on the instructions the shuffle enrichment attaches to it. Hence a private
        // copy, which is what makes a multi-level build side safe to re-run.
        QueryDAG dag = threeWayShuffleJoinDag();
        List<ShuffleRuntimeFilters.Descriptor> planned = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR);
        assertFalse("the three-way plan must offer at least one filter", planned.isEmpty());

        ShuffleRuntimeFilters.Descriptor d = planned.get(0);
        Stage buildStage = stageById(dag, d.buildStageId());
        QueryDAG prePass = ShuffleRuntimeFilters.prePassDag("qid-rf", buildStage, d);
        assertNotNull(prePass);

        // No Stage object may be shared with the query's DAG, at any depth.
        Set<Stage> queryStages = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        collectStages(dag.rootStage(), queryStages);
        Set<Stage> prePassStages = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        collectStages(prePass.rootStage(), prePassStages);
        for (Stage s : prePassStages) {
            assertFalse("pre-pass shares stage " + s.getStageId() + " with the query DAG", queryStages.contains(s));
        }

        // Ids are preserved on purpose: renumbering would mean rewriting every StageInputScan reference
        // inside the fragments, and the mini-DAG runs under its own query id so ids cannot collide.
        if (!buildStage.getChildStages().isEmpty()) {
            assertEquals(
                "the copy keeps the original stage ids",
                buildStage.getChildStages().stream().map(Stage::getStageId).sorted().toList(),
                prePass.rootStage().getChildStages().stream().map(Stage::getStageId).sorted().toList()
            );
        }
    }

    public void testThePrePassCostGateCountsEveryTableItWouldRead() {
        // Re-running a multi-table subtree to build a filter is a much larger bet than re-scanning one
        // column, so the gate covers what the pre-pass READS, summed over the subtree — not just what the
        // build side emits.
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER, /* leftRows */ LARGE, /* rightRows */ LARGE / 100);
        assertEquals("a generous gate admits it", 1, ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).size());
        assertTrue(
            "a gate below what the pre-pass would scan refuses it",
            ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, 1L, NO_PROBE_FLOOR).isEmpty()
        );
    }

    public void testTheRowGateSuppressesAnExpensivePrePass() {
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER);
        // The pre-pass is a SECOND scan of the build table, so above the gate it cannot repay itself.
        assertTrue(
            "build side over the gate must yield no filter",
            ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, LARGE - 1, NO_PROBE_FLOOR).isEmpty()
        );
        assertEquals("at the gate it is still planned", 1, ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, LARGE, NO_PROBE_FLOOR).size());
    }

    public void testDisabledByZeroOrNegativeInputs() {
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER);
        assertTrue("a zero gate disables the shuffle family", ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, 0, NO_PROBE_FLOOR).isEmpty());
        assertTrue("a zero size is not a filter", ShuffleRuntimeFilters.plan(dag, 0, GENEROUS_GATE, NO_PROBE_FLOOR).isEmpty());
        assertTrue("a null DAG is tolerated", ShuffleRuntimeFilters.plan(null, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).isEmpty());
    }

    public void testPlansNothingWithoutAShuffleJoin() {
        // A single scan has no join at all; the pass must return empty rather than fail.
        Map<String, Integer> shardCounts = Map.of("a_idx", 3);
        PlannerContext context = mppContext(shardCounts, Map.of("a_idx", LARGE));
        RelNode cbo = runPlanner(stubScan(mockTable("a_idx", "status", "size")), context);
        RelNode enforced = DistributionEnforcementPass.enforce(
            cbo,
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L,
            /* shuffleAggregateEnabled */ true
        );
        QueryDAG dag = DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        assertTrue(ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).isEmpty());
    }

    public void testFilterIdsAreDistinctAcrossJoins() {
        // A three-way join forms two binary worker tiers, so two joins are candidates. Ids must not
        // collide: the id is what pairs a plan predicate with its payload instruction, and a duplicate
        // would make one join probe the other's filter.
        QueryDAG dag = threeWayShuffleJoinDag();
        List<ShuffleRuntimeFilters.Descriptor> planned = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR);
        assertFalse("the three-way plan should offer at least one filter", planned.isEmpty());
        assertEquals(
            "filter ids must be unique",
            planned.size(),
            planned.stream().map(ShuffleRuntimeFilters.Descriptor::filterId).distinct().count()
        );
    }

    // ── Plan surgery ─────────────────────────────────────────────────────

    public void testPlantsTheProbePredicateDirectlyAboveTheProbeScan() {
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER);
        ShuffleRuntimeFilters.Descriptor d = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).get(0);
        ShuffleRuntimeFilters.Planted planted = ShuffleRuntimeFilters.plantProbePredicates(dag, List.of(d));

        assertEquals("the descriptor is reported as planted", List.of(d), planted.descriptors());
        RelNode probeFragment = stageById(planted.dag(), d.probeStageId()).getFragment();
        assertEquals(
            "the probe fragment carries exactly this filter's predicate",
            Set.of(d.filterId()),
            ShuffleRuntimeFilterPayload.plantedFilterIds(probeFragment)
        );

        // Directly above the scan, which is the earliest point in a shard-local fragment: every row it drops
        // is a row nothing above it — and nothing in the shuffle — has to handle.
        List<OpenSearchFilter> filters = RelNodeUtils.findNodes(probeFragment, OpenSearchFilter.class);
        OpenSearchFilter probeFilter = filters.stream()
            .filter(f -> !ShuffleRuntimeFilterPayload.plantedFilterIds(f).isEmpty())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no planted filter found in " + probeFragment));
        assertTrue(
            "the predicate must sit on the scan, not above a Project that could rename its key",
            RelNodeUtils.unwrapHep(probeFilter.getInput(0)) instanceof OpenSearchTableScan
        );
    }

    public void testPlantingLeavesTheBuildSideUnfiltered() {
        // The filter describes the build side; applying it there would be circular, and on an inner join it
        // would also be the one place a false positive costs correctness rather than time.
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER);
        ShuffleRuntimeFilters.Descriptor d = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).get(0);
        ShuffleRuntimeFilters.Planted planted = ShuffleRuntimeFilters.plantProbePredicates(dag, List.of(d));

        assertTrue(
            "no predicate on the build producer",
            ShuffleRuntimeFilterPayload.plantedFilterIds(stageById(planted.dag(), d.buildStageId()).getFragment()).isEmpty()
        );
    }

    public void testThePrePassIsAStandaloneShardStageOverTheBuildKey() {
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER);
        ShuffleRuntimeFilters.Descriptor d = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).get(0);
        Stage buildStage = stageById(dag, d.buildStageId());

        QueryDAG prePass = ShuffleRuntimeFilters.prePassDag("qid-rf", buildStage, d);
        assertNotNull(prePass);
        Stage stage = prePass.rootStage();

        // Standalone: sharing a child Stage object with the query's own DAG would put two concurrent
        // executions behind one object.
        assertTrue("the pre-pass must not reach into the query's DAG", stage.getChildStages().isEmpty());
        assertNotSame("and it must not be the build stage itself", buildStage, stage);
        assertEquals("it runs per shard, like the producer it summarises", StageExecutionType.SHARD_FRAGMENT, stage.getExecutionType());
        assertTrue("its id is outside the range DAGBuilder allocates", stage.getStageId() >= 100_000);

        // One VARBINARY column: the bitset the coordinator unions.
        assertEquals(1, stage.getFragment().getRowType().getFieldCount());
        assertEquals(SqlTypeName.VARBINARY, stage.getFragment().getRowType().getFieldList().get(0).getType().getSqlTypeName());

        List<OpenSearchAggregate> aggregates = RelNodeUtils.findNodes(stage.getFragment(), OpenSearchAggregate.class);
        assertEquals("one global aggregate", 1, aggregates.size());
        OpenSearchAggregate aggregate = aggregates.get(0);
        assertTrue("no group keys — one filter per shard, not one per key", aggregate.getGroupSet().isEmpty());
        assertEquals(1, aggregate.getAggCallList().size());
        assertEquals(RuntimeFilterFunction.BLOOM_AGG_NAME, aggregate.getAggCallList().get(0).getAggregation().getName());
        assertEquals(
            "the aggregate takes (key, numBytes), both materialised as columns by the Project below it",
            2,
            aggregate.getAggCallList().get(0).getArgList().size()
        );
    }

    // ── Payload delivery ─────────────────────────────────────────────────

    public void testThePayloadAttachesToTheStageCarryingThePredicate() {
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER);
        ShuffleRuntimeFilters.Descriptor d = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).get(0);
        QueryDAG planted = ShuffleRuntimeFilters.plantProbePredicates(dag, List.of(d)).dag();
        // Stand in for what the conversion pipeline leaves behind: an instruction has to live on a plan.
        givePlanAlternatives(planted.rootStage());

        byte[] bitset = new byte[] { 1, 2, 3, 4 };
        assertEquals("exactly one stage takes the payload", 1, ShuffleRuntimeFilterPayload.attach(planted.rootStage(), Map.of(0, bitset)));

        Stage probeStage = stageById(planted, d.probeStageId());
        List<InstructionNode> instructions = probeStage.getPlanAlternatives().get(0).instructions();
        assertEquals(1, instructions.size());
        RuntimeFilterInstructionNode node = (RuntimeFilterInstructionNode) instructions.get(0);
        assertEquals(d.filterId(), node.getFilterId());
        assertArrayEquals(bitset, node.getPayload());
        assertTrue(
            "the build producer gets nothing — it has no predicate to satisfy",
            stageById(planted, d.buildStageId()).getPlanAlternatives().get(0).instructions().isEmpty()
        );
    }

    public void testAPayloadForAnUnplantedIdIsNotAttachedAnywhere() {
        // Ids pair a predicate with its bytes. Attaching by anything looser would let one join's filter be
        // installed for another's key.
        QueryDAG dag = twoWayShuffleJoinDag(JoinRelType.INNER);
        ShuffleRuntimeFilters.Descriptor d = ShuffleRuntimeFilters.plan(dag, BLOOM_BYTES, GENEROUS_GATE, NO_PROBE_FLOOR).get(0);
        QueryDAG planted = ShuffleRuntimeFilters.plantProbePredicates(dag, List.of(d)).dag();
        givePlanAlternatives(planted.rootStage());

        assertEquals(0, ShuffleRuntimeFilterPayload.attach(planted.rootStage(), Map.of(d.filterId() + 1, new byte[] { 9 })));
        assertTrue(stageById(planted, d.probeStageId()).getPlanAlternatives().get(0).instructions().isEmpty());
    }

    // ── Fixtures ─────────────────────────────────────────────────────────

    /** Stage id of the producer whose fragment scans {@code indexName}. */
    private static int stageScanning(QueryDAG dag, String indexName) {
        Integer found = findStageScanning(dag.rootStage(), indexName);
        assertNotNull("no stage scans " + indexName, found);
        return found;
    }

    private static Integer findStageScanning(Stage stage, String indexName) {
        if (stage.getFragment() != null) {
            for (OpenSearchTableScan scan : RelNodeUtils.findNodes(stage.getFragment(), OpenSearchTableScan.class)) {
                if (scan.getTable().getQualifiedName().contains(indexName)) {
                    return stage.getStageId();
                }
            }
        }
        for (Stage child : stage.getChildStages()) {
            Integer found = findStageScanning(child, indexName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static Stage stageById(QueryDAG dag, int stageId) {
        return findStage(dag.rootStage(), stageId);
    }

    private static void collectStages(Stage stage, Set<Stage> out) {
        out.add(stage);
        for (Stage child : stage.getChildStages()) {
            collectStages(child, out);
        }
    }

    private static Stage findStage(Stage stage, int stageId) {
        if (stage.getStageId() == stageId) {
            return stage;
        }
        for (Stage child : stage.getChildStages()) {
            Stage found = findStage(child, stageId);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** Gives every stage the single unconverted plan alternative the real pipeline would have produced. */
    private static void givePlanAlternatives(Stage stage) {
        stage.setPlanAlternatives(List.of(new StagePlan(stage.getFragment(), "mock-datafusion")));
        for (Stage child : stage.getChildStages()) {
            givePlanAlternatives(child);
        }
    }

    /**
     * {@code (fact ⋈ dim1) ⋈ dim2} — the left-deep shape of a real multi-way join, with a large fact table
     * and two small dimensions, and DISTINCT key column names so the descent is unambiguous.
     */
    private QueryDAG factWithTwoDimensionsDag(JoinRelType joinType) {
        Map<String, Integer> shardCounts = Map.of("fact_idx", 3, "dim1_idx", 3, "dim2_idx", 3);
        Map<String, Long> rowCounts = Map.of("fact_idx", LARGE, "dim1_idx", LARGE / 1000, "dim2_idx", LARGE / 1000);
        PlannerContext context = mppContext(shardCounts, rowCounts, 1L);

        RelNode fact = stubScan(mockTable("fact_idx", "dim1_key", "dim2_key", "amount"));
        RelNode dim1 = stubScan(mockTable("dim1_idx", "dim1_key", "label1"));
        RelNode dim2 = stubScan(mockTable("dim2_idx", "dim2_key", "label2"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // fact.dim1_key = dim1.dim1_key
        RexNode cond1 = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, fact.getRowType().getFieldCount())
        );
        RelNode joined1 = LogicalJoin.create(fact, dim1, List.of(), cond1, Set.of(), joinType);
        // (…).dim2_key = dim2.dim2_key — dim2_key is ordinal 1 of the fact side, preserved through the join
        RexNode cond2 = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeInputRef(intType, joined1.getRowType().getFieldCount())
        );
        RelNode joined2 = LogicalJoin.create(joined1, dim2, List.of(), cond2, Set.of(), joinType);

        RelNode enforced = DistributionEnforcementPass.enforce(
            runPlanner(joined2, context),
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L,
            /* shuffleAggregateEnabled */ true
        );
        return DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
    }

    /** How many table scans in this fragment expose {@code column} — the ambiguity the descent must refuse. */
    private static long countScansExposing(RelNode fragment, String column) {
        return RelNodeUtils.findNodes(fragment, OpenSearchTableScan.class)
            .stream()
            .filter(scan -> scan.getRowType().getFieldNames().contains(column))
            .count();
    }

    /** {@code a ⋈ b} on {@code status}, planned and enforced into two shuffle producers under one join. */
    private QueryDAG twoWayShuffleJoinDag(JoinRelType joinType) {
        return twoWayShuffleJoinDag(joinType, LARGE, LARGE);
    }

    /** As above with a chosen type for the join key, so an unfilterable key type is testable. */
    private QueryDAG twoWayShuffleJoinDagKeyedOn(SqlTypeName keyType) {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE);
        PlannerContext context = mppContext(shardCounts, rowCounts, 1L);

        String[] fields = { "status", "size" };
        SqlTypeName[] types = { keyType, SqlTypeName.INTEGER };
        RelNode leftScan = stubScan(mockTable("a_idx", fields, types));
        RelNode rightScan = stubScan(mockTable("b_idx", fields, types));
        RelDataType keyRelType = typeFactory.createSqlType(keyType);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(keyRelType, 0),
            rexBuilder.makeInputRef(keyRelType, leftScan.getRowType().getFieldCount())
        );
        RelNode join = LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), JoinRelType.INNER);

        RelNode enforced = DistributionEnforcementPass.enforce(
            runPlanner(join, context),
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L,
            /* shuffleAggregateEnabled */ true
        );
        return DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
    }

    /** As above with per-side row counts, so the size-driven choice of which side to summarise is testable. */
    private QueryDAG twoWayShuffleJoinDag(JoinRelType joinType, long leftRows, long rightRows) {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", leftRows, "b_idx", rightRows);
        // A tiny broadcast cap forces CBO to shuffle rather than take the broadcast alternative, which is
        // the family under test here.
        PlannerContext context = mppContext(shardCounts, rowCounts, 1L);

        RelNode leftScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode rightScan = stubScan(mockTable("b_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftScan.getRowType().getFieldCount())
        );
        RelNode join = LogicalJoin.create(leftScan, rightScan, List.of(), condition, Set.of(), joinType);

        RelNode enforced = DistributionEnforcementPass.enforce(
            runPlanner(join, context),
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L,
            /* shuffleAggregateEnabled */ true
        );
        return DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
    }

    /** {@code (a ⋈ b) ⋈ c} on a shared key, which the enforcement pass lowers into two binary tiers. */
    private QueryDAG threeWayShuffleJoinDag() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE);
        PlannerContext context = mppContext(shardCounts, rowCounts, 1L);

        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aScan.getRowType().getFieldCount())
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);
        RexNode abcCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, ab.getRowType().getFieldCount())
        );
        RelNode abc = LogicalJoin.create(ab, cScan, List.of(), abcCond, Set.of(), JoinRelType.INNER);

        RelNode enforced = DistributionEnforcementPass.enforce(
            runPlanner(abc, context),
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L,
            /* shuffleAggregateEnabled */ true
        );
        return DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
    }

    /** {@code (a ⋈ b) ⋈ (c ⋈ d)} — a bushy tree, whose top join has an intermediate on BOTH sides. */
    private QueryDAG bushyShuffleJoinDag() {
        Map<String, Integer> shardCounts = Map.of("a_idx", 3, "b_idx", 3, "c_idx", 3, "d_idx", 3);
        Map<String, Long> rowCounts = Map.of("a_idx", LARGE, "b_idx", LARGE, "c_idx", LARGE, "d_idx", LARGE);
        PlannerContext context = mppContext(shardCounts, rowCounts, 1L);

        RelNode aScan = stubScan(mockTable("a_idx", "status", "size"));
        RelNode bScan = stubScan(mockTable("b_idx", "status", "size"));
        RelNode cScan = stubScan(mockTable("c_idx", "status", "size"));
        RelNode dScan = stubScan(mockTable("d_idx", "status", "size"));
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RexNode abCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, aScan.getRowType().getFieldCount())
        );
        RelNode ab = LogicalJoin.create(aScan, bScan, List.of(), abCond, Set.of(), JoinRelType.INNER);
        RexNode cdCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, cScan.getRowType().getFieldCount())
        );
        RelNode cd = LogicalJoin.create(cScan, dScan, List.of(), cdCond, Set.of(), JoinRelType.INNER);
        RexNode topCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, ab.getRowType().getFieldCount())
        );
        RelNode top = LogicalJoin.create(ab, cd, List.of(), topCond, Set.of(), JoinRelType.INNER);

        RelNode enforced = DistributionEnforcementPass.enforce(
            runPlanner(top, context),
            context.getDistributionTraitDef(),
            CLUSTER_DATA_NODES,
            /* minRows */ 1L,
            /* shuffleAggregateEnabled */ true
        );
        return DAGBuilder.build(enforced, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
    }

    private PlannerContext mppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts) {
        return mppContext(shardCounts, rowCounts, -1L);
    }

    /**
     * @param maxBroadcastBytes when {@code >= 0}, caps {@code analytics.mpp.broadcast.max_bytes} so the
     *     broadcast alternative cannot fit and CBO must shuffle. These tests are about the shuffle family,
     *     and the broadcast-preserve branch would otherwise legitimately take the plan.
     */
    private PlannerContext mppContext(Map<String, Integer> shardCounts, Map<String, Long> rowCounts, long maxBroadcastBytes) {
        Settings.Builder settings = Settings.builder().put("analytics.mpp.enabled", true);
        if (maxBroadcastBytes >= 0) {
            settings.put("analytics.mpp.broadcast.max_bytes", maxBroadcastBytes + "b");
        }
        ToLongFunction<String> rowCountLookup = name -> rowCounts.getOrDefault(name, PlannerContext.UNKNOWN_ROW_COUNT);
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        CapabilityRegistry registry = new CapabilityRegistry(
            List.of(new ShuffleAwareBackend(CLUSTER_DATA_NODES), LUCENE),
            fieldStorageFactory
        );
        return new PlannerContext(registry, clusterStateWithDataNodes(shardCounts), settings.build(), rowCountLookup, false);
    }

    private static ClusterState clusterStateWithDataNodes(Map<String, Integer> shardCounts) {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(state.metadata()).thenReturn(metadata);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(state.nodes()).thenReturn(nodes);
        Map<String, DiscoveryNode> dataNodes = new HashMap<>();
        for (int i = 0; i < CLUSTER_DATA_NODES; i++) {
            dataNodes.put("node-" + i, mock(DiscoveryNode.class));
        }
        when(nodes.getDataNodes()).thenReturn(dataNodes);

        for (Map.Entry<String, Integer> entry : shardCounts.entrySet()) {
            String indexName = entry.getKey();
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getIndex()).thenReturn(new Index(indexName, indexName + "-uuid"));
            when(indexMetadata.getNumberOfShards()).thenReturn(entry.getValue());
            MappingMetadata mappingMetadata = mock(MappingMetadata.class);
            // Every column any fixture scans has to be declared here, or the table-scan rule rejects the
            // plan before the pass under test ever runs.
            when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", fixtureFields()));
            when(indexMetadata.mapping()).thenReturn(mappingMetadata);
            when(indexMetadata.getSettings()).thenReturn(
                Settings.builder()
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene")
                    .build()
            );
            when(metadata.index(indexName)).thenReturn(indexMetadata);
        }
        return state;
    }

    /** The union of every column name the fixtures scan, all integers. */
    private static Map<String, Map<String, Object>> fixtureFields() {
        Map<String, Map<String, Object>> fields = new HashMap<>(intFields());
        for (String name : List.of("dim1_key", "dim2_key", "amount", "label1", "label2")) {
            fields.put(name, Map.of("type", "integer"));
        }
        return fields;
    }

    /** Declares shuffle parallelism so the enforcement pass has somewhere to distribute to. */
    private static class ShuffleAwareBackend extends MockDataFusionBackend {
        private final int parallelism;

        ShuffleAwareBackend(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public int defaultShuffleParallelism(ClusterState state) {
            return parallelism;
        }
    }
}
