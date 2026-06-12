/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FieldStorageInfo;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates the matches() contract for the three join split rules per M2's strategy table:
 * <table>
 *   <caption>Strategy = f(joinType, mpp.enabled, shuffle.enabled)</caption>
 *   <tr><th>Case</th><th>Coord rule</th><th>Broadcast rule</th><th>Hash rule</th></tr>
 *   <tr><td>theta, any settings</td><td>fires</td><td>does NOT</td><td>does NOT</td></tr>
 *   <tr><td>equi, mpp.enabled=false</td><td>fires</td><td>does NOT</td><td>does NOT</td></tr>
 *   <tr><td>equi, mpp.enabled=true</td><td>does NOT</td><td>fires</td><td>fires (if shuffle.enabled)</td></tr>
 * </table>
 *
 * <p>The contract is what makes CBO produce exactly the legal alternative set for each case
 * — Volcano picks within the set by cost, but it can only pick from what the rules emit.
 */
public class SplitRuleContractTests extends BasePlannerRulesTests {

    private RelOptCluster volcanoCluster;
    private OpenSearchDistributionTraitDef traitDef;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        VolcanoPlanner volcano = new VolcanoPlanner();
        // Build a minimal trait def that doesn't require a full PlannerContext yet — we'll
        // construct contexts per-test with different settings.
        traitDef = new OpenSearchDistributionTraitDef(mock(PlannerContext.class));
        volcano.addRelTraitDef(org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
        volcano.addRelTraitDef(traitDef);
        volcanoCluster = RelOptCluster.create(volcano, new RexBuilder(typeFactory));
    }

    // ── theta joins ───────────────────────────────────────────────────────

    public void testThetaJoinFiresCoordRuleRegardlessOfSettings() {
        OpenSearchJoin theta = makeJoin(/* equi */ false);
        // Both mpp settings: theta routes coord-centric.
        assertTrue("Coord rule must match theta when mpp.disabled", coordRule(/* mppEnabled */ false).matches(stubCallFor(theta)));
        assertTrue("Coord rule must match theta when mpp.enabled", coordRule(/* mppEnabled */ true).matches(stubCallFor(theta)));
    }

    public void testThetaJoinDoesNotFireBroadcastRule() {
        OpenSearchJoin theta = makeJoin(/* equi */ false);
        assertFalse("Broadcast rule must not match theta", broadcastRule(/* mppEnabled */ true).matches(stubCallFor(theta)));
    }

    public void testThetaJoinDoesNotFireHashRule() {
        OpenSearchJoin theta = makeJoin(/* equi */ false);
        assertFalse("Hash rule must not match theta", hashRule(/* mppEnabled */ true).matches(stubCallFor(theta)));
    }

    // ── equi, mpp.enabled=false ───────────────────────────────────────────

    public void testEquiJoinMppDisabledFiresOnlyCoordRule() {
        OpenSearchJoin equi = makeJoin(/* equi */ true);
        assertTrue("Coord rule must match equi when mpp.disabled", coordRule(/* mppEnabled */ false).matches(stubCallFor(equi)));
        assertFalse("Broadcast rule must not match when mpp.disabled", broadcastRule(/* mppEnabled */ false).matches(stubCallFor(equi)));
        assertFalse("Hash rule must not match when mpp.disabled", hashRule(/* mppEnabled */ false).matches(stubCallFor(equi)));
    }

    // ── equi, mpp.enabled=true ────────────────────────────────────────────

    public void testEquiJoinMppEnabledFiresBroadcastAndHashRulesNotCoord() {
        OpenSearchJoin equi = makeJoin(/* equi */ true);
        assertFalse(
            "Coord rule must NOT match equi when mpp.enabled — coord-centric is not a competitor for MPP equi joins",
            coordRule(/* mppEnabled */ true).matches(stubCallFor(equi))
        );
        assertTrue("Broadcast rule must match equi when mpp.enabled", broadcastRule(/* mppEnabled */ true).matches(stubCallFor(equi)));
        assertTrue("Hash rule must match equi when mpp.enabled", hashRule(/* mppEnabled */ true).matches(stubCallFor(equi)));
    }

    // ── aggregate shuffle: shard-local input gate ──────────────────────────

    public void testAggregateShuffleFiresOnShardLocalChild() {
        // GROUP BY over a SHARD-distributed scan: the canonical shuffle-eligible shape. The
        // shard-local gate must let it through so the hash-shuffle alternative is registered.
        OpenSearchAggregate agg = makeGroupedAggregate(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        assertTrue(
            "Aggregate shuffle rule must match a GROUP BY over a SHARD-local scan when mpp.enabled",
            aggShuffleRule(/* mppEnabled */ true).matches(stubCallFor1(agg, agg.getInput()))
        );
    }

    public void testAggregateShuffleDoesNotFireWhenSubToggleDisabled() {
        // Same shard-local shape as testAggregateShuffleFiresOnShardLocalChild, but with
        // analytics.mpp.shuffle.aggregate.enabled=false (mpp.enabled still true). The per-strategy
        // sub-toggle must keep the rule out so the aggregate routes through the coord-centric path,
        // exactly as if MPP were off — without disabling MPP joins.
        OpenSearchAggregate agg = makeGroupedAggregate(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        OpenSearchAggregateShuffleSplitRule rule = new OpenSearchAggregateShuffleSplitRule(
            makeContext(/* mppEnabled */ true, /* aggShuffleEnabled */ false)
        );
        assertFalse(
            "Aggregate shuffle rule must NOT match when analytics.mpp.shuffle.aggregate.enabled=false",
            rule.matches(stubCallFor1(agg, agg.getInput()))
        );
    }

    public void testAggregateShuffleDoesNotFireOnCoordinatorChild() {
        // GROUP BY whose child is already gathered to COORDINATOR (e.g. `join … | stats by …`,
        // where the join output is coordinator-local). A scan exists beneath the child, so the
        // old structural "contains a table scan" gate would have wrongly fired here. The trait
        // gate must reject it: cutting a shuffle over a non-shard fragment mis-targets shards
        // and drops producer output. Regression guard for the OpenSearchAggregateShuffleSplitRule
        // shard-local gate.
        OpenSearchAggregate agg = makeGroupedAggregate(traitDef.coordSingleton());
        assertFalse(
            "Aggregate shuffle rule must NOT match a GROUP BY over a COORDINATOR-gathered child",
            aggShuffleRule(/* mppEnabled */ true).matches(stubCallFor1(agg, agg.getInput()))
        );
    }

    public void testAggregateShuffleDoesNotFireOnWorkerChild() {
        // GROUP BY whose child is a post-shuffle WORKER fragment — also non-shard-local. The gate
        // must reject it for the same reason (only shard-fragment execution materializes the
        // partitioned shuffle sink).
        OpenSearchAggregate agg = makeGroupedAggregate(scanWith(traitDef.hash(List.of(0), /* partitionCount */ 3)));
        assertFalse(
            "Aggregate shuffle rule must NOT match a GROUP BY over a WORKER+HASH child",
            aggShuffleRule(/* mppEnabled */ true).matches(stubCallFor1(agg, agg.getInput()))
        );
    }

    public void testAggregateShuffleDoesNotFireOnBroadcastJoinChild() {
        // GROUP BY directly above a BROADCAST-JOIN probe fragment: Join(BroadcastExchange(build),
        // probeScan). The broadcast split rule copies the probe's SHARD trait onto the join, so the
        // join output carries Locality.SHARD even though its subtree holds a broadcast exchange and
        // two scans. A top-level-locality-only gate would wrongly fire here, but the producer is not
        // a leaf shard scan: DAGBuilder.cutShuffle would cut the nested broadcast exchange into a
        // grandchild stage (forcing targetResolver=null) and HashShuffleAggregateDispatch never
        // injects the broadcast build data — the join inside the producer would hang. The
        // leaf-shard-producer gate must reject it (exchange present AND two scans).
        OpenSearchAggregate agg = makeGroupedAggregate(makeBroadcastProbeJoin());
        assertFalse(
            "Aggregate shuffle rule must NOT match a GROUP BY over a broadcast-join probe fragment",
            aggShuffleRule(/* mppEnabled */ true).matches(stubCallFor1(agg, agg.getInput()))
        );
    }

    public void testAggregateShuffleDoesNotFireOnTwoScanShardChild() {
        // GROUP BY above a co-located shard join — two scans, SHARD locality, NO exchange. Still
        // not a leaf producer: ShardTargetResolver targets only the first scan it finds, dropping
        // the other side's rows (the one-scan-per-stage constraint). The gate's scan-count check
        // must reject it even though there's no exchange.
        OpenSearchAggregate agg = makeGroupedAggregate(makeColocatedShardJoin());
        assertFalse(
            "Aggregate shuffle rule must NOT match a GROUP BY over a two-scan shard join",
            aggShuffleRule(/* mppEnabled */ true).matches(stubCallFor1(agg, agg.getInput()))
        );
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private OpenSearchAggregateShuffleSplitRule aggShuffleRule(boolean mppEnabled) {
        return new OpenSearchAggregateShuffleSplitRule(makeContext(mppEnabled));
    }

    /** A {@code mode=SINGLE} grouped aggregate (GROUP BY col 0, SUM(col 1)) over a single
     *  SHARD+RANDOM scan — convenience overload for the common case. */
    private OpenSearchAggregate makeGroupedAggregate(OpenSearchDistribution childDist) {
        return makeGroupedAggregate(scanWith(childDist));
    }

    /** A {@code mode=SINGLE} grouped aggregate (GROUP BY col 0, SUM(col 1)) over an arbitrary
     *  child RelNode. The child's shape/locality is the knob the leaf-shard-producer gate reads;
     *  the aggregate's OWN trait is kept at COORDINATOR+SINGLETON (never WORKER+HASH) so the rule's
     *  {@code aggregateAlreadyResolvedAsHash} guard never short-circuits — that keeps these tests
     *  exercising the input gate specifically, not the re-fire guard. */
    private OpenSearchAggregate makeGroupedAggregate(RelNode child) {
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            child,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total_size"
        );
        RelTraitSet aggTraits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(traitDef.coordSingleton());
        return new OpenSearchAggregate(
            volcanoCluster,
            aggTraits,
            child,
            ImmutableBitSet.of(0),
            List.of(ImmutableBitSet.of(0)),
            List.of(sum),
            AggregateMode.SINGLE,
            List.of("mock-parquet"),
            Map.of()
        );
    }

    /** A broadcast-join probe fragment: {@code Join(OpenSearchBroadcastExchange(buildScan),
     *  probeScan)} carrying the probe's SHARD trait — the exact shape
     *  {@code OpenSearchBroadcastJoinSplitRule#emitBroadcastAlternative} produces (probe trait
     *  copied onto the join via {@code distTraitDef.from(probeDist)}). */
    private OpenSearchJoin makeBroadcastProbeJoin() {
        OpenSearchDistribution probeDist = traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3);
        OpenSearchTableScan probe = scanWith(probeDist);
        OpenSearchTableScan build = scanWith(traitDef.shardRandom(/* tableId */ 2, /* shardCount */ 3));
        RelTraitSet broadcastTraits = build.getTraitSet().replace(traitDef.broadcast(/* probeNodeEstimate */ 3));
        OpenSearchBroadcastExchange broadcastBuild = new OpenSearchBroadcastExchange(
            volcanoCluster,
            broadcastTraits,
            build,
            /* probeNodeEstimate */ 3,
            List.of("mock-parquet")
        );
        int buildCols = broadcastBuild.getRowType().getFieldCount();
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), buildCols)
        );
        // Join output sits at the probe's SHARD trait — the broadcast rule's join-locality copy.
        RelTraitSet joinTraits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(traitDef.from(probeDist));
        return new OpenSearchJoin(volcanoCluster, joinTraits, broadcastBuild, probe, condition, JoinRelType.INNER, List.of("mock-parquet"));
    }

    /** A co-located shard join: {@code Join(leftScan, rightScan)} at SHARD+SINGLETON locality with
     *  no exchange — two scans in one SHARD-local fragment. */
    private OpenSearchJoin makeColocatedShardJoin() {
        OpenSearchTableScan left = scanWith(traitDef.shardSingleton(/* tableId */ 1, /* shardCount */ 1));
        OpenSearchTableScan right = scanWith(traitDef.shardSingleton(/* tableId */ 1, /* shardCount */ 1));
        int leftCols = left.getRowType().getFieldCount();
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), leftCols)
        );
        RelTraitSet joinTraits = RelTraitSet.createEmpty()
            .plus(OpenSearchConvention.INSTANCE)
            .plus(traitDef.shardSingleton(/* tableId */ 1, /* shardCount */ 1));
        return new OpenSearchJoin(volcanoCluster, joinTraits, left, right, condition, JoinRelType.INNER, List.of("mock-parquet"));
    }

    private OpenSearchJoin makeJoin(boolean equi) {
        // Build two SHARD+RANDOM scans (matches what OpenSearchTableScanRule produces for
        // multi-shard tables — the broadcast rule's matches() requires this trait).
        OpenSearchTableScan left = scanWith(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        OpenSearchTableScan right = scanWith(traitDef.shardRandom(/* tableId */ 2, /* shardCount */ 3));
        int leftCols = left.getRowType().getFieldCount();
        RexNode condition = equi
            ? rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), leftCols)
            )
            : rexBuilder.makeCall(
                SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), leftCols)
            );
        RelTraitSet joinTraits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(traitDef.coordSingleton());
        return new OpenSearchJoin(volcanoCluster, joinTraits, left, right, condition, JoinRelType.INNER, List.of("mock-parquet"));
    }

    private OpenSearchTableScan scanWith(OpenSearchDistribution dist) {
        RelTraitSet traits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(dist);
        return new OpenSearchTableScan(
            volcanoCluster,
            traits,
            mockTable("test_index", "status", "size"),
            List.of("mock-parquet"),
            List.<FieldStorageInfo>of()
        );
    }

    private OpenSearchJoinSplitRule coordRule(boolean mppEnabled) {
        return new OpenSearchJoinSplitRule(makeContext(mppEnabled));
    }

    private OpenSearchBroadcastJoinSplitRule broadcastRule(boolean mppEnabled) {
        return new OpenSearchBroadcastJoinSplitRule(makeContext(mppEnabled));
    }

    private OpenSearchHashJoinSplitRule hashRule(boolean mppEnabled) {
        return new OpenSearchHashJoinSplitRule(makeContext(mppEnabled));
    }

    private PlannerContext makeContext(boolean mppEnabled) {
        return makeContext(mppEnabled, /* aggShuffleEnabled */ true);
    }

    private PlannerContext makeContext(boolean mppEnabled, boolean aggShuffleEnabled) {
        Settings settings = Settings.builder()
            .put("analytics.mpp.enabled", mppEnabled)
            .put("analytics.mpp.shuffle.aggregate.enabled", aggShuffleEnabled)
            .build();
        return new PlannerContext(
            new CapabilityRegistry(List.<AnalyticsSearchBackendPlugin>of(DATAFUSION, LUCENE), FieldStorageResolver::new),
            mockClusterState(),
            settings
        );
    }

    /**
     * Tiny ClusterState mock — just enough to satisfy the broadcast rule's
     * resolveProbeNodeEstimate() defensive-check path. Returns a 3-data-node cluster so
     * probeNodes > 1 and the rule actually fires (rather than bailing on the 1-node fallback).
     */
    private static ClusterState mockClusterState() {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(state.metadata()).thenReturn(metadata);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(state.nodes()).thenReturn(nodes);
        Map<String, DiscoveryNode> dataNodes = new HashMap<>();
        dataNodes.put("node-1", mock(DiscoveryNode.class));
        dataNodes.put("node-2", mock(DiscoveryNode.class));
        dataNodes.put("node-3", mock(DiscoveryNode.class));
        when(nodes.getDataNodes()).thenReturn(dataNodes);

        // Stub a test_index with 3 shards so the table-scan rule could resolve it if invoked
        // (the rules under test don't go through table-scan resolution, but keeping this
        // makes the mock more robust if a future rule path needs it).
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getIndex()).thenReturn(new Index("test_index", "test_index-uuid"));
        when(indexMetadata.getNumberOfShards()).thenReturn(3);
        MappingMetadata mappingMetadata = mock(MappingMetadata.class);
        when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", Map.of()));
        when(indexMetadata.mapping()).thenReturn(mappingMetadata);
        when(indexMetadata.getSettings()).thenReturn(
            Settings.builder()
                .put("index.composite.primary_data_format", "parquet")
                .putList("index.composite.secondary_data_formats", "lucene")
                .build()
        );
        when(metadata.index("test_index")).thenReturn(indexMetadata);

        return state;
    }

    /** RelOptRuleCall is awkward to construct directly (package-private constructor in
     *  VolcanoRuleCall). All matches() calls need is `call.rel(0)`, so a Mockito stub is
     *  enough. */
    private static org.apache.calcite.plan.RelOptRuleCall stubCallFor(RelNode rel) {
        org.apache.calcite.plan.RelOptRuleCall call = mock(org.apache.calcite.plan.RelOptRuleCall.class);
        when(call.<RelNode>rel(0)).thenReturn(rel);
        return call;
    }

    /** Two-operand variant for rules whose {@code matches()} reads both {@code call.rel(0)} (the
     *  matched node) and {@code call.rel(1)} (its child) — e.g. the aggregate shuffle rule's
     *  shard-local input gate. */
    private static org.apache.calcite.plan.RelOptRuleCall stubCallFor1(RelNode rel0, RelNode rel1) {
        org.apache.calcite.plan.RelOptRuleCall call = mock(org.apache.calcite.plan.RelOptRuleCall.class);
        when(call.<RelNode>rel(0)).thenReturn(rel0);
        when(call.<RelNode>rel(1)).thenReturn(rel1);
        return call;
    }
}
