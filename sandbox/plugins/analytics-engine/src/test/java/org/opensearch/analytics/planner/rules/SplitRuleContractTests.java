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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
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

    // ── broadcast build-side byte gate ─────────────────────────────────────

    // The build side here is a scan over two INTEGER columns (status, size) → estimated row width
    // is 4 + 4 = 8 bytes/row, derived from the RelDataType (not RelMetadataQuery.getAverageRowSize,
    // which returns null for OpenSearch RelNodes — the bug this gate works around). The gate uses
    // the OPTIMISTIC selectivity-aware expected count (getRowCount), Spark-style; the runtime cap +
    // broadcast→shuffle retry corrects builds whose filter under-delivers.

    public void testBroadcastGateAdmitsBuildWithinCap() {
        RelNode build = scanWith(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        when(mq.getRowCount(build)).thenReturn(1_000.0); // 1000 × 8B = ~8KB
        assertTrue(
            "build estimated at ~8KB must be admitted under a 32MB cap",
            OpenSearchBroadcastJoinSplitRule.buildSideFitsBroadcast(build, mq, 32L * 1024 * 1024)
        );
    }

    public void testBroadcastGateSuppressesBuildOverCap() {
        RelNode build = scanWith(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        // 60M rows × 8 bytes = 480MB — TPC-H q18-class build side, ~15x the 32MB default cap.
        when(mq.getRowCount(build)).thenReturn(60_000_000.0);
        assertFalse(
            "build estimated at 480MB must be suppressed under a 32MB cap so CBO falls back to shuffle/coord",
            OpenSearchBroadcastJoinSplitRule.buildSideFitsBroadcast(build, mq, 32L * 1024 * 1024)
        );
    }

    public void testBroadcastGateTrustsOptimisticFilteredEstimate() {
        // Spark-style optimism: the gate uses the EXPECTED (post-filter) count, not a selectivity-
        // ignoring upper bound. A selective filter (e.g. q17's part WHERE p_brand=… AND p_container=…)
        // drops the expected count below the cap → broadcast is admitted even though the unfiltered
        // table is huge. If the filter under-delivers at runtime, the broadcast→shuffle retry corrects
        // it. getMaxRowCount is deliberately NOT consulted.
        RelNode build = scanWith(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        when(mq.getRowCount(build)).thenReturn(1_000.0); // optimistic post-filter: ~8KB
        assertTrue(
            "filtered build estimated at ~8KB must be admitted (optimistic), runtime retry is the backstop",
            OpenSearchBroadcastJoinSplitRule.buildSideFitsBroadcast(build, mq, 32L * 1024 * 1024)
        );
    }

    public void testBroadcastGateAdmitsWhenRowCountUnknown() {
        RelNode build = scanWith(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        // getRowCount null (no seeded stats). Missing stats must NOT suppress broadcast; the runtime
        // cap + retry stay the safety net.
        when(mq.getRowCount(build)).thenReturn(null);
        assertTrue(
            "unknown row count must admit broadcast (runtime backstop)",
            OpenSearchBroadcastJoinSplitRule.buildSideFitsBroadcast(build, mq, 32L * 1024 * 1024)
        );
    }

    public void testBroadcastGateDisabledWhenCapNonPositive() {
        RelNode build = scanWith(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        when(mq.getRowCount(build)).thenReturn(60_000_000.0);
        assertTrue(
            "a non-positive cap means 'no limit' — always admit",
            OpenSearchBroadcastJoinSplitRule.buildSideFitsBroadcast(build, mq, 0L)
        );
    }

    public void testBroadcastGateEstimatesUnspecifiedPrecisionVarchar() {
        // A bare VARCHAR column (OpenSearch keyword/text) has precision = PRECISION_NOT_SPECIFIED (-1).
        // A naive width formula (precision × bytesPerChar) would go negative and zero the estimate,
        // silently no-op-ing the gate for string-heavy builds. The width must fall back to a positive
        // cap so a large-row-count build is still suppressed.
        RelNode build = scanWithVarcharColumn(traitDef.shardRandom(/* tableId */ 1, /* shardCount */ 3));
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        when(mq.getRowCount(build)).thenReturn(60_000_000.0); // 60M rows × ~100B/row ≫ 32MB
        assertFalse(
            "a 60M-row VARCHAR build must be suppressed — unspecified precision must not zero the width",
            OpenSearchBroadcastJoinSplitRule.buildSideFitsBroadcast(build, mq, 32L * 1024 * 1024)
        );
    }

    // ── helpers ───────────────────────────────────────────────────────────

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

    /** A scan over a single bare VARCHAR column (precision unspecified = PRECISION_NOT_SPECIFIED),
     *  mirroring an OpenSearch keyword/text field — the case where a naive width formula goes negative. */
    private OpenSearchTableScan scanWithVarcharColumn(OpenSearchDistribution dist) {
        RelTraitSet traits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(dist);
        return new OpenSearchTableScan(
            volcanoCluster,
            traits,
            mockTable("text_index", new String[] { "body" }, new SqlTypeName[] { SqlTypeName.VARCHAR }),
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
        Settings settings = Settings.builder().put("analytics.mpp.enabled", mppEnabled).build();
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
}
