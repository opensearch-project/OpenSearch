/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Verifies {@link OpenSearchDistributionTraitDef#convert} materializes the right exchange
 * RelNode for each demanded trait. This is the cost-driven planner's primary mechanism for
 * inserting transport between operators with mismatched localities — when a downstream
 * operator demands a trait the input doesn't satisfy, Volcano calls {@code convert(...)} and
 * stores the result as an alternative the cost model evaluates against the no-conversion
 * path.
 *
 * <p>Each test asserts the produced RelNode is the right exchange type and carries the
 * right metadata (partition keys, partition count, probe-node estimate). A bug here would
 * manifest as wrong-shaped plans or as the converter throwing where it shouldn't.
 */
public class OpenSearchDistributionTraitDefConvertTests extends BasePlannerRulesTests {

    private VolcanoPlanner volcano;
    private RelOptCluster volcanoCluster;
    private PlannerContext context;
    private OpenSearchDistributionTraitDef traitDef;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // We need a context (for the trait def) plus a *fresh* VolcanoPlanner whose cluster is
        // distinct from the BasePlannerRulesTests HEP cluster. Calcite enforces "a RelNode
        // belongs to exactly one planner" — building scans against the test's cluster and
        // then asking convert() to register them on a different planner would trip that
        // invariant. So we build the scan against the volcano-cluster directly.
        context = buildContext("parquet", /* shardCount */ 3, intFields());
        traitDef = context.getDistributionTraitDef();
        volcano = new VolcanoPlanner();
        volcano.addRelTraitDef(org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
        volcano.addRelTraitDef(traitDef);
        volcanoCluster = RelOptCluster.create(volcano, new RexBuilder(typeFactory));
    }

    public void testConvertSingletonInsertsExchangeReducer() {
        OpenSearchTableScan scan = makeShardScan(/* shardCount */ 3);
        RelNode converted = unwrapSubset(traitDef.convert(volcano, scan, traitDef.coordSingleton(), false));

        assertTrue(
            "Converting SHARD+RANDOM to COORDINATOR+SINGLETON must insert OpenSearchExchangeReducer, got "
                + converted.getClass().getSimpleName(),
            converted instanceof OpenSearchExchangeReducer
        );
    }

    public void testConvertHashInsertsShuffleExchange() {
        OpenSearchTableScan scan = makeShardScan(/* shardCount */ 3);
        OpenSearchDistribution hashTrait = traitDef.hash(List.of(0), /* partitionCount */ 4);
        RelNode converted = unwrapSubset(traitDef.convert(volcano, scan, hashTrait, false));

        assertTrue(
            "Converting SHARD+RANDOM to HASH must insert OpenSearchShuffleExchange, got " + converted.getClass().getSimpleName(),
            converted instanceof OpenSearchShuffleExchange
        );
        OpenSearchShuffleExchange shuffle = (OpenSearchShuffleExchange) converted;
        assertEquals("hashKeys must propagate from the demanded trait", List.of(0), shuffle.getHashKeys());
        assertEquals("partitionCount must propagate from the demanded trait", 4, shuffle.getPartitionCount());
    }

    public void testConvertBroadcastInsertsBroadcastExchange() {
        OpenSearchTableScan scan = makeShardScan(/* shardCount */ 3);
        OpenSearchDistribution broadcastTrait = traitDef.broadcast(/* probeNodes */ 3);
        RelNode converted = unwrapSubset(traitDef.convert(volcano, scan, broadcastTrait, false));

        assertTrue(
            "Converting SHARD+RANDOM to BROADCAST must insert OpenSearchBroadcastExchange, got " + converted.getClass().getSimpleName(),
            converted instanceof OpenSearchBroadcastExchange
        );
        OpenSearchBroadcastExchange broadcast = (OpenSearchBroadcastExchange) converted;
        assertEquals("probeNodeEstimate must propagate from the demanded trait", 3, broadcast.getProbeNodeEstimate());
    }

    public void testConvertReturnsInputUnchangedWhenAlreadySatisfied() {
        OpenSearchTableScan scan = makeShardScan(/* shardCount */ 1);
        // 1-shard scan is already SHARD+SINGLETON which satisfies anySingleton() (null
        // locality) — no exchange should be inserted.
        RelNode converted = traitDef.convert(volcano, scan, traitDef.anySingleton(), false);
        assertSame("Already-satisfying input must be returned unchanged", scan, converted);
    }

    /** Volcano's {@code planner.register(...)} wraps registered nodes in {@link RelSubset};
     *  unwrap to inspect the underlying RelNode. */
    private static RelNode unwrapSubset(RelNode node) {
        if (node instanceof RelSubset subset) {
            RelNode best = subset.getBestOrOriginal();
            return best != null ? best : node;
        }
        return node;
    }

    public void testConvertHashWithNullPartitionCountThrows() {
        OpenSearchTableScan scan = makeShardScan(/* shardCount */ 3);
        // hashAny() carries null partitionCount — converter rejects it because the produced
        // ShuffleExchange would have no concrete count to size partitions against.
        OpenSearchDistribution hashAny = traitDef.hashAny(List.of(0));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> traitDef.convert(volcano, scan, hashAny, false));
        assertTrue("Error message should mention partitionCount; got: " + ex.getMessage(), ex.getMessage().contains("partitionCount"));
    }

    /**
     * Builds an {@link OpenSearchTableScan} attached to {@code volcanoCluster} (rather than the
     * inherited HEP test cluster). This avoids the "RelNode belongs to a different planner"
     * AssertionError that would fire if {@code convert(...)} tried to register a HEP-cluster
     * scan against the test's VolcanoPlanner. The scan is otherwise equivalent to what
     * {@code OpenSearchTableScanRule} produces in the real planner pipeline.
     */
    private OpenSearchTableScan makeShardScan(int shardCount) {
        RelOptTable mockTable = mockTable("test_index", "status", "size");
        // Wrap the mock table to carry the bare index name (matches what OpenSearchTableScanRule
        // does in production via its private IndexNameTable).
        RelOptTable indexNameTable = new TestIndexNameTable(mockTable, "test_index");
        return OpenSearchTableScan.create(
            volcanoCluster,
            indexNameTable,
            List.of("mock-parquet"),
            List.<FieldStorageInfo>of(),
            shardCount,
            traitDef
        );
    }

    /** Matches the production {@code IndexNameTable} from {@code OpenSearchTableScanRule}.
     *  We can't directly reuse it (private inner class), so we replicate the minimum needed. */
    private static class TestIndexNameTable extends org.apache.calcite.plan.RelOptAbstractTable {
        TestIndexNameTable(RelOptTable delegate, String indexName) {
            super(delegate.getRelOptSchema(), indexName, delegate.getRowType());
        }
    }
}
