/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Verifies the trait satisfies matrix that drives Volcano's exchange-insertion logic.
 * Each test case asserts a specific (produced, demanded) pair: the produced trait either
 * satisfies the demand (no exchange needed) or doesn't (Volcano inserts the corresponding
 * OpenSearch exchange via {@link OpenSearchDistributionTraitDef#convert}).
 *
 * <p>The satisfies semantics are load-bearing for cost-driven plan selection — they decide
 * which alternatives Volcano explores. The matrix here pins the contract so future trait
 * changes don't silently break the cost model's discrimination ability.
 */
public class OpenSearchDistributionSatisfiesTests extends OpenSearchTestCase {

    private OpenSearchDistributionTraitDef traitDef;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // PlannerContext is mocked — satisfies() doesn't consult it; we only need the trait def
        // to be a valid object so the OpenSearchDistribution instances can reference it.
        PlannerContext context = mock(PlannerContext.class);
        traitDef = new OpenSearchDistributionTraitDef(context);
    }

    // ── SINGLETON ────────────────────────────────────────────────────────

    public void testCoordSingletonSatisfiesItself() {
        assertTrue(traitDef.coordSingleton().satisfies(traitDef.coordSingleton()));
    }

    public void testShardSingletonSatisfiesNullLocalitySingleton() {
        // Used at root demand: anySingleton() accepts a 1-shard SHARD source without an ER.
        OpenSearchDistribution shardSingleton = traitDef.shardSingleton(/* tableId */ 1, /* shardCount */ 1);
        assertTrue(shardSingleton.satisfies(traitDef.anySingleton()));
    }

    public void testShardSingletonDoesNotSatisfyCoordSingleton() {
        // SHARD-localized data does NOT satisfy a COORDINATOR-localized demand even though
        // both are SINGLETON — the locality must match. This is what forces an ER above a
        // shard scan when a downstream operator demands COORD.
        OpenSearchDistribution shardSingleton = traitDef.shardSingleton(/* tableId */ 1, /* shardCount */ 1);
        assertFalse(shardSingleton.satisfies(traitDef.coordSingleton()));
    }

    // ── HASH ──────────────────────────────────────────────────────────────

    public void testHashSatisfiesItself() {
        OpenSearchDistribution h = traitDef.hash(List.of(0), 4);
        assertTrue(h.satisfies(h));
    }

    public void testHashOnMoreKeysDoesNotSatisfyDemandOnFewer() {
        // HASH(k1, k2) does NOT satisfy a demand for HASH(k1). The partition of a row is a function of the
        // WHOLE key tuple: the producer builds DataFusion's Partitioning::Hash(exprs, n) over every key and
        // partitions with DataFusion's own BatchPartitioner (create_hashes over all exprs, then % n — see
        // partition_batch_by_hash in api.rs). So two rows sharing k1 but differing in k2 land in DIFFERENT
        // partitions, and a consumer keyed on k1 alone (a join on k1, or GROUP BY k1) would see its groups
        // split across workers.
        //
        // This direction was once asserted to hold, on the reasoning that "rows colocated by hash(k1,k2) are
        // also colocated by hash(k1)" — true of sorting/clustering, false of hashing a tuple.
        OpenSearchDistribution onTwoKeys = traitDef.hash(List.of(0, 1), 4);
        OpenSearchDistribution demandOnOne = traitDef.hash(List.of(0), 4);
        assertFalse(onTwoKeys.satisfies(demandOnOne));
    }

    public void testHashOnFewerKeysSatisfiesDemandOnMore() {
        // HASH(k1) DOES satisfy a demand for HASH(k1, k2): every (k1, k2) group is a subset of a k1 group,
        // and the whole k1 group lives in one partition — so the finer grouping is whole there too. Coarser
        // satisfies finer, i.e. produced keys must be a SUBSET of demanded keys (the direction DataFusion's
        // own Partitioning::satisfy and Spark's HashPartitioning.satisfies(ClusteredDistribution) use).
        OpenSearchDistribution onOneKey = traitDef.hash(List.of(0), 4);
        OpenSearchDistribution demandOnTwo = traitDef.hash(List.of(0, 1), 4);
        assertTrue(onOneKey.satisfies(demandOnTwo));
    }

    public void testHashWithDifferentPartitionCountsDoNotSatisfy() {
        // HASH(k, 4) and HASH(k, 8) place rows in entirely different buckets — neither
        // satisfies the other regardless of keys. Cost-model implication: the converter
        // would insert a re-shuffle if the planner chose two different N values upstream.
        OpenSearchDistribution h4 = traitDef.hash(List.of(0), 4);
        OpenSearchDistribution h8 = traitDef.hash(List.of(0), 8);
        assertFalse(h4.satisfies(h8));
        assertFalse(h8.satisfies(h4));
    }

    public void testHashDoesNotSatisfySingleton() {
        // Hash-partitioned data does NOT satisfy a SINGLETON demand — it's spread across
        // workers. The converter must insert a gather (ExchangeReducer) above the shuffle
        // when a SINGLETON consumer reads HASH-partitioned input.
        OpenSearchDistribution h = traitDef.hash(List.of(0), 4);
        assertFalse(h.satisfies(traitDef.coordSingleton()));
    }

    // ── BROADCAST ─────────────────────────────────────────────────────────

    public void testBroadcastSatisfiesItself() {
        OpenSearchDistribution b = traitDef.broadcast(/* probeNodes */ 3);
        assertTrue(b.satisfies(b));
    }

    public void testBroadcastDoesNotSatisfyHash() {
        // Broadcast replicates everywhere; hash partitions by key. They're not interchangeable
        // — neither satisfies the other regardless of keys. (A hash join needs partitioned
        // inputs; a broadcast input is not partitioned, it's full-data.)
        OpenSearchDistribution b = traitDef.broadcast(/* probeNodes */ 3);
        OpenSearchDistribution h = traitDef.hash(List.of(0), 4);
        assertFalse(b.satisfies(h));
        assertFalse(h.satisfies(b));
    }

    public void testBroadcastDoesNotSatisfySingleton() {
        // Broadcast replicates to many nodes; SINGLETON expects all rows on one node.
        // The converter would need a gather to satisfy SINGLETON from BROADCAST.
        OpenSearchDistribution b = traitDef.broadcast(/* probeNodes */ 3);
        assertFalse(b.satisfies(traitDef.coordSingleton()));
    }

    // ── ANY ───────────────────────────────────────────────────────────────

    public void testAnyTraitIsSatisfiedByEverything() {
        OpenSearchDistribution any = traitDef.any();
        assertTrue(traitDef.coordSingleton().satisfies(any));
        assertTrue(traitDef.hash(List.of(0), 4).satisfies(any));
        assertTrue(traitDef.broadcast(3).satisfies(any));
    }
}
