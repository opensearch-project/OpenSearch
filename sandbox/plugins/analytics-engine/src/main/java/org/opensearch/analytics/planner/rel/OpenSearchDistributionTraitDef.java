/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.PlannerContext;

import java.util.List;

/**
 * Trait definition for OpenSearch distribution.
 *
 * <p>Called by Volcano via ExpandConversionRule when a distribution trait mismatch
 * is detected. Produces an {@link OpenSearchExchangeReducer} for SINGLETON demands and
 * an {@link OpenSearchShuffleExchange} for HASH_DISTRIBUTED demands. RANGE exchanges
 * are not implemented.
 *
 * <p>One instance per query — created by {@link PlannerContext}.
 *
 * @opensearch.internal
 */
public class OpenSearchDistributionTraitDef extends RelTraitDef<OpenSearchDistribution> {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchDistributionTraitDef.class);

    private final PlannerContext plannerContext;

    public OpenSearchDistributionTraitDef(PlannerContext plannerContext) {
        this.plannerContext = plannerContext;
    }

    // ---- Factory methods ----

    /** COORDINATOR + SINGLETON — data gathered to coord. Stamped on ER output, FINAL
     *  aggregate output, Join/Union output; demanded by cost gates on collated Sort /
     *  RexOver Project / Join / Union. */
    public OpenSearchDistribution coordSingleton() {
        return new OpenSearchDistribution(
            this,
            OpenSearchDistribution.Locality.COORDINATOR,
            RelDistribution.Type.SINGLETON,
            List.of(),
            null,
            null,
            null
        );
    }

    /** SINGLETON with null locality — accepts either SHARD+SINGLETON or COORDINATOR+SINGLETON.
     *  Used as the root demand: a 1-shard SHARD+SINGLETON subtree already satisfies, so no top
     *  ER is inserted; a multi-shard RANDOM subtree still mismatches and triggers ER insertion. */
    public OpenSearchDistribution anySingleton() {
        return new OpenSearchDistribution(this, null, RelDistribution.Type.SINGLETON, List.of(), null, null, null);
    }

    /** SHARD + SINGLETON — single-shard TableScan output. {@code shardCount=1} is what lets
     *  {@code UnionSplitRule} / {@code JoinSplitRule} skip inserting an ER when all inputs
     *  co-locate (same {@code tableId}, {@code shardCount=1}). */
    public OpenSearchDistribution shardSingleton(int tableId, int shardCount) {
        return new OpenSearchDistribution(
            this,
            OpenSearchDistribution.Locality.SHARD,
            RelDistribution.Type.SINGLETON,
            List.of(),
            tableId,
            shardCount,
            null
        );
    }

    /** SHARD + RANDOM — multi-shard TableScan output, and also the shape for shard-local
     *  Filter/Project/PARTIAL aggregate that pass through the scan's trait. */
    public OpenSearchDistribution shardRandom(int tableId, int shardCount) {
        return new OpenSearchDistribution(
            this,
            OpenSearchDistribution.Locality.SHARD,
            RelDistribution.Type.RANDOM_DISTRIBUTED,
            List.of(),
            tableId,
            shardCount,
            null
        );
    }

    /** ANY — universal sink; any distribution satisfies it. Used as {@link #getDefault}. */
    public OpenSearchDistribution any() {
        return new OpenSearchDistribution(this, null, RelDistribution.Type.ANY, List.of(), null, null, null);
    }

    /**
     * HASH_DISTRIBUTED demand on the given keys, partition count, and WORKER locality.
     * Used by {@code OpenSearchHashJoinSplitRule} to demand both join inputs deliver a
     * hash partitioning compatible with the join keys. The trait converter materializes
     * an {@link OpenSearchShuffleExchange} when the demand isn't already satisfied.
     */
    public OpenSearchDistribution hash(List<Integer> keys, int partitionCount) {
        return new OpenSearchDistribution(
            this,
            OpenSearchDistribution.Locality.WORKER,
            RelDistribution.Type.HASH_DISTRIBUTED,
            keys,
            null,
            null,
            partitionCount
        );
    }

    /**
     * HASH_DISTRIBUTED demand without a concrete partition count or locality — accepts any
     * upstream HASH on the same keys. Used in narrow contexts where the partition count is
     * not yet resolved (e.g. early planner exploration). Production-side rules should always
     * use {@link #hash(List, int)} with a concrete count.
     */
    public OpenSearchDistribution hashAny(List<Integer> keys) {
        return new OpenSearchDistribution(this, null, RelDistribution.Type.HASH_DISTRIBUTED, keys, null, null, null);
    }

    /**
     * BROADCAST_DISTRIBUTED + REPLICATED demand — full row set replicated to every probe-side
     * worker. {@code probeNodeEstimate} feeds the cost model: {@link OpenSearchBroadcastExchange}
     * scales its self-cost by this count. Used by {@code OpenSearchBroadcastJoinSplitRule} to
     * demand the build side be replicated; Volcano's trait converter materializes an
     * {@link OpenSearchBroadcastExchange} on any input not already so distributed.
     */
    public OpenSearchDistribution broadcast(int probeNodeEstimate) {
        // probeNodeEstimate piggybacks onto the partitionCount field — semantically distinct
        // from HASH's partitionCount but uses the same record slot for transit through the
        // trait. The broadcast exchange reads it from the trait at convert() time.
        return new OpenSearchDistribution(
            this,
            OpenSearchDistribution.Locality.REPLICATED,
            RelDistribution.Type.BROADCAST_DISTRIBUTED,
            List.of(),
            null,
            null,
            probeNodeEstimate
        );
    }

    /** Copies a distribution from another trait def — preserves all fields. */
    public OpenSearchDistribution from(OpenSearchDistribution other) {
        return new OpenSearchDistribution(
            this,
            other.getLocality(),
            other.getType(),
            other.getKeys(),
            other.getTableId(),
            other.getShardCount(),
            other.getPartitionCount()
        );
    }

    public OpenSearchDistribution fromType(RelDistribution.Type type, List<Integer> keys) {
        return new OpenSearchDistribution(this, null, type, keys, null, null, null);
    }

    // ---- RelTraitDef ----

    @Override
    public Class<OpenSearchDistribution> getTraitClass() {
        return OpenSearchDistribution.class;
    }

    @Override
    public String getSimpleName() {
        return "dist";
    }

    @Override
    public OpenSearchDistribution getDefault() {
        return any();
    }

    @Override
    public RelNode convert(RelOptPlanner planner, RelNode rel, OpenSearchDistribution toTrait, boolean allowInfiniteCostConverters) {
        OpenSearchDistribution fromTrait = rel.getTraitSet().getTrait(this);

        if (toTrait.getType() == RelDistribution.Type.ANY) {
            return rel;
        }

        if (fromTrait != null && fromTrait.satisfies(toTrait)) {
            return rel;
        }

        List<String> viableBackends = resolveViableBackendsFromRel(rel);

        LOGGER.info(
            "convert(): rel={}#{}, fromTrait={}, toTrait={}, backend={}",
            rel.getClass().getSimpleName(),
            rel.getId(),
            fromTrait,
            toTrait,
            viableBackends.getFirst()
        );

        CapabilityRegistry registry = plannerContext.getCapabilityRegistry();

        RelNode result;
        if (toTrait.getType() == RelDistribution.Type.SINGLETON) {
            List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(registry, viableBackends);
            // ER output always lives at the coordinator. Even if the demand is null-locality
            // (root demand), stamp COORDINATOR so the resulting subset is well-typed.
            OpenSearchDistribution stamp = toTrait.getLocality() == null ? coordSingleton() : toTrait;
            result = new OpenSearchExchangeReducer(rel.getCluster(), rel.getTraitSet().replace(stamp), rel, reduceViable);
        } else if (toTrait.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            // The split rule that issued the demand resolved the concrete partition count
            // (cluster setting → engine default) and embedded it in toTrait. A null demand
            // is rejected here — the caller must resolve a concrete count before requesting
            // a HASH conversion, otherwise the exchange is incoherent with downstream
            // ShuffleScan handlers that index per-partition buffers.
            if (toTrait.getPartitionCount() == null) {
                throw new IllegalStateException(
                    "HASH_DISTRIBUTED demand has null partitionCount; rule must resolve count "
                        + "via OpenSearchDistributionTraitDef.hash(keys, partitionCount). toTrait="
                        + toTrait
                );
            }
            // A shuffle producer must serialize + ship hash partitions, which only a backend that
            // declares DataTransferCapability(PRODUCER) can do. Prune scan-only backends (e.g.
            // Lucene, kept viable for a keyword scan under prefer_metadata_driver) so the producer
            // never lands on a driver that throws SHUFFLE_PRODUCER UOE at execution.
            List<String> shuffleViable = CapabilityResolutionUtils.filterByShuffleProducerCapability(registry, viableBackends);
            result = new OpenSearchShuffleExchange(
                rel.getCluster(),
                rel.getTraitSet().replace(toTrait),
                rel,
                toTrait.getKeys(),
                toTrait.getPartitionCount(),
                shuffleViable
            );
        } else if (toTrait.getType() == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
            // Broadcast demand: the build side gets replicated to every probe node. The split
            // rule resolved probeNodeEstimate (cluster setting → cluster's data-node count)
            // and stashed it in partitionCount. The exchange reads it for its cost function.
            if (toTrait.getPartitionCount() == null) {
                throw new IllegalStateException(
                    "BROADCAST_DISTRIBUTED demand has null probe-node estimate; rule must "
                        + "resolve via OpenSearchDistributionTraitDef.broadcast(probeNodes). toTrait="
                        + toTrait
                );
            }
            result = new OpenSearchBroadcastExchange(
                rel.getCluster(),
                rel.getTraitSet().replace(toTrait),
                rel,
                toTrait.getPartitionCount(),
                viableBackends
            );
        } else {
            // RANGE is still not implemented; never produced by analytics-engine rules today.
            throw new UnsupportedOperationException("RANGE exchange not yet implemented [toTrait=" + toTrait + "]");
        }

        return planner.register(result, rel);
    }

    @Override
    public boolean canConvert(RelOptPlanner planner, OpenSearchDistribution fromTrait, OpenSearchDistribution toTrait) {
        return true;
    }

    private static List<String> resolveViableBackendsFromRel(RelNode rel) {
        if (rel instanceof RelSubset subset) {
            rel = subset.getBestOrOriginal();
        }
        if (rel instanceof OpenSearchRelNode openSearchRel) {
            return openSearchRel.getViableBackends();
        }
        throw new IllegalStateException("Expected OpenSearchRelNode but got [" + rel.getClass().getSimpleName() + "#" + rel.getId() + "]");
    }
}
