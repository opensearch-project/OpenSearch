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
 * is detected. Produces an {@link OpenSearchExchangeReducer} for SINGLETON demands.
 * HASH/RANGE shuffle exchanges are not yet implemented.
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
            null
        );
    }

    /** SINGLETON with null locality — accepts either SHARD+SINGLETON or COORDINATOR+SINGLETON.
     *  Used as the root demand: a 1-shard SHARD+SINGLETON subtree already satisfies, so no top
     *  ER is inserted; a multi-shard RANDOM subtree still mismatches and triggers ER insertion. */
    public OpenSearchDistribution anySingleton() {
        return new OpenSearchDistribution(this, null, RelDistribution.Type.SINGLETON, List.of(), null, null);
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
            shardCount
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
            shardCount
        );
    }

    /** ANY — universal sink; any distribution satisfies it. Used as {@link #getDefault}. */
    public OpenSearchDistribution any() {
        return new OpenSearchDistribution(this, null, RelDistribution.Type.ANY, List.of(), null, null);
    }

    public OpenSearchDistribution hash(List<Integer> keys) {
        // HASH is currently only used as a downstream demand (future: shuffle exchanges);
        // we never stamp HASH on a scan, so locality/tableId/shardCount aren't meaningful.
        return new OpenSearchDistribution(this, null, RelDistribution.Type.HASH_DISTRIBUTED, keys, null, null);
    }

    /** Copies a distribution from another trait def — preserves all fields. */
    public OpenSearchDistribution from(OpenSearchDistribution other) {
        return new OpenSearchDistribution(
            this,
            other.getLocality(),
            other.getType(),
            other.getKeys(),
            other.getTableId(),
            other.getShardCount()
        );
    }

    public OpenSearchDistribution fromType(RelDistribution.Type type, List<Integer> keys) {
        return new OpenSearchDistribution(this, null, type, keys, null, null);
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
            // Partition count of 0 is a marker — the real partition count is set by the coordinator
            // when it rewrites the DAG for HASH_SHUFFLE dispatch (typically # of data nodes, from
            // plugins.analytics.shuffle_partitions). Trait conversion doesn't know the cluster
            // topology, only the key set.
            result = new OpenSearchShuffleExchange(
                rel.getCluster(),
                rel.getTraitSet().replace(toTrait),
                rel,
                toTrait.getKeys(),
                /* partitionCount */ 0,
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
