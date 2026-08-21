/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Distribution trait for OpenSearch Analytics operators.
 *
 * <p>Carries five pieces of information:
 * <ul>
 *   <li>{@link Locality} — where the rows physically live. {@code SHARD} means data sits
 *       at its storage location (TableScan output, shard-local Filter/Project/PARTIAL agg).
 *       {@code COORDINATOR} means data has been gathered to the coord (ER output, FINAL
 *       aggregate output, Join/Union output). {@code WORKER} means data has been
 *       hash-shuffled and is being processed on data-node workers (post-shuffle Join/Agg).</li>
 *   <li>{@link Type} — Calcite's partitioning model (SINGLETON / RANDOM / HASH / ANY).</li>
 *   <li>{@code tableId} — for {@code SHARD} distributions, identifies the source table so
 *       the planner can reason about when two SHARD streams belong to the same physical
 *       layout (future: co-located joins). Null on COORDINATOR / WORKER distributions.</li>
 *   <li>{@code shardCount} — for {@code SHARD} distributions only.</li>
 *   <li>{@code partitionCount} — for {@code HASH_DISTRIBUTED} distributions only; the number
 *       of post-shuffle output partitions. Two HASH distributions with the same keys but
 *       different partition counts are <em>not</em> compatible (data isn't physically aligned).
 *       Null for non-HASH distributions and for HASH demands that don't yet know the count
 *       (caller resolves to a concrete number before stamping on a producer).</li>
 * </ul>
 *
 * <p><b>Satisfies semantics.</b> For {@code SINGLETON}, the same locality satisfies; a
 * SINGLETON demand with null locality accepts either SHARD or COORDINATOR (used by callers
 * that don't care whether data is shard-local or gathered). For {@code HASH_DISTRIBUTED},
 * the produced trait satisfies the demand iff the produced keys are a SUBSET of the demanded
 * keys (coarser satisfies finer — partitioning on {@code hash(k1)} keeps every {@code (k1,k2)}
 * group whole, but not the reverse) AND the partition counts match exactly.
 *
 * @opensearch.internal
 */
@SuppressWarnings("unchecked")
public class OpenSearchDistribution implements RelDistribution {

    /** Where the rows physically live. */
    public enum Locality {
        /** Data sits at shard storage nodes. */
        SHARD,
        /** Data has been gathered to the coordinator. */
        COORDINATOR,
        /** Data has been hash-shuffled to data-node workers (post-shuffle execution locale).
         *  Each worker holds 1/N of the rows, partitioned by hash key. */
        WORKER,
        /** Data has been replicated to every probe-side worker — the broadcast distribution.
         *  Every worker holds the full row set (small build side). Distinct from {@link #WORKER}
         *  because the row set per node is different in shape: replicated has all rows on
         *  every node; worker+hash has 1/N of rows on each. */
        REPLICATED
    }

    private final OpenSearchDistributionTraitDef traitDef;
    private final Locality locality;
    private final Type type;
    private final List<Integer> keys;
    private final Integer tableId;
    private final Integer shardCount;
    private final Integer partitionCount;
    /**
     * True when this partitioning was MATERIALIZED by an exchange (a shuffle actually moved the rows),
     * false when it was merely DERIVED by an operator claiming its output happens to be so partitioned.
     *
     * <p>This is what makes the trait TIER-AWARE. The hash-shuffle transport delivers exactly two named
     * inputs per worker, so each distributed join input must arrive as its OWN producer stream: a lower
     * join's derived {@code HASH(k,N)} must NOT satisfy a parent join's {@code HASH(k,N)} demand, or
     * Volcano would reuse it in place and collapse N joins into one fragment with N shuffle leaves —
     * which the transport cannot run.
     *
     * <p>Previously the same effect was obtained accidentally: the enforcement pass tracked the real
     * distribution in a side record while the rel's traitSet kept CBO's stale {@code coordSingleton}, so
     * {@code satisfies()} happened to return false and an inter-tier shuffle got inserted
     * ({@code DistributionEnforcementPass}:464-467 documents the dependency). Encoding the tier in the
     * trait replaces that accident with a stated rule, which is the prerequisite for making traits
     * authoritative and retiring the pass's forced {@code buildReducer} calls.
     */
    private final boolean exchangeMaterialized;

    OpenSearchDistribution(
        OpenSearchDistributionTraitDef traitDef,
        Locality locality,
        Type type,
        List<Integer> keys,
        Integer tableId,
        Integer shardCount,
        Integer partitionCount
    ) {
        this(traitDef, locality, type, keys, tableId, shardCount, partitionCount, false);
    }

    OpenSearchDistribution(
        OpenSearchDistributionTraitDef traitDef,
        Locality locality,
        Type type,
        List<Integer> keys,
        Integer tableId,
        Integer shardCount,
        Integer partitionCount,
        boolean exchangeMaterialized
    ) {
        this.traitDef = traitDef;
        this.locality = locality;
        this.type = type;
        this.keys = keys;
        this.tableId = tableId;
        this.shardCount = shardCount;
        this.partitionCount = partitionCount;
        this.exchangeMaterialized = exchangeMaterialized;
    }

    /** See {@link #exchangeMaterialized}. */
    public boolean isExchangeMaterialized() {
        return exchangeMaterialized;
    }

    /** This distribution with the exchange-materialized flag set — used by the shuffle exchange. */
    public OpenSearchDistribution asExchangeMaterialized() {
        return exchangeMaterialized
            ? this
            : new OpenSearchDistribution(traitDef, locality, type, keys, tableId, shardCount, partitionCount, true);
    }

    public Locality getLocality() {
        return locality;
    }

    public Integer getTableId() {
        return tableId;
    }

    public Integer getShardCount() {
        return shardCount;
    }

    public Integer getPartitionCount() {
        return partitionCount;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public List<Integer> getKeys() {
        return keys;
    }

    @Override
    public RelTraitDef<? extends RelTrait> getTraitDef() {
        return traitDef;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        if (!(trait instanceof OpenSearchDistribution other)) {
            return false;
        }
        if (other.type == Type.ANY) {
            return true;
        }
        if (this.type != other.type) {
            return false;
        }
        if (this.type == Type.SINGLETON) {
            if (!this.keys.equals(other.keys)) return false;
            // SINGLETON demand with null locality accepts either SHARD or COORDINATOR.
            // Otherwise the locality must match exactly — this is what prevents an ER over a
            // SHARD scan from dedup-ing into a COORDINATOR-producing sibling subset, and keeps
            // the two classes of "on one node" distinguishable during plan search.
            if (other.locality == null) return true;
            return this.locality == other.locality;
        }
        if (this.type == Type.HASH_DISTRIBUTED) {
            // PRODUCED keys must be a SUBSET of DEMANDED keys (Spark's
            // HashPartitioning.satisfies(ClusteredDistribution) direction). Partitioning on hash(k1) keeps
            // every (k1,k2) group whole, so HASH[k1] satisfies a demand for HASH[k1,k2] — coarser satisfies
            // finer. NOT the reverse: rows sharing k1 but differing in k2 hash to DIFFERENT buckets, so
            // HASH[k1,k2] colocates nothing a consumer keyed on k1 alone needs.
            //
            // This direction was inverted (`isPrefix(demanded, produced)`, justified as "rows colocated by
            // hash(k1,k2) are also colocated by hash(k1)", which is false of hashing a tuple). It was masked:
            // a JOIN is a tier boundary by default and forces its shuffle without consulting satisfies(), and
            // no unary operator demanded HASH until OpenSearchAggregate began asking for HASH(groupKeys) — at
            // which point the aggregate would have ridden a child partitioned on [k1,k2] while grouping by
            // [k1], aggregating groups split across partitions.
            if (!other.keys.containsAll(this.keys)) return false;
            // Partition counts must match exactly: HASH(k, 4) and HASH(k, 8) place rows in
            // entirely different buckets, so neither satisfies the other regardless of keys.
            // A null demanded partitionCount accepts any (used while the rule is still
            // resolving the count); a concrete demand requires equality.
            if (other.partitionCount != null && !other.partitionCount.equals(this.partitionCount)) {
                return false;
            }
            // TIER AWARENESS: a demand for a MATERIALIZED partitioning is not met by a merely DERIVED
            // one. A lower join whose output "is" HASH(k,N) never actually shipped those rows through a
            // shuffle, so satisfying a materialized demand with it puts two join tiers in one fragment.
            // The transport can now RUN that shape (ShuffleSlots is N-ary), so this is no longer a
            // capability limit — it is the conservative default: collapsing tiers raises one worker's peak
            // memory and DataFusion's hash-join build does not spill. A caller that wants collapse asks for
            // a DERIVED partitioning (the demand `hash()` builds is un-materialized, which any
            // co-partitioned child satisfies); only a demand explicitly marked materialized insists on a
            // real shuffle. Producing-side materialized data satisfies BOTH kinds of demand.
            if (other.exchangeMaterialized && !this.exchangeMaterialized) {
                return false;
            }
            // Locality: WORKER produced data satisfies a WORKER demand. A null demanded
            // locality accepts either (rare; HASH demands always carry WORKER today).
            if (other.locality == null) return true;
            return this.locality == other.locality;
        }
        if (this.type == Type.BROADCAST_DISTRIBUTED) {
            // Broadcast = full-row-set replicated to every probe-side node. The demand carries
            // no keys (broadcast doesn't partition on anything). Locality must match: broadcast
            // produced data satisfies a REPLICATED demand only. A null demanded locality
            // accepts either (rare; broadcast demands always carry REPLICATED today).
            if (other.locality == null) return true;
            return this.locality == other.locality;
        }
        return this.keys.equals(other.keys);
    }

    /** Returns true iff {@code prefix} is a prefix of {@code list}. */
    private static boolean isPrefix(List<Integer> prefix, List<Integer> list) {
        if (prefix.size() > list.size()) return false;
        for (int i = 0; i < prefix.size(); i++) {
            if (!Objects.equals(prefix.get(i), list.get(i))) return false;
        }
        return true;
    }

    @Override
    public void register(RelOptPlanner planner) {}

    @Override
    public RelDistribution apply(Mappings.TargetMapping mapping) {
        if (type != Type.HASH_DISTRIBUTED || keys.isEmpty()) {
            return this;
        }
        // Calcite's contract on RelDistribution.apply (RelDistribution.java:53-67) is to
        // silently degrade to ANY if any HASH key cannot be mapped through the projection.
        // Mappings.apply2 throws on an unmapped key, which is the wrong behavior here — fall
        // back to ANY when the mapping drops a key we depend on.
        List<Integer> newKeys = new ArrayList<>(keys.size());
        for (int key : keys) {
            int target = mapping.getTargetOpt(key);
            if (target < 0) {
                return new OpenSearchDistribution(traitDef, null, Type.ANY, List.of(), null, null, null);
            }
            newKeys.add(target);
        }
        return new OpenSearchDistribution(traitDef, locality, Type.HASH_DISTRIBUTED, newKeys, tableId, shardCount, partitionCount);
    }

    @Override
    public boolean isTop() {
        return type == Type.ANY;
    }

    @Override
    public int compareTo(org.apache.calcite.plan.RelMultipleTrait other) {
        if (other instanceof OpenSearchDistribution otherDist) {
            return type.compareTo(otherDist.type);
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof OpenSearchDistribution other)) return false;
        return type == other.type
            && locality == other.locality
            && Objects.equals(keys, other.keys)
            && Objects.equals(tableId, other.tableId)
            && Objects.equals(shardCount, other.shardCount)
            && Objects.equals(partitionCount, other.partitionCount)
            && exchangeMaterialized == other.exchangeMaterialized;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, locality, keys, tableId, shardCount, partitionCount, exchangeMaterialized);
    }

    @Override
    public String toString() {
        String base = switch (type) {
            case SINGLETON -> "SINGLETON";
            case RANDOM_DISTRIBUTED -> "RANDOM";
            case HASH_DISTRIBUTED -> "HASH" + keys;
            case ANY -> "ANY";
            default -> type.shortName;
        };
        if (type == Type.ANY) return base;
        StringBuilder sb = new StringBuilder(base);
        if (locality != null) {
            sb.append('(').append(locality);
            if (tableId != null) {
                sb.append(":t=").append(tableId);
            }
            if (shardCount != null) {
                sb.append(":s=").append(shardCount);
            }
            if (partitionCount != null) {
                sb.append(":p=").append(partitionCount);
            }
            sb.append(')');
        }
        return sb.toString();
    }
}
