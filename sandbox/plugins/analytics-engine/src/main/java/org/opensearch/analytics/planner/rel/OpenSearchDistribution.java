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

import java.util.List;
import java.util.Objects;

/**
 * Distribution trait for OpenSearch Analytics operators.
 *
 * <p>Carries three pieces of information:
 * <ul>
 *   <li>{@link Locality} — where the rows physically live. {@code SHARD} means data sits
 *       at its storage location (TableScan output, shard-local Filter/Project/PARTIAL agg).
 *       {@code COORDINATOR} means data has been gathered to the coord (ER output, FINAL
 *       aggregate output, Join/Union output).</li>
 *   <li>{@link Type} — Calcite's partitioning model (SINGLETON / RANDOM / HASH / ANY).</li>
 *   <li>{@code tableId} — for {@code SHARD} distributions, identifies the source table so
 *       the planner can reason about when two SHARD streams belong to the same physical
 *       layout (future: co-located joins). Null on COORDINATOR distributions.</li>
 * </ul>
 *
 * <p><b>Satisfies semantics.</b> {@code SINGLETON} with the same locality satisfies; a
 * SINGLETON demand with null locality accepts either (used by callers that don't care
 * whether data is shard-local or gathered). Non-SINGLETON types fall back to plain
 * type+keys equality.
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
        COORDINATOR
    }

    private final OpenSearchDistributionTraitDef traitDef;
    private final Locality locality;
    private final Type type;
    private final List<Integer> keys;
    private final Integer tableId;
    private final Integer shardCount;

    OpenSearchDistribution(
        OpenSearchDistributionTraitDef traitDef,
        Locality locality,
        Type type,
        List<Integer> keys,
        Integer tableId,
        Integer shardCount
    ) {
        this.traitDef = traitDef;
        this.locality = locality;
        this.type = type;
        this.keys = keys;
        this.tableId = tableId;
        this.shardCount = shardCount;
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
        if (this.type != other.type || !this.keys.equals(other.keys)) {
            return false;
        }
        if (this.type == Type.SINGLETON) {
            // SINGLETON demand with null locality accepts either SHARD or COORDINATOR.
            // Otherwise the locality must match exactly — this is what prevents an ER over a
            // SHARD scan from dedup-ing into a COORDINATOR-producing sibling subset, and keeps
            // the two classes of "on one node" distinguishable during plan search.
            if (other.locality == null) return true;
            return this.locality == other.locality;
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
        List<Integer> newKeys = new java.util.ArrayList<>(keys.size());
        for (int key : keys) {
            int target = mapping.getTargetOpt(key);
            if (target < 0) {
                return new OpenSearchDistribution(traitDef, null, Type.ANY, List.of(), null, null);
            }
            newKeys.add(target);
        }
        return new OpenSearchDistribution(traitDef, locality, Type.HASH_DISTRIBUTED, newKeys, tableId, shardCount);
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
            && Objects.equals(shardCount, other.shardCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, locality, keys, tableId, shardCount);
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
            sb.append(')');
        }
        return sb.toString();
    }
}
