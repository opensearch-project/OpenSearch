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
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;

/**
 * Distribution trait for OpenSearch Analytics operators.
 * Each instance holds a reference to its {@link OpenSearchDistributionTraitDef}
 * for Calcite's identity-based trait matching.
 *
 * <p>Created via {@link OpenSearchDistributionTraitDef} factory methods
 * to ensure the correct trait def reference.
 *
 * @opensearch.internal
 */
@SuppressWarnings("unchecked")
public class OpenSearchDistribution implements RelDistribution {

    private final OpenSearchDistributionTraitDef traitDef;
    private final Type type;
    private final List<Integer> keys;

    OpenSearchDistribution(OpenSearchDistributionTraitDef traitDef, Type type, List<Integer> keys) {
        this.traitDef = traitDef;
        this.type = type;
        this.keys = keys;
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
        return this.type == other.type && this.keys.equals(other.keys);
    }

    @Override
    public void register(RelOptPlanner planner) {}

    @Override
    public RelDistribution apply(Mappings.TargetMapping mapping) {
        if (type != Type.HASH_DISTRIBUTED || keys.isEmpty()) {
            return this;
        }
        List<Integer> newKeys = Mappings.apply2((Mapping) mapping, keys);
        return new OpenSearchDistribution(traitDef, Type.HASH_DISTRIBUTED, newKeys);
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
    public String toString() {
        return switch (type) {
            case SINGLETON -> "SINGLETON";
            case RANDOM_DISTRIBUTED -> "RANDOM";
            case HASH_DISTRIBUTED -> "HASH" + keys;
            case ANY -> "ANY";
            default -> type.shortName;
        };
    }
}
