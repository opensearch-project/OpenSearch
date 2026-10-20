/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Calcite convention for all OpenSearch Analytics operators.
 * Operators using this convention participate in Volcano CBO
 * for distribution trait propagation and exchange insertion.
 *
 * @opensearch.internal
 */
public enum OpenSearchConvention implements Convention {
    INSTANCE;

    @Override
    public Class<?> getInterface() {
        return OpenSearchRelNode.class;
    }

    @Override
    public String getName() {
        return "OPENSEARCH";
    }

    @Override
    public RelTraitDef<Convention> getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        return this == trait;
    }

    @Override
    public void register(RelOptPlanner planner) {}

    @Override
    public boolean canConvertConvention(Convention toConvention) {
        return false;
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }
}
