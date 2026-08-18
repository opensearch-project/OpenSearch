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
import org.apache.calcite.rel.RelNode;

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

    /**
     * Materializes the exchange that carries {@code input} to the required distribution.
     *
     * <p>Under top-down Volcano this is how a trait request becomes a physical operator: when an
     * operator's {@link OpenSearchRelNode#passThroughTraits} demand cannot be satisfied in place,
     * Calcite calls {@code enforce} to insert the converter. Delegates to
     * {@link OpenSearchDistributionTraitDef#buildEnforcer}, which is satisfies-gated and therefore a
     * no-op when {@code input} already delivers the requested distribution.
     *
     * <p>Returning {@code null} for a request that carries no {@link OpenSearchDistribution} means
     * "cannot enforce", which is correct: our only physical trait is the distribution, so a request
     * for anything else has no exchange to build.
     */
    @Override
    public RelNode enforce(RelNode input, RelTraitSet required) {
        OpenSearchDistribution distribution = OpenSearchRelNode.distributionOf(required);
        if (distribution == null) {
            return null;
        }
        return ((OpenSearchDistributionTraitDef) distribution.getTraitDef()).buildEnforcer(input, distribution);
    }

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
