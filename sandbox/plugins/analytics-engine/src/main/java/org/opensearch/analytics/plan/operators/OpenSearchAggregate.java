/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.operators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * OpenSearch-specific aggregate operator.
 * Wraps a Calcite {@link Aggregate} and carries a backend tag and aggregation mode.
 *
 * <p>Requirements: 3.3, 3.5, 4.4
 */
public final class OpenSearchAggregate extends Aggregate implements BackendTagged {

    private final String backendTag;
    private final AggMode mode;

    public OpenSearchAggregate(RelOptCluster cluster, RelTraitSet traits,
                               RelNode input, ImmutableBitSet groupSet,
                               List<ImmutableBitSet> groupSets,
                               List<AggregateCall> aggCalls,
                               String backendTag, AggMode mode) {
        super(cluster, traits, List.of(), input, groupSet, groupSets, aggCalls);
        this.backendTag = backendTag;
        this.mode = mode;
    }

    @Override
    public String getBackendTag() {
        return backendTag;
    }

    public AggMode getMode() {
        return mode;
    }

    @Override
    public OpenSearchAggregate withBackendTag(String tag) {
        return new OpenSearchAggregate(getCluster(), getTraitSet(), getInput(),
            getGroupSet(), getGroupSets(), getAggCallList(), tag, mode);
    }

    @Override
    public OpenSearchAggregate copy(RelTraitSet traitSet, RelNode input,
            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        return new OpenSearchAggregate(getCluster(), traitSet, input,
            groupSet, groupSets, aggCalls, backendTag, mode);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backendTag).item("mode", mode);
    }
}
