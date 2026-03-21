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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * OpenSearch-specific filter operator.
 * Wraps a Calcite {@link Filter} and carries a backend tag for resolution.
 *
 * <p>Requirements: 3.2, 3.5, 3.6
 */
public final class OpenSearchFilter extends Filter implements BackendTagged {

    private final String backendTag;

    public OpenSearchFilter(RelOptCluster cluster, RelTraitSet traits,
                            RelNode input, RexNode condition, String backendTag) {
        super(cluster, traits, input, condition);
        this.backendTag = backendTag;
    }

    @Override
    public String getBackendTag() {
        return backendTag;
    }

    @Override
    public OpenSearchFilter withBackendTag(String tag) {
        return new OpenSearchFilter(getCluster(), getTraitSet(), getInput(), getCondition(), tag);
    }

    @Override
    public OpenSearchFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OpenSearchFilter(getCluster(), traitSet, input, condition, backendTag);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backendTag);
    }
}
