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

import java.util.Map;

/**
 * A filter whose predicates span multiple backends.
 * Created during Phase 5 when predicates in an {@link OpenSearchFilter} require different backends.
 * Carries the split predicate metadata for future cross-engine execution.
 *
 * <p>Requirements: 5.4
 */
public final class OpenSearchHybridFilter extends Filter implements BackendTagged {

    private final String backendTag;
    private final Map<String, RexNode> backendPredicates;

    public OpenSearchHybridFilter(RelOptCluster cluster, RelTraitSet traits,
                                   RelNode input, RexNode condition,
                                   String backendTag,
                                   Map<String, RexNode> backendPredicates) {
        super(cluster, traits, input, condition);
        this.backendTag = backendTag;
        this.backendPredicates = Map.copyOf(backendPredicates);
    }

    public Map<String, RexNode> getBackendPredicates() {
        return backendPredicates;
    }

    @Override
    public String getBackendTag() {
        return backendTag;
    }

    @Override
    public RelNode withBackendTag(String tag) {
        return new OpenSearchHybridFilter(getCluster(), getTraitSet(), getInput(),
            getCondition(), tag, backendPredicates);
    }

    @Override
    public OpenSearchHybridFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OpenSearchHybridFilter(getCluster(), traitSet, input, condition,
            backendTag, backendPredicates);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backendTag);
    }
}
