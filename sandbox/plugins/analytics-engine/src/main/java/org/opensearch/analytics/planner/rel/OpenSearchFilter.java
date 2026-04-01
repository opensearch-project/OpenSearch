/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.FieldStorageInfo;

import java.util.List;

/**
 * @opensearch.internal
 */
public class OpenSearchFilter extends Filter implements OpenSearchRelNode {

    private final String backend;
    private final List<String> viableBackends;

    public OpenSearchFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                            RexNode condition, String backend, List<String> viableBackends) {
        super(cluster, traitSet, input, condition);
        this.backend = backend;
        this.viableBackends = viableBackends;
    }

    @Override
    public String getBackend() {
        return backend;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Filter doesn't change schema — pass through child's field storage. */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        if (getInput() instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OpenSearchFilter(getCluster(), traitSet, input, condition, backend, viableBackends);
    }

    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(org.apache.calcite.plan.RelOptPlanner planner,
                                                               org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backend).item("viableBackends", viableBackends);
    }
}
