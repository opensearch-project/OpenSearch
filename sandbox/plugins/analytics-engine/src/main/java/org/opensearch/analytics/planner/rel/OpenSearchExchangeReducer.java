/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.analytics.planner.FieldStorageInfo;

import java.util.List;

/**
 * Coordinator-side reducer for SINGLETON exchanges. Receives streaming
 * Arrow batches from data nodes via Analytics Core transport. The backend
 * decides internally how to reduce (in-memory table, streaming sink, etc.).
 *
 * <p>Only used for SINGLETON distribution. Shuffle exchanges use
 * {@link OpenSearchShuffleReader} instead.
 *
 * @opensearch.internal
 */
public class OpenSearchExchangeReducer extends SingleRel implements OpenSearchRelNode {

    private final String backend;

    public OpenSearchExchangeReducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                     String backend) {
        super(cluster, traitSet, input);
        this.backend = backend;
    }

    @Override
    public String getBackend() {
        return backend;
    }

    @Override
    public List<String> getViableBackends() {
        if (getInput() instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getViableBackends();
        }
        return List.of();
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        if (getInput() instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchExchangeReducer(getCluster(), traitSet, sole(inputs), backend);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backend).item("viableBackends", getViableBackends());
    }
}
