/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.operators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

/**
 * OpenSearch-specific table scan operator.
 * Wraps a Calcite {@link TableScan} and carries a backend tag for resolution.
 *
 * <p>Requirements: 3.1, 3.5
 */
public final class OpenSearchTableScan extends TableScan implements BackendTagged {

    private final String backendTag;

    public OpenSearchTableScan(RelOptCluster cluster, RelTraitSet traits,
                               RelOptTable table, String backendTag) {
        super(cluster, traits, List.of(), table);
        this.backendTag = backendTag;
    }

    @Override
    public String getBackendTag() {
        return backendTag;
    }

    @Override
    public OpenSearchTableScan withBackendTag(String tag) {
        return new OpenSearchTableScan(getCluster(), getTraitSet(), getTable(), tag);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchTableScan(getCluster(), traitSet, getTable(), backendTag);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backendTag);
    }
}
