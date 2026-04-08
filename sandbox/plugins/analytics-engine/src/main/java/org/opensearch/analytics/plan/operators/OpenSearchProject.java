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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * OpenSearch-specific project operator.
 * Wraps a Calcite {@link Project} and carries a backend tag for resolution.
 *
 * <p>Requirements: 3.4, 3.5
 */
public final class OpenSearchProject extends Project implements BackendTagged {

    private final String backendTag;

    public OpenSearchProject(RelOptCluster cluster, RelTraitSet traits,
                             RelNode input, List<? extends RexNode> projects,
                             RelDataType rowType, String backendTag) {
        super(cluster, traits, List.of(), input, projects, rowType);
        this.backendTag = backendTag;
    }

    @Override
    public String getBackendTag() {
        return backendTag;
    }

    @Override
    public OpenSearchProject withBackendTag(String tag) {
        return new OpenSearchProject(getCluster(), getTraitSet(), getInput(),
            getProjects(), getRowType(), tag);
    }

    @Override
    public OpenSearchProject copy(RelTraitSet traitSet, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
        return new OpenSearchProject(getCluster(), traitSet, input, projects, rowType, backendTag);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backendTag);
    }
}
