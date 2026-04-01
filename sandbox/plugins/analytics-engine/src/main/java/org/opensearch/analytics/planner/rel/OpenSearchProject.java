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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.FieldStorageInfo;

import java.util.List;

/**
 * @opensearch.internal
 */
public class OpenSearchProject extends Project implements OpenSearchRelNode {

    private final String backend;

    public OpenSearchProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                             List<? extends RexNode> projects, RelDataType rowType,
                             String backend) {
        super(cluster, traitSet, List.of(), input, projects, rowType);
        this.backend = backend;
    }

    @Override
    public String getBackend() {
        return backend;
    }

    @Override
    public List<String> getViableBackends() {
        return List.of(backend);
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        return getRowType().getFieldList().stream()
            .map(field -> FieldStorageInfo.derivedColumn(field.getName(), field.getType().toString()))
            .toList();
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new OpenSearchProject(getCluster(), traitSet, input, projects, rowType, backend);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("backend", backend);
    }
}
