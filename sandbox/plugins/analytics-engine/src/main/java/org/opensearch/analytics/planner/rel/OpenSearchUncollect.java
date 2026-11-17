/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.LinkedHashSet;
import java.util.List;

/** Marked Uncollect used by multi-value row expansion. */
public class OpenSearchUncollect extends Uncollect implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchUncollect(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        boolean withOrdinality,
        List<String> itemAliases,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input, withOrdinality, itemAliases);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        LinkedHashSet<String> dependencies = new LinkedHashSet<>();
        if (input instanceof OpenSearchRelNode openSearchInput) {
            for (FieldStorageInfo field : openSearchInput.getOutputFieldStorage()) {
                dependencies.add(field.getFieldName());
            }
        }
        return getRowType().getFieldList()
            .stream()
            .map(field -> FieldStorageInfo.derivedColumn(field.getName(), field.getType().getSqlTypeName(), dependencies))
            .toList();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, RelNode input) {
        return new OpenSearchUncollect(getCluster(), traitSet, input, withOrdinality, getItemAliases(), viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchUncollect(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            withOrdinality,
            getItemAliases(),
            List.of(backend)
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return Uncollect.create(getTraitSet(), strippedChildren.getFirst(), withOrdinality, getItemAliases());
    }
}
