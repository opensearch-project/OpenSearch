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
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/** Marked Correlate used for the frontend's {@code mvexpand} Correlate+Uncollect shape. */
public class OpenSearchCorrelate extends Correlate implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchCorrelate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        CorrelationId correlationId,
        ImmutableBitSet requiredColumns,
        JoinRelType joinType,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, List.of(), left, right, correlationId, requiredColumns, joinType);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode left = RelNodeUtils.unwrapHep(getLeft());
        List<FieldStorageInfo> result = new ArrayList<>();
        if (left instanceof OpenSearchRelNode openSearchLeft) {
            result.addAll(openSearchLeft.getOutputFieldStorage());
        }
        LinkedHashSet<String> dependencies = new LinkedHashSet<>();
        for (int index : requiredColumns) {
            if (index < left.getRowType().getFieldCount()) {
                dependencies.add(left.getRowType().getFieldList().get(index).getName());
            }
        }
        for (int index = left.getRowType().getFieldCount(); index < getRowType().getFieldCount(); index++) {
            var field = getRowType().getFieldList().get(index);
            result.add(FieldStorageInfo.derivedColumn(field.getName(), field.getType().getSqlTypeName(), dependencies));
        }
        return result;
    }

    @Override
    public Correlate copy(
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        CorrelationId correlationId,
        ImmutableBitSet requiredColumns,
        JoinRelType joinType
    ) {
        return new OpenSearchCorrelate(getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType, viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchCorrelate(
            getCluster(),
            getTraitSet(),
            children.get(0),
            children.get(1),
            correlationId,
            requiredColumns,
            joinType,
            List.of(backend)
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalCorrelate.create(strippedChildren.get(0), strippedChildren.get(1), correlationId, requiredColumns, joinType);
    }
}
