/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Adds implicit element expansion for LIST-valued GROUP BY keys. */
final class MultiValueRelRewriter {

    private MultiValueRelRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited instanceof Aggregate aggregate ? rewriteAggregate(aggregate) : visited;
            }
        });
    }

    private static RelNode rewriteAggregate(Aggregate aggregate) {
        RelNode input = aggregate.getInput();
        Map<Integer, Integer> expandedGroupFields = new HashMap<>();
        for (int fieldIndex : aggregate.getGroupSet()) {
            if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() != null) {
                MultiValueExpandRel expansion = new MultiValueExpandRel(input, fieldIndex);
                input = expansion;
                expandedGroupFields.put(fieldIndex, expansion.expandedFieldIndex());
            }
        }
        if (expandedGroupFields.isEmpty()) {
            return aggregate;
        }

        ImmutableBitSet groupSet = remap(aggregate.getGroupSet(), expandedGroupFields);
        List<ImmutableBitSet> groupSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
            aggregate.getGroupSets().stream().map(fields -> remap(fields, expandedGroupFields)).toList()
        );
        Aggregate rewritten = aggregate.copy(aggregate.getTraitSet(), input, groupSet, groupSets, aggregate.getAggCallList());

        // Appending group keys can change their ordinal order and uses internal field names. Restore
        // the original aggregate output order and names while retaining the expanded scalar types.
        List<Integer> rewrittenGroupFields = groupSet.asList();
        List<RexNode> projects = new ArrayList<>(rewritten.getRowType().getFieldCount());
        for (int originalField : aggregate.getGroupSet()) {
            int rewrittenField = expandedGroupFields.getOrDefault(originalField, originalField);
            projects.add(rewritten.getCluster().getRexBuilder().makeInputRef(rewritten, rewrittenGroupFields.indexOf(rewrittenField)));
        }
        for (int index = groupSet.cardinality(); index < rewritten.getRowType().getFieldCount(); index++) {
            projects.add(rewritten.getCluster().getRexBuilder().makeInputRef(rewritten, index));
        }
        return LogicalProject.create(rewritten, List.of(), projects, aggregate.getRowType().getFieldNames());
    }

    private static ImmutableBitSet remap(ImmutableBitSet fields, Map<Integer, Integer> replacements) {
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int field : fields) {
            builder.set(replacements.getOrDefault(field, field));
        }
        return builder.build();
    }
}
