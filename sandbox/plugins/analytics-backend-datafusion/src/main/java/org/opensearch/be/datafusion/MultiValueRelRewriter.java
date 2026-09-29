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

/**
 * Adds implicit element expansion for LIST-valued GROUP BY keys.
 *
 * <p>This rewriter appends a scalar unnested column per LIST GROUP BY key and remaps
 * the grouping set indices so that Substrait serialisation sees scalar types throughout.
 * It currently runs at Substrait-conversion time (inside
 * {@code DataFusionFragmentConvertor.preprocessForSubstrait}), which is <em>after</em>
 * {@code PlannerImpl.decomposeAggregates} has already split the aggregate into
 * PARTIAL/FINAL halves.  In a multi-shard query the FINAL fragment therefore receives
 * a {@code List(Utf8)} GROUP BY key where Calcite expects the element type
 * ({@code Utf8View}), causing a type mismatch 500.
 *
 * <p><b>Known issue</b>: the correct fix is to move this expansion into
 * {@code PlannerImpl.runAllOptimizations} <em>before</em> the
 * {@code decomposeAggregates} call so that Calcite propagates element types into both
 * the PARTIAL and FINAL fragments.  That requires relocating
 * {@link MultiValueExpandRel} into a module that {@code analytics-engine} can depend
 * on (the dependency currently flows the other way: {@code analytics-backend-datafusion}
 * extends {@code analytics-engine}).  This is tracked for a follow-up PR.
 */
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
