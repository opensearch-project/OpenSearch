/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchMultiValueExpand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Expands LIST-valued GROUP BY keys into one bucket contribution per distinct element per
 * document by inserting an {@link OpenSearchMultiValueExpand} beneath the {@link Aggregate}.
 *
 * <p>Runs in {@code PlannerImpl.runAllOptimizations} <em>before</em> {@code decomposeAggregates},
 * so Calcite's own PARTIAL/FINAL aggregate split observes the post-expansion scalar element type
 * on the GROUP BY key throughout — both fragments agree, avoiding the {@code List(Utf8)} vs
 * {@code Utf8View} type mismatch that resulted from running an equivalent rewrite after
 * decomposition (the former backend-local {@code MultiValueRelRewriter} in
 * {@code analytics-backend-datafusion}).
 *
 * <p>The expand rel appends the scalar element column after the input's columns, so the source
 * LIST column stays directly addressable at its original ordinal — aggregate argument references
 * (e.g. {@code stats list(tags) by tags}) need no remapping. Only the GROUP BY / grouping-set
 * indices that pointed at the LIST column are remapped to the appended ordinal, and the final
 * {@link LogicalProject} restores the original output field order and names.
 *
 * <p>Ideal end state: emit the same {@code LogicalCorrelate + Uncollect} shape an explicit
 * {@code mvexpand} lowers to, so both paths share one fragment-conversion recognizer. This branch
 * has no Correlate marking / distribution support yet, so that convergence is deferred to the
 * frontend {@code mvexpand} work; the two shapes already converge on the same
 * {@code MULTI_VALUE_EXPAND} Substrait extension at fragment conversion.
 */
public final class OpenSearchMultiValueGroupByRewriter {

    private OpenSearchMultiValueGroupByRewriter() {}

    /** Applies the rewrite bottom-up so a nested Aggregate's expansion is visible to any Aggregate above it. */
    public static RelNode rewrite(RelNode root) {
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
        Map<Integer, Integer> expandedGroupFields = new LinkedHashMap<>();
        for (int fieldIndex : aggregate.getGroupSet()) {
            if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() != null) {
                OpenSearchMultiValueExpand expansion = OpenSearchMultiValueExpand.create(input, fieldIndex);
                input = expansion;
                expandedGroupFields.put(fieldIndex, expansion.getExpandedFieldIndex());
            }
        }
        if (expandedGroupFields.isEmpty()) {
            return aggregate;
        }

        // The expand APPENDS the scalar element, so grouping on it directly would give a non-prefix
        // groupSet ({N} with N >= groupCount). OpenSearchAggregateSplitRule's non-prefix guard (built
        // for the `avg(x) by span(y)` shape, where a trailing key would land on a PARTIAL agg-output
        // slot) vetoes the PARTIAL/FINAL split for that shape — which would silently gather every
        // expanded row to the coordinator and re-introduce exactly the problem this rewrite exists
        // to fix. So re-project the input to put the (remapped) group keys FIRST, in the original
        // groupSet order, followed by every remaining input column, and group on the prefix
        // {0..groupCount-1}. Aggregate args are remapped through the same permutation.
        RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        List<Integer> keyOrder = new ArrayList<>();
        for (int originalField : aggregate.getGroupSet()) {
            keyOrder.add(expandedGroupFields.getOrDefault(originalField, originalField));
        }
        List<Integer> permutation = new ArrayList<>(keyOrder);
        for (int index = 0; index < input.getRowType().getFieldCount(); index++) {
            if (!permutation.contains(index)) {
                permutation.add(index);
            }
        }
        // oldIndex -> newIndex
        Map<Integer, Integer> newIndexOf = new HashMap<>();
        List<RexNode> reordered = new ArrayList<>(permutation.size());
        List<String> reorderedNames = new ArrayList<>(permutation.size());
        for (int newIndex = 0; newIndex < permutation.size(); newIndex++) {
            int oldIndex = permutation.get(newIndex);
            newIndexOf.put(oldIndex, newIndex);
            reordered.add(rexBuilder.makeInputRef(input, oldIndex));
            reorderedNames.add(input.getRowType().getFieldNames().get(oldIndex));
        }
        RelNode keysFirst = LogicalProject.create(input, List.of(), reordered, reorderedNames);

        int groupCount = aggregate.getGroupSet().cardinality();
        ImmutableBitSet groupSet = ImmutableBitSet.range(groupCount);
        List<ImmutableBitSet> groupSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
            aggregate.getGroupSets().stream().map(fields -> remap(remap(fields, expandedGroupFields), newIndexOf)).toList()
        );
        List<AggregateCall> aggCalls = new ArrayList<>(aggregate.getAggCallList().size());
        for (AggregateCall call : aggregate.getAggCallList()) {
            List<Integer> args = call.getArgList().stream().map(newIndexOf::get).toList();
            int filterArg = call.filterArg < 0 ? -1 : newIndexOf.get(call.filterArg);
            aggCalls.add(call.copy(args, filterArg, call.distinctKeys, call.collation));
        }
        Aggregate rewritten = aggregate.copy(aggregate.getTraitSet(), keysFirst, groupSet, groupSets, aggCalls);

        // Group keys are already in original order at the front; agg outputs follow. Only the
        // internal `___mvexpand_N` field names need restoring to the user-facing names.
        List<RexNode> projects = new ArrayList<>(rewritten.getRowType().getFieldCount());
        for (int index = 0; index < rewritten.getRowType().getFieldCount(); index++) {
            projects.add(rexBuilder.makeInputRef(rewritten, index));
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
