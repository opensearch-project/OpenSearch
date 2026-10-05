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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adds implicit element expansion for LIST-valued GROUP BY keys.
 *
 * <p>This rewriter appends a scalar unnested column per LIST GROUP BY key and remaps
 * the grouping set indices so that Substrait serialisation sees scalar types throughout.
 * It runs at Substrait-conversion time (inside
 * {@code DataFusionFragmentConvertor.preprocessForSubstrait}), which is <em>after</em>
 * {@code PlannerImpl.decomposeAggregates} has already split the aggregate into
 * PARTIAL/FINAL halves. The PARTIAL half is expanded here; the FINAL half reads the
 * already-expanded rows from its stage input, so its LIST keys are retyped to the element
 * type instead of being expanded again (see {@link #retypeExpandedStageInputKeys}).
 *
 * <p><b>Known issue</b>: the cleaner fix is to move this expansion into
 * {@code PlannerImpl.runAllOptimizations} <em>before</em> the
 * {@code decomposeAggregates} call so that Calcite propagates element types into both
 * the PARTIAL and FINAL fragments and the FINAL-side retyping becomes unnecessary.  That
 * requires relocating {@link MultiValueExpandRel} into a module that {@code analytics-engine}
 * can depend on (the dependency currently flows the other way:
 * {@code analytics-backend-datafusion} extends {@code analytics-engine}).  This is tracked
 * for a follow-up PR.
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
        if (OpenSearchAggregate.isFinalMode(aggregate) && input instanceof DataFusionFragmentConvertor.StageInputTableScan stageInput) {
            return retypeExpandedStageInputKeys(aggregate, stageInput);
        }
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

    /**
     * FINAL-side counterpart of the expansion above. The PARTIAL fragment already replaced each
     * LIST GROUP BY key with its expanded scalar element (see {@link #rewriteAggregate}), so the
     * rows arriving on the reduce stage's input partition carry the element type. Calcite,
     * however, still types the stage input from the pre-expansion aggregate row type, so the
     * FINAL aggregate would (a) expand an already-scalar column a second time and (b) declare
     * the partition's Substrait {@code base_schema} as {@code List(Utf8)} where the registered
     * stream is {@code Utf8View}, which DataFusion rejects at the ReadRel. Retype the affected
     * keys on the stage input to the nullable element type instead; the aggregate's row type
     * re-derives from the input, so no expansion or reordering Project is needed.
     */
    private static RelNode retypeExpandedStageInputKeys(Aggregate aggregate, DataFusionFragmentConvertor.StageInputTableScan stageInput) {
        RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();
        List<RelDataTypeField> fields = stageInput.getRowType().getFieldList();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        boolean changed = false;
        for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            RelDataTypeField field = fields.get(fieldIndex);
            RelDataType elementType = field.getType().getComponentType();
            if (elementType != null && aggregate.getGroupSet().get(fieldIndex)) {
                builder.add(field.getName(), typeFactory.createTypeWithNullability(elementType, true));
                changed = true;
            } else {
                builder.add(field.getName(), field.getType());
            }
        }
        if (!changed) {
            return aggregate;
        }
        RelNode retyped = new DataFusionFragmentConvertor.StageInputTableScan(
            stageInput.getCluster(),
            stageInput.getTraitSet(),
            stageInput.getTable().getQualifiedName().getFirst(),
            builder.build()
        );
        return aggregate.copy(
            aggregate.getTraitSet(),
            retyped,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList()
        );
    }

    private static ImmutableBitSet remap(ImmutableBitSet fields, Map<Integer, Integer> replacements) {
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int field : fields) {
            builder.set(replacements.getOrDefault(field, field));
        }
        return builder.build();
    }
}
