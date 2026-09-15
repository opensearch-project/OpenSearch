/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Rewrites LIST sort keys to fixed {@code MIN(list)} scalar keys. */
final class MultiValueSortRewriter {

    static final SqlFunction LIST_MIN_OP = new SqlFunction("list_min", SqlKind.OTHER_FUNCTION, opBinding -> {
        var component = opBinding.getOperandType(0).getComponentType();
        if (component == null) {
            throw new IllegalArgumentException("list_min requires an ARRAY operand");
        }
        return opBinding.getTypeFactory().createTypeWithNullability(component, true);
    }, null, OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION);

    private MultiValueSortRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                return visited instanceof Sort sort ? rewriteSort(sort) : visited;
            }
        });
    }

    private static RelNode rewriteSort(Sort sort) {
        RelNode input = sort.getInput();
        List<RelFieldCollation> oldFields = sort.getCollation().getFieldCollations();
        Map<Integer, Integer> hiddenByInput = new LinkedHashMap<>();
        for (RelFieldCollation field : oldFields) {
            int inputIndex = field.getFieldIndex();
            if (input.getRowType().getFieldList().get(inputIndex).getType().getComponentType() != null) {
                hiddenByInput.computeIfAbsent(inputIndex, ignored -> input.getRowType().getFieldCount() + hiddenByInput.size());
            }
        }
        if (hiddenByInput.isEmpty()) {
            return sort;
        }

        RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>(input.getRowType().getFieldCount() + hiddenByInput.size());
        List<String> names = new ArrayList<>(input.getRowType().getFieldNames());
        for (int index = 0; index < input.getRowType().getFieldCount(); index++) {
            projects.add(rexBuilder.makeInputRef(input, index));
        }
        for (int inputIndex : hiddenByInput.keySet()) {
            RexNode list = rexBuilder.makeInputRef(input, inputIndex);
            projects.add(rexBuilder.makeCall(LIST_MIN_OP, list));
            names.add("___mv_sort_" + inputIndex);
        }
        RelNode withKeys = LogicalProject.create(input, List.of(), projects, names);

        List<RelFieldCollation> newFields = oldFields.stream().map(field -> {
            Integer hidden = hiddenByInput.get(field.getFieldIndex());
            return hidden == null ? field : new RelFieldCollation(hidden, field.getDirection(), field.nullDirection);
        }).toList();
        RelNode sorted = sort.copy(sort.getTraitSet(), withKeys, RelCollations.of(newFields), sort.offset, sort.fetch);

        List<RexNode> output = new ArrayList<>(input.getRowType().getFieldCount());
        for (int index = 0; index < input.getRowType().getFieldCount(); index++) {
            output.add(rexBuilder.makeInputRef(sorted, index));
        }
        return LogicalProject.create(sorted, List.of(), output, input.getRowType().getFieldNames());
    }
}
