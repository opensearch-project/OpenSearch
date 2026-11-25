/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

/**
 * Backend-local relation that appends one row-expanded scalar column for a LIST input column.
 *
 * <p>The SQL frontend already represents explicit {@code mvexpand} as Correlate+Uncollect.
 * This relation is used only for implicit multi-value GROUP BY semantics, where each document
 * contributes at most once to each element bucket. The source LIST remains available to aggregate
 * calls while GROUP BY uses the appended scalar column.
 */
final class MultiValueExpandRel extends SingleRel {

    private final int fieldIndex;
    private final String expandedFieldName;

    MultiValueExpandRel(RelNode input, int fieldIndex) {
        super(input.getCluster(), input.getTraitSet(), input);
        this.fieldIndex = fieldIndex;
        if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() == null) {
            throw new IllegalArgumentException("field " + fieldIndex + " is not a collection");
        }
        String candidate = "___mvexpand_" + fieldIndex;
        while (input.getRowType().getFieldNames().contains(candidate)) {
            candidate += "_";
        }
        this.expandedFieldName = candidate;
    }

    int fieldIndex() {
        return fieldIndex;
    }

    int expandedFieldIndex() {
        return getInput().getRowType().getFieldCount();
    }

    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        List<RelDataTypeField> fields = getInput().getRowType().getFieldList();
        for (RelDataTypeField field : fields) {
            builder.add(field.getName(), field.getType());
        }
        RelDataType elementType = fields.get(fieldIndex).getType().getComponentType();
        builder.add(expandedFieldName, getCluster().getTypeFactory().createTypeWithNullability(elementType, true));
        return builder.build();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MultiValueExpandRel(sole(inputs), fieldIndex);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("field", getInput().getRowType().getFieldNames().get(fieldIndex))
            .item("output", expandedFieldName)
            .item("distinct", true);
    }
}
