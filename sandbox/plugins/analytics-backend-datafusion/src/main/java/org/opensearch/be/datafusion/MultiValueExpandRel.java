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
 * Backend-local relation that replaces one LIST column with one row per distinct element.
 *
 * <p>The SQL frontend already represents explicit {@code mvexpand} as Correlate+Uncollect.
 * This relation is used only for implicit multi-value GROUP BY semantics, where each document
 * contributes at most once to each element bucket.
 */
final class MultiValueExpandRel extends SingleRel {

    private final int fieldIndex;

    MultiValueExpandRel(RelNode input, int fieldIndex) {
        super(input.getCluster(), input.getTraitSet(), input);
        this.fieldIndex = fieldIndex;
        if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() == null) {
            throw new IllegalArgumentException("field " + fieldIndex + " is not a collection");
        }
    }

    int fieldIndex() {
        return fieldIndex;
    }

    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        List<RelDataTypeField> fields = getInput().getRowType().getFieldList();
        for (int index = 0; index < fields.size(); index++) {
            RelDataTypeField field = fields.get(index);
            RelDataType type = field.getType();
            if (index == fieldIndex) {
                type = getCluster().getTypeFactory().createTypeWithNullability(type.getComponentType(), true);
            }
            builder.add(field.getName(), type);
        }
        return builder.build();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MultiValueExpandRel(sole(inputs), fieldIndex);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("field", getInput().getRowType().getFieldNames().get(fieldIndex)).item("distinct", true);
    }
}
