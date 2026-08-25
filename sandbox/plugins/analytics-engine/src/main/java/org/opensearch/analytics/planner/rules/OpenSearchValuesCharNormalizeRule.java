/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-marking rule that rewrites the inline {@link LogicalValues} character columns to
 * precision-unspecified {@code VARCHAR}, rebuilds the row literals at that type, and wraps the result
 * in a casting {@link org.apache.calcite.rel.logical.LogicalProject} restoring the original row type.
 * It covers {@code CHAR} columns and any {@code VARCHAR} column that carries an explicit precision.
 *
 * <p>isthmus types each Substrait {@code VirtualTable} cell from its own literal, so mixed-length
 * {@code CHAR} literals produce fixed-length cells that fail the {@code VirtualTableScan}
 * row-conforms-to-schema check. Normalising column and literals to {@code Str} makes both sides match
 * and keeps {@code DataFusionFragmentConvertor} generic.
 *
 * <p>Literals are rebuilt with {@link RexLiteral#fromJdbcString} at a plain unspecified-precision
 * {@code VARCHAR} type. {@code makeLiteral} would canonicalise a bare string back to {@code CHAR}.
 *
 * @opensearch.internal
 */
public class OpenSearchValuesCharNormalizeRule extends RelOptRule {

    public OpenSearchValuesCharNormalizeRule() {
        super(operand(LogicalValues.class, any()), "OpenSearchValuesCharNormalizeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalValues values = call.rel(0);
        if (values.getTuples().isEmpty()) {
            return false;
        }
        return values.getRowType().getFieldList().stream().anyMatch(f -> needsNormalization(f.getType()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalValues values = call.rel(0);
        RelDataTypeFactory typeFactory = values.getCluster().getTypeFactory();
        RexBuilder rexBuilder = values.getCluster().getRexBuilder();
        RelDataType originalRowType = values.getRowType();
        List<RelDataTypeField> fields = originalRowType.getFieldList();

        RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
        boolean[] normalized = new boolean[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            if (needsNormalization(field.getType())) {
                normalized[i] = true;
                typeBuilder.add(field.getName(), strType(typeFactory, field.getType().isNullable()));
            } else {
                typeBuilder.add(field);
            }
        }
        RelDataType normalizedRowType = typeBuilder.build();

        RelDataType literalTarget = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        ImmutableList.Builder<ImmutableList<RexLiteral>> newTuples = ImmutableList.builder();
        for (List<RexLiteral> tuple : values.getTuples()) {
            ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
            for (int i = 0; i < tuple.size(); i++) {
                RexLiteral lit = tuple.get(i);
                if (!normalized[i]) {
                    row.add(lit);
                } else if (lit.isNull()) {
                    row.add((RexLiteral) rexBuilder.makeNullLiteral(strType(typeFactory, true)));
                } else {
                    row.add(RexLiteral.fromJdbcString(literalTarget, SqlTypeName.CHAR, lit.getValueAs(String.class)));
                }
            }
            newTuples.add(row.build());
        }

        LogicalValues normalizedValues = (LogicalValues) LogicalValues.create(values.getCluster(), normalizedRowType, newTuples.build());
        call.transformTo(projectToOriginalRowType(call, originalRowType, normalizedValues));
    }

    private static RelNode projectToOriginalRowType(RelOptRuleCall call, RelDataType originalRowType, LogicalValues normalizedValues) {
        if (normalizedValues.getRowType().equals(originalRowType)) {
            return normalizedValues;
        }
        RelBuilder relBuilder = call.builder();
        relBuilder.push(normalizedValues);
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RelDataTypeField> origFields = originalRowType.getFieldList();
        List<RelDataTypeField> newFields = normalizedValues.getRowType().getFieldList();
        List<RexNode> projects = new ArrayList<>(origFields.size());
        List<String> names = new ArrayList<>(origFields.size());
        for (int i = 0; i < origFields.size(); i++) {
            RexNode ref = rexBuilder.makeInputRef(normalizedValues, i);
            RelDataType targetType = origFields.get(i).getType();
            if (!newFields.get(i).getType().equals(targetType)) {
                ref = rexBuilder.makeCast(targetType, ref);
            }
            projects.add(ref);
            names.add(origFields.get(i).getName());
        }
        relBuilder.project(projects, names, /* forceProject */ true);
        return relBuilder.build();
    }

    /** {@code CHAR} and explicit-precision {@code VARCHAR} need rewriting. Bare {@code VARCHAR} is already {@code Str}. */
    private static boolean needsNormalization(RelDataType type) {
        SqlTypeName t = type.getSqlTypeName();
        if (t == SqlTypeName.CHAR) {
            return true;
        }
        return t == SqlTypeName.VARCHAR && type.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED;
    }

    private static RelDataType strType(RelDataTypeFactory typeFactory, boolean nullable) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), nullable);
    }
}
