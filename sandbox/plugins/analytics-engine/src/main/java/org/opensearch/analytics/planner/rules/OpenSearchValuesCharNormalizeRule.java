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
 * Pre-marking rule that normalises the character columns of an inline {@link LogicalValues}
 * (e.g. from {@code makeresults}) to a length-independent {@code VARCHAR} (precision unspecified),
 * rebuilds the row literals at that type, and wraps the result in a casting
 * {@link org.apache.calcite.rel.logical.LogicalProject} restoring the original row type (HepPlanner
 * requires a rule's {@code transformTo} output row type to equal the matched node's).
 *
 * <p>Modeled on {@link OpenSearchDistinctCountRule}: it runs before the marking phase so isthmus
 * converts the {@code Values} generically. isthmus types each Substrait {@code VirtualTable} cell
 * from its own {@link RexLiteral} (borrowing only nullability from the schema), so a {@code CHAR}
 * column with mixed-length literals yields {@code FixedChar(n)} cells that fail
 * {@code VirtualTableScan}'s row-conforms-to-schema check. Normalising the column and its literals to
 * precision-unspecified {@code VARCHAR} makes both the schema and the cells {@code Str}, so the check
 * passes without a backend-specific {@code visit(Values)} override in {@code DataFusionFragmentConvertor}.
 *
 * <p>The literals must be rebuilt with {@link RexLiteral#fromJdbcString} at the {@code VARCHAR} type,
 * not {@code RexBuilder.makeLiteral} (which canonicalises a bare string literal back to {@code CHAR});
 * and the {@code VARCHAR} type must come from {@code createSqlType(VARCHAR)} (precision {@code -1}) with
 * no charset/collation decoration (that trips {@code RexLiteral}'s constructor precondition).
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

    /**
     * True for a {@code CHAR} column, or a {@code VARCHAR} column with an explicit precision (isthmus
     * maps those to {@code FixedChar} / {@code varChar(n)} respectively); a precision-unspecified
     * {@code VARCHAR} is already {@code Str} and needs no rewrite.
     */
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
