/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Walks a Calcite {@link RexNode} filter tree and rewrites indexed column
 * predicates to {@code index_filter('column', 'value')} {@link RexCall}s.
 * <p>
 * Non-indexed predicates pass through unchanged. Boolean structure
 * (AND, OR, NOT) is preserved.
 * <p>
 * This class resides in analytics-engine and has no imports from
 * analytics-backend-datafusion or analytics-backend-lucene.
 */
public class IndexFilterDecomposer {

    /**
     * The {@code index_filter} SQL function used to represent indexed column
     * filters in the Calcite plan. Accepts two VARCHAR arguments (column name
     * and filter value) and returns BOOLEAN.
     */
    public static final SqlFunction INDEX_FILTER_FUNCTION = new SqlFunction(
        "index_filter",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /** Creates a new IndexFilterDecomposer. */
    public IndexFilterDecomposer() {}

    /**
     * Walks the RexNode tree and rewrites indexed column predicates to
     * {@code index_filter('column', 'value')} RexCalls.
     * <p>
     * Rewriting rules:
     * <ul>
     *   <li>{@code column = 'value'} where column ∈ indexedCols →
     *       {@code index_filter('column', 'value')}</li>
     *   <li>{@code AND(a, b)} → {@code AND(decompose(a), decompose(b))}</li>
     *   <li>{@code OR(a, b)} → {@code OR(decompose(a), decompose(b))}</li>
     *   <li>{@code NOT(a)} → {@code NOT(decompose(a))}</li>
     *   <li>Non-indexed predicates pass through unchanged</li>
     * </ul>
     *
     * @param filter      the original filter RexNode
     * @param indexedCols set of column names that have an associated index
     * @param rexBuilder  Calcite RexBuilder for constructing new nodes
     * @param fieldNames  ordered list of field names matching RexInputRef indices
     * @return rewritten RexNode with index_filter() calls for indexed columns
     */
    public RexNode decompose(RexNode filter, Set<String> indexedCols, RexBuilder rexBuilder, List<String> fieldNames) {
        if (filter instanceof RexCall) {
            RexCall call = (RexCall) filter;
            switch (call.getKind()) {
                case AND:
                    return rewriteBoolean(call, indexedCols, rexBuilder, fieldNames);
                case OR:
                    return rewriteBoolean(call, indexedCols, rexBuilder, fieldNames);
                case NOT:
                    return rewriteNot(call, indexedCols, rexBuilder, fieldNames);
                case EQUALS:
                    return rewriteEquals(call, indexedCols, rexBuilder, fieldNames);
                default:
                    return filter;
            }
        }
        return filter;
    }

    /**
     * Rewrites AND/OR nodes by recursively decomposing each operand.
     */
    private RexNode rewriteBoolean(RexCall call, Set<String> indexedCols, RexBuilder rexBuilder, List<String> fieldNames) {
        List<RexNode> newOperands = new ArrayList<>(call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            newOperands.add(decompose(operand, indexedCols, rexBuilder, fieldNames));
        }
        return rexBuilder.makeCall(call.getOperator(), newOperands);
    }

    /**
     * Rewrites NOT nodes by recursively decomposing the inner operand.
     */
    private RexNode rewriteNot(RexCall call, Set<String> indexedCols, RexBuilder rexBuilder, List<String> fieldNames) {
        RexNode inner = decompose(call.getOperands().get(0), indexedCols, rexBuilder, fieldNames);
        return rexBuilder.makeCall(call.getOperator(), inner);
    }

    /**
     * Rewrites equality predicates on indexed columns to index_filter calls.
     * {@code column = 'value'} where column ∈ indexedCols →
     * {@code index_filter('column', 'value')}
     */
    private RexNode rewriteEquals(RexCall call, Set<String> indexedCols, RexBuilder rexBuilder, List<String> fieldNames) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        // Check pattern: RexInputRef = RexLiteral
        String columnName = null;
        RexLiteral literal = null;

        if (left instanceof RexInputRef && right instanceof RexLiteral) {
            int index = ((RexInputRef) left).getIndex();
            if (index >= 0 && index < fieldNames.size()) {
                columnName = fieldNames.get(index);
            }
            literal = (RexLiteral) right;
        } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
            int index = ((RexInputRef) right).getIndex();
            if (index >= 0 && index < fieldNames.size()) {
                columnName = fieldNames.get(index);
            }
            literal = (RexLiteral) left;
        }

        if (columnName != null && indexedCols.contains(columnName) && literal != null) {
            String value = literalToString(literal);
            RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
            RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
            RexNode colArg = rexBuilder.makeLiteral(columnName, varcharType, false);
            RexNode valArg = rexBuilder.makeLiteral(value, varcharType, false);
            return rexBuilder.makeCall(INDEX_FILTER_FUNCTION, colArg, valArg);
        }

        // Not an indexed column — pass through unchanged
        return call;
    }

    /**
     * Converts a RexLiteral to its string representation.
     */
    private static String literalToString(RexLiteral literal) {
        Object value = literal.getValue2();
        return value != null ? value.toString() : "";
    }
}
