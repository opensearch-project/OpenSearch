/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts simple range predicates from a logical plan fragment suitable for
 * can-match evaluation. Only extracts predicates that can be cheaply checked
 * against Parquet row-group statistics:
 * <ul>
 *   <li>{@code column > literal} / {@code column >= literal}</li>
 *   <li>{@code column < literal} / {@code column <= literal}</li>
 *   <li>{@code column BETWEEN literal AND literal}</li>
 * </ul>
 *
 * <p>Returns empty list if no extractable predicates are found (conservative: all shards match).
 */
public final class CanMatchFilterExtractor {

    private CanMatchFilterExtractor() {}

    /**
     * Walk the plan tree looking for Filter nodes, extract range predicates.
     */
    public static List<CanMatchFilter> extract(RelNode plan) {
        List<CanMatchFilter> filters = new ArrayList<>();
        extractRecursive(plan, filters);
        return filters;
    }

    private static void extractRecursive(RelNode node, List<CanMatchFilter> filters) {
        if (node instanceof Filter filter) {
            extractFromCondition(filter.getCondition(), filter.getInput(), filters);
        }
        for (RelNode input : node.getInputs()) {
            extractRecursive(input, filters);
        }
    }

    private static void extractFromCondition(RexNode condition, RelNode input, List<CanMatchFilter> filters) {
        if (condition instanceof RexCall call) {
            SqlKind kind = call.getKind();
            List<RexNode> operands = call.getOperands();

            switch (kind) {
                case AND -> {
                    for (RexNode operand : operands) {
                        extractFromCondition(operand, input, filters);
                    }
                }
                case GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                    extractRangeBound(operands, input, filters, true);
                }
                case LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    extractRangeBound(operands, input, filters, false);
                }
                default -> {
                    // Not a range predicate we can push down
                }
            }
        }
    }

    private static void extractRangeBound(List<RexNode> operands, RelNode input, List<CanMatchFilter> filters, boolean isLowerBound) {
        if (operands.size() != 2) return;

        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        String columnName = null;
        long literalValue = 0;
        boolean found = false;

        if (left instanceof RexInputRef ref && right instanceof RexLiteral literal) {
            columnName = resolveColumnName(ref, input);
            Long val = extractLongValue(literal);
            if (val != null) {
                literalValue = val;
                found = true;
            }
        } else if (right instanceof RexInputRef ref && left instanceof RexLiteral literal) {
            columnName = resolveColumnName(ref, input);
            Long val = extractLongValue(literal);
            if (val != null) {
                literalValue = val;
                isLowerBound = !isLowerBound; // flip direction
                found = true;
            }
        }

        if (found && columnName != null) {
            if (isLowerBound) {
                filters.add(new CanMatchFilter(columnName, literalValue, Long.MAX_VALUE));
            } else {
                filters.add(new CanMatchFilter(columnName, Long.MIN_VALUE, literalValue));
            }
        }
    }

    private static String resolveColumnName(RexInputRef ref, RelNode input) {
        int idx = ref.getIndex();
        if (input.getRowType() != null && idx < input.getRowType().getFieldCount()) {
            return input.getRowType().getFieldList().get(idx).getName();
        }
        return null;
    }

    private static Long extractLongValue(RexLiteral literal) {
        if (literal.getValue() instanceof Number num) {
            return num.longValue();
        }
        // Handle timestamp literals (epoch millis)
        if (literal.getTypeName().getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.TIMESTAMP
            || literal.getTypeName().getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.DATE) {
            Object value = literal.getValue();
            if (value instanceof Number num) {
                return num.longValue();
            }
        }
        return null;
    }
}
