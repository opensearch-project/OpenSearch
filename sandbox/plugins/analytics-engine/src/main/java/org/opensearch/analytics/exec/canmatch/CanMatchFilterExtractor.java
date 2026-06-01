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
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;

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
        // Unwrap planner annotations (AnnotatedPredicate extends RexCall but its SqlKind is
        // OTHER_FUNCTION — instanceof check must come BEFORE the generic RexCall handling).
        if (condition instanceof AnnotatedPredicate annotated) {
            extractFromCondition(annotated.getOriginal(), input, filters);
            return;
        }
        if (condition instanceof RexCall call) {
            SqlKind kind = call.getKind();
            List<RexNode> operands = call.getOperands();

            switch (kind) {
                case AND -> {
                    for (RexNode operand : operands) {
                        extractFromCondition(operand, input, filters);
                    }
                }
                case GREATER_THAN -> extractRangeBound(operands, input, filters, true, true);
                case GREATER_THAN_OR_EQUAL -> extractRangeBound(operands, input, filters, true, false);
                case LESS_THAN -> extractRangeBound(operands, input, filters, false, true);
                case LESS_THAN_OR_EQUAL -> extractRangeBound(operands, input, filters, false, false);
                case BETWEEN -> {
                    // BETWEEN x AND a AND b → x ∈ [a, b]. Calcite normally rewrites to two
                    // comparisons via ReduceExpressionsRule, but if it survives we handle it.
                    if (operands.size() == 3 && operands.get(0) instanceof RexInputRef ref
                        && operands.get(1) instanceof RexLiteral lo
                        && operands.get(2) instanceof RexLiteral hi) {
                        String col = resolveColumnName(ref, input);
                        Long loVal = extractLongValue(lo);
                        Long hiVal = extractLongValue(hi);
                        if (col != null && loVal != null && hiVal != null) {
                            filters.add(new CanMatchFilter(col, loVal, hiVal));
                        }
                    }
                }
                case SEARCH -> {
                    // Calcite folds conjunctions of range bounds into SEARCH(col, Sarg).
                    // E.g. `value > 0 AND value < 1000` → SEARCH($0, Sarg[(0..1000)]).
                    // Extract a single-range Sarg as the corresponding CanMatchFilter.
                    extractFromSearch(operands, input, filters);
                }
                default -> {
                    // Not a range predicate we can push down
                }
            }
        }
    }

    private static void extractRangeBound(
        List<RexNode> operands,
        RelNode input,
        List<CanMatchFilter> filters,
        boolean isLowerBound,
        boolean strict
    ) {
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
                isLowerBound = !isLowerBound; // flip direction; strictness is preserved
                found = true;
            }
        }

        if (found && columnName != null) {
            if (isLowerBound) {
                // Strict `> N` → smallest matching long is N+1. Saturate at MAX_VALUE.
                long lo = strict && literalValue < Long.MAX_VALUE ? literalValue + 1 : literalValue;
                filters.add(new CanMatchFilter(columnName, lo, Long.MAX_VALUE));
            } else {
                long hi = strict && literalValue > Long.MIN_VALUE ? literalValue - 1 : literalValue;
                filters.add(new CanMatchFilter(columnName, Long.MIN_VALUE, hi));
            }
        }
    }

    /**
     * Handles the {@code SEARCH(ref, Sarg[..])} form Calcite uses to collapse multi-bound
     * conjunctions and disjunctions. We only extract when the Sarg is a single range — multi-
     * range Sargs (UNIONs / NOT) are skipped (conservative).
     */
    private static void extractFromSearch(List<RexNode> operands, RelNode input, List<CanMatchFilter> filters) {
        if (operands.size() != 2 || !(operands.get(0) instanceof RexInputRef ref) || !(operands.get(1) instanceof RexLiteral lit)) {
            return;
        }
        Object sargVal = lit.getValue();
        if (!(sargVal instanceof org.apache.calcite.util.Sarg<?> sarg)) {
            return;
        }
        if (sarg.nullAs == org.apache.calcite.rex.RexUnknownAs.TRUE) {
            // nullAs=TRUE means rows where the column IS NULL match the predicate. Without
            // a null-count check on the data-node side, an all-nulls row group would be
            // incorrectly eliminated. Skip — fail-open. UNKNOWN and FALSE both exclude
            // null rows from the result set, so the range check is sufficient.
            return;
        }
        // RangeSet may contain multiple disjoint ranges (OR). We only handle the single-range case.
        java.util.Set<? extends com.google.common.collect.Range<?>> ranges = sarg.rangeSet.asRanges();
        if (ranges.size() != 1) {
            return;
        }
        com.google.common.collect.Range<?> r = ranges.iterator().next();
        long lo = Long.MIN_VALUE;
        long hi = Long.MAX_VALUE;
        if (r.hasLowerBound()) {
            Object lb = r.lowerEndpoint();
            if (!(lb instanceof Number n) || !isIntegerValued(n)) return;
            lo = n.longValue();
            // OPEN bound on a long means "strictly greater than"; bump by 1 to fold into the
            // closed [lo, hi] CanMatchFilter shape. Saturates at MIN/MAX_VALUE.
            if (r.lowerBoundType() == com.google.common.collect.BoundType.OPEN && lo < Long.MAX_VALUE) {
                lo = lo + 1;
            }
        }
        if (r.hasUpperBound()) {
            Object ub = r.upperEndpoint();
            if (!(ub instanceof Number n) || !isIntegerValued(n)) return;
            hi = n.longValue();
            if (r.upperBoundType() == com.google.common.collect.BoundType.OPEN && hi > Long.MIN_VALUE) {
                hi = hi - 1;
            }
        }
        String col = resolveColumnName(ref, input);
        if (col != null) {
            filters.add(new CanMatchFilter(col, lo, hi));
        }
    }

    private static String resolveColumnName(RexInputRef ref, RelNode input) {
        int idx = ref.getIndex();
        // Walk through renaming projections to find the source column. The extractor's
        // column name flows through to a data-node-side lookup against parquet stats,
        // which uses the ORIGINAL field name — not whatever alias the inner SELECT chose.
        // Composed projections (e.g. `status + 1 AS sev`) can't be reverse-mapped to a
        // single column; bail out (no extraction → fail-open) in that case.
        while (input instanceof org.apache.calcite.rel.core.Project project) {
            if (idx >= project.getProjects().size()) return null;
            RexNode expr = project.getProjects().get(idx);
            if (!(expr instanceof RexInputRef innerRef)) return null;
            idx = innerRef.getIndex();
            input = project.getInput();
        }
        if (input.getRowType() != null && idx < input.getRowType().getFieldCount()) {
            return input.getRowType().getFieldList().get(idx).getName();
        }
        return null;
    }

    private static Long extractLongValue(RexLiteral literal) {
        if (literal.getValue() instanceof Number num) {
            return isIntegerValued(num) ? num.longValue() : null;
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

    /**
     * True when {@code n} carries no fractional component. Truncating a fractional value to
     * a long would over-prune (e.g. {@code col < 9.99} should keep integer 9, but
     * {@code longValue()=9} combined with an OPEN-bound bump drops it).
     */
    private static boolean isIntegerValued(Number n) {
        if (n instanceof java.math.BigDecimal bd) {
            return bd.stripTrailingZeros().scale() <= 0;
        }
        if (n instanceof Double || n instanceof Float) {
            double d = n.doubleValue();
            return Double.isFinite(d) && d == Math.floor(d);
        }
        return true;  // Integer/Long/Short/Byte are always integer-valued
    }
}
