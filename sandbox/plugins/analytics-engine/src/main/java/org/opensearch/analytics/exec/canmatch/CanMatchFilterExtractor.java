/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Walks a Calcite {@link RelNode} plan tree and extracts range predicates
 * suitable for can-match evaluation. Produces a flat list of
 * {@link CanMatchFilter} (AND semantics across the list).
 *
 * <p>Only conjunctive (AND) predicates are extracted. Top-level OR
 * causes the extractor to return an empty list (no can-match for that query).
 *
 * @opensearch.internal
 */
public final class CanMatchFilterExtractor {

    private static final Logger logger = LogManager.getLogger(CanMatchFilterExtractor.class);

    private CanMatchFilterExtractor() {}

    /**
     * Extract can-match filters from the plan. Returns an empty list if
     * no extractable range predicates are found, or if the filter structure
     * is not supported (e.g. top-level OR).
     */
    public static List<CanMatchFilter> extract(RelNode plan) {
        Filter filterNode = findFilter(plan);
        if (filterNode == null) {
            logger.debug("can-match extractor: no Filter node found in plan");
            return Collections.emptyList();
        }
        RexNode condition = filterNode.getCondition();
        List<RelDataTypeField> fields = filterNode.getInput().getRowType().getFieldList();
        List<CanMatchFilter> filters = new ArrayList<>();
        extractFromCondition(condition, fields, filters);
        logger.debug("can-match extractor: extracted {} filters from condition {}", filters.size(), condition);
        return filters;
    }

    private static Filter findFilter(RelNode node) {
        if (node instanceof Filter filter) {
            return filter;
        }
        for (RelNode input : node.getInputs()) {
            Filter found = findFilter(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static void extractFromCondition(RexNode condition, List<RelDataTypeField> fields, List<CanMatchFilter> filters) {
        if (!(condition instanceof RexCall call)) {
            return;
        }

        switch (call.getKind()) {
            case AND -> {
                for (RexNode operand : call.getOperands()) {
                    extractFromCondition(operand, fields, filters);
                }
            }
            case OR -> {
                // OR subtree: can't extract range from a disjunction. Skip this branch
                // without clearing filters already extracted from AND siblings.
                logger.debug("can-match extractor: OR detected, skipping this branch");
            }
            case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> extractComparison(call, fields, filters);
            case BETWEEN -> extractBetween(call, fields, filters);
            case SEARCH -> extractSarg(call, fields, filters);
            case OTHER_FUNCTION -> {
                // Unwrap AnnotatedPredicate — it wraps the real predicate as its single operand
                if (call.getOperands().size() == 1) {
                    extractFromCondition(call.getOperands().get(0), fields, filters);
                }
            }
            default -> {
                // Non-range predicate (equality, LIKE, etc.) — skip
            }
        }
    }

    private static void extractComparison(RexCall call, List<RelDataTypeField> fields, List<CanMatchFilter> filters) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        // Identify which operand is the column ref and which is the literal
        String columnName;
        long literalValue;
        boolean columnIsLeft;

        if (left instanceof RexInputRef ref) {
            columnName = fields.get(ref.getIndex()).getName();
            literalValue = resolveNumericValue(right);
            if (literalValue == Long.MIN_VALUE) return;
            columnIsLeft = true;
        } else if (right instanceof RexInputRef ref) {
            columnName = fields.get(ref.getIndex()).getName();
            literalValue = resolveNumericValue(left);
            if (literalValue == Long.MIN_VALUE) return;
            columnIsLeft = false;
        } else {
            return; // not a simple column-vs-literal comparison
        }

        long min = Long.MIN_VALUE;
        long max = Long.MAX_VALUE;

        SqlKind kind = call.getKind();
        // Normalize: if column is on the right, flip the operator
        if (!columnIsLeft) {
            kind = kind.reverse();
        }

        switch (kind) {
            case GREATER_THAN -> min = saturateAdd(literalValue, 1);
            case GREATER_THAN_OR_EQUAL -> min = literalValue;
            case LESS_THAN -> max = saturateSubtract(literalValue, 1);
            case LESS_THAN_OR_EQUAL -> max = literalValue;
            default -> {
                return;
            }
        }

        filters.add(new LongRange(columnName, min, max));
    }

    private static void extractBetween(RexCall call, List<RelDataTypeField> fields, List<CanMatchFilter> filters) {
        // BETWEEN has 3 operands: column, low, high
        if (call.getOperands().size() != 3) return;
        RexNode columnNode = call.getOperands().get(0);
        RexNode lowNode = call.getOperands().get(1);
        RexNode highNode = call.getOperands().get(2);

        if (!(columnNode instanceof RexInputRef ref)) return;

        long low = resolveNumericValue(lowNode);
        long high = resolveNumericValue(highNode);
        if (low == Long.MIN_VALUE || high == Long.MIN_VALUE) return;

        String columnName = fields.get(ref.getIndex()).getName();
        filters.add(new LongRange(columnName, low, high));
    }

    private static void extractSarg(RexCall call, List<RelDataTypeField> fields, List<CanMatchFilter> filters) {
        // SEARCH(column, Sarg) — Calcite's collapsed range representation
        if (call.getOperands().size() != 2) return;
        RexNode columnNode = call.getOperands().get(0);
        RexNode sargNode = call.getOperands().get(1);

        if (!(columnNode instanceof RexInputRef ref)) return;
        if (!(sargNode instanceof RexLiteral sargLiteral)) return;

        // Sarg is stored as a RexLiteral with a Sarg value
        Object sargValue = sargLiteral.getValue();
        if (sargValue == null) return;
        if (!(sargValue instanceof org.apache.calcite.util.Sarg<?> sarg)) return;

        String columnName = fields.get(ref.getIndex()).getName();

        // Extract the overall min/max from the Sarg's range set
        // For can-match, we take the bounding range [min of all ranges, max of all ranges]
        // This is a conservative over-approximation for disjoint Sarg ranges
        long overallMin = Long.MAX_VALUE;
        long overallMax = Long.MIN_VALUE;

        for (Range<?> range : sarg.rangeSet.asRanges()) {
            if (range.hasLowerBound()) {
                Object lb = range.lowerEndpoint();
                if (!(lb instanceof Number n) || !isIntegerValued(n)) return;
                long lo = n.longValue();
                if (range.lowerBoundType() == BoundType.OPEN) {
                    lo = saturateAdd(lo, 1);
                }
                overallMin = Math.min(overallMin, lo);
            } else {
                overallMin = Long.MIN_VALUE;
            }
            if (range.hasUpperBound()) {
                Object ub = range.upperEndpoint();
                if (!(ub instanceof Number n) || !isIntegerValued(n)) return;
                long hi = n.longValue();
                if (range.upperBoundType() == BoundType.OPEN) {
                    hi = saturateSubtract(hi, 1);
                }
                overallMax = Math.max(overallMax, hi);
            } else {
                overallMax = Long.MAX_VALUE;
            }
        }

        if (overallMin <= overallMax) {
            filters.add(new LongRange(columnName, overallMin, overallMax));
        }
    }

    /**
     * Resolves a RexNode to a long value. Handles:
     * - RexLiteral directly (numeric types)
     * - RexCall wrapping a literal: TIMESTAMP('...'), CAST('...':TIMESTAMP), etc.
     */
    private static long resolveNumericValue(RexNode node) {
        if (node instanceof RexLiteral lit) {
            return extractLongValue(lit);
        }
        if (node instanceof RexCall call) {
            // TIMESTAMP('2026-07-13':VARCHAR) or CAST(literal AS TIMESTAMP)
            // Dig through to find the inner RexLiteral
            for (RexNode operand : call.getOperands()) {
                if (operand instanceof RexLiteral innerLit) {
                    // PPL produces TIMESTAMP('2026-07-13':VARCHAR) — try string parse first
                    long val = extractTimestampFromStringLiteral(innerLit);
                    if (val != Long.MIN_VALUE) return val;
                    // Fallback: literal might already be numeric (e.g. epoch millis directly)
                    val = extractLongValue(innerLit);
                    if (val != Long.MIN_VALUE) return val;
                }
                // Recurse one level deeper (e.g. CAST(TIMESTAMP(...)))
                if (operand instanceof RexCall innerCall) {
                    long val = resolveNumericValue(innerCall);
                    if (val != Long.MIN_VALUE) return val;
                }
            }
        }
        return Long.MIN_VALUE;
    }

    /**
     * Parses a string-typed RexLiteral containing a timestamp/date string
     * into epoch millis.
     */
    private static long extractTimestampFromStringLiteral(RexLiteral literal) {
        if (literal.isNull()) return Long.MIN_VALUE;
        SqlTypeName typeName = literal.getTypeName();
        // String literal representing a timestamp (e.g. '2026-07-13')
        if (typeName == SqlTypeName.VARCHAR || typeName == SqlTypeName.CHAR) {
            String strValue = literal.getValueAs(String.class);
            if (strValue == null) return Long.MIN_VALUE;
            return parseTimestampString(strValue);
        }
        // Already a timestamp/date-typed literal
        if (typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE || typeName == SqlTypeName.DATE) {
            try {
                Long val = literal.getValueAs(Long.class);
                return val != null ? val : Long.MIN_VALUE;
            } catch (Exception e) {
                return Long.MIN_VALUE;
            }
        }
        return Long.MIN_VALUE;
    }

    /**
     * Parses common timestamp/date string formats into epoch millis.
     */
    private static long parseTimestampString(String str) {
        try {
            // Try ISO date-time: 2026-07-13T10:00:00Z or 2026-07-13 10:00:00
            if (str.contains("T") || str.contains(" ")) {
                Instant instant = Instant.parse(str.replace(" ", "T") + (str.endsWith("Z") ? "" : (str.contains("T") ? "" : "Z")));
                return instant.toEpochMilli();
            }
            // Try date-only: 2026-07-13 → start of day UTC
            LocalDate date = LocalDate.parse(str);
            return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (Exception e) {
            logger.debug("[can-match-extractor] Failed to parse timestamp string '{}': {}", str, e.getMessage());
            return Long.MIN_VALUE;
        }
    }

    private static long extractLongValue(RexLiteral literal) {
        if (literal.isNull()) return Long.MIN_VALUE;

        // RexLiteral.getValue() returns Comparable — typically BigDecimal for numerics
        Comparable<?> value = literal.getValue();
        if (value == null) return Long.MIN_VALUE;

        if (value instanceof Number n) {
            if (!isIntegerValued(n)) return Long.MIN_VALUE;
            return n.longValue();
        }

        // Try getValueAs for numeric types — handles BigDecimal → Long conversion
        SqlTypeName typeName = literal.getTypeName();
        switch (typeName) {
            case INTEGER, BIGINT, SMALLINT, TINYINT, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, DATE -> {
                try {
                    Long longVal = literal.getValueAs(Long.class);
                    return longVal != null ? longVal : Long.MIN_VALUE;
                } catch (Exception e) {
                    return Long.MIN_VALUE;
                }
            }
            default -> {
                return Long.MIN_VALUE;
            }
        }
    }

    private static boolean isIntegerValued(Number n) {
        return n.doubleValue() == Math.floor(n.doubleValue()) && !Double.isInfinite(n.doubleValue());
    }

    private static long saturateAdd(long a, long b) {
        long result = a + b;
        if (b > 0 && result < a) return Long.MAX_VALUE; // overflow
        return result;
    }

    private static long saturateSubtract(long a, long b) {
        long result = a - b;
        if (b > 0 && result > a) return Long.MIN_VALUE; // underflow
        return result;
    }
}
