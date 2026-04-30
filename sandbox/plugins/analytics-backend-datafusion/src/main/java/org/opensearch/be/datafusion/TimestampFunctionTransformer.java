/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.analytics.spi.RexNodeTransformer;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Folds {@code TIMESTAMP(varchar_literal)} into a {@code TIMESTAMP} literal with
 * precision derived from the field's mapping type (date→3, date_nanos→9).
 *
 * @opensearch.internal
 */
class TimestampFunctionTransformer implements RexNodeTransformer {

    private static final String TIMESTAMP_FUNCTION_NAME = "TIMESTAMP";

    @Override
    public RexNode transform(RexNode node, RexBuilder rexBuilder, FieldMappingLookup fieldMappingTypes) {
        if (!(node instanceof RexCall call)) {
            return node;
        }
        int precision = resolveTimestampPrecision(call, fieldMappingTypes);
        if (precision < 0) {
            return node;
        }
        boolean changed = false;
        List<RexNode> newOperands = new ArrayList<>(call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            RexNode converted = convertTimestampFunction(operand, rexBuilder, precision);
            newOperands.add(converted);
            if (converted != operand) {
                changed = true;
            }
        }
        return changed ? call.clone(call.getType(), newOperands) : node;
    }

    private RexNode convertTimestampFunction(RexNode operand, RexBuilder rexBuilder, int precision) {
        if (operand instanceof RexCall call
            && call.getOperands().size() == 1
            && TIMESTAMP_FUNCTION_NAME.equals(call.getOperator().getName())
            && call.getOperands().get(0) instanceof RexLiteral literal
            && literal.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
            String value = literal.getValueAs(String.class);
            if (value != null) {
                return rexBuilder.makeTimestampLiteral(parseTimestamp(value), precision);
            }
        }
        return operand;
    }

    private int resolveTimestampPrecision(RexNode node, FieldMappingLookup lookup) {
        Set<Integer> fieldIndices = new HashSet<>();
        collectFieldIndices(node, fieldIndices);
        for (int idx : fieldIndices) {
            String mappingType = lookup.getMappingType(idx);
            if ("date".equals(mappingType)) return 3;
            if ("date_nanos".equals(mappingType)) return 9;
        }
        return -1;
    }

    private void collectFieldIndices(RexNode node, Set<Integer> result) {
        if (node instanceof RexInputRef inputRef) {
            result.add(inputRef.getIndex());
        } else if (node instanceof RexCall rexCall) {
            for (RexNode operand : rexCall.getOperands()) {
                collectFieldIndices(operand, result);
            }
        }
    }

    /**
     * Parses a timestamp string into Calcite's {@link TimestampString}.
     * Handles ISO-8601, date-only, timezone offsets, and sub-second precision.
     */
    TimestampString parseTimestamp(String input) {
        try {
            LocalDate date = LocalDate.parse(input);
            return toTimestampString(date.atStartOfDay());
        } catch (DateTimeParseException ignored) {}

        try {
            OffsetDateTime odt = OffsetDateTime.parse(input);
            return toTimestampString(LocalDateTime.ofInstant(odt.toInstant(), ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {}

        try {
            Instant instant = Instant.parse(input);
            return toTimestampString(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {}

        try {
            LocalDateTime ldt = LocalDateTime.parse(input);
            return toTimestampString(ldt);
        } catch (DateTimeParseException ignored) {}

        return new TimestampString(input);
    }

    private TimestampString toTimestampString(LocalDateTime ldt) {
        TimestampString ts = new TimestampString(
            ldt.getYear(),
            ldt.getMonthValue(),
            ldt.getDayOfMonth(),
            ldt.getHour(),
            ldt.getMinute(),
            ldt.getSecond()
        );
        int nanos = ldt.getNano();
        if (nanos > 0) {
            ts = ts.withNanos(nanos);
        }
        return ts;
    }
}
