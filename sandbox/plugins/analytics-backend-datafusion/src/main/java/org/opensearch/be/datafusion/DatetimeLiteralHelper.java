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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;

/**
 * Shared helper for datetime adapters that need to extract a date-part from a TIME value.
 *
 * <p>TIME operands can't reach DataFusion's {@code date_part} directly: our Substrait yaml
 * doesn't declare a {@code precision_time<P>} sig (declaring one triggers a runtime
 * {@code ParameterizedTypeThrowsVisitor} error on every query). Earlier fallback chained
 * {@code CAST(time AS VARCHAR) AS TIMESTAMP}, but DataFusion's simplifier folds that to
 * {@code cast('HH:MM:SS' AS TIMESTAMP)} and Arrow's parser rejects strings shorter than
 * 10 characters.
 *
 * <p>PPL reference semantics ({@code DateTimeFunctions.exprHour} et al.) treat a bare
 * TIME as {@code LocalDateTime.of(LocalDate.ofEpochDay(0), time)} — date component is
 * 1970-01-01 UTC. This helper pattern-matches {@code LOCAL_TIME_OP(varchar_literal)} at
 * the adapter site (post-operand-adaptation) and synthesizes a TIMESTAMP literal with
 * that date, bypassing the lossy CAST chain. Non-literal TIME operands are not handled
 * here; current ITs exercise only literal-TIME, and a column path would need per-row
 * synthesis which is not in scope.
 *
 * @opensearch.internal
 */
final class DatetimeLiteralHelper {

    private DatetimeLiteralHelper() {}

    /**
     * If {@code operand} is TIME-typed and reducible to a VARCHAR literal (either the raw
     * VARCHAR literal or a {@code LOCAL_TIME_OP(varchar_literal)} wrapper), returns a
     * TIMESTAMP literal pinned to 1970-01-01 with the parsed time component; otherwise
     * returns null so the caller can fall back to its default handling.
     */
    static RexNode unwrapTimeLiteralToTimestamp(RexNode operand, RexBuilder rexBuilder) {
        if (operand.getType().getSqlTypeName() != SqlTypeName.TIME) {
            return null;
        }
        String timeString = extractVarcharLiteral(operand);
        if (timeString == null) {
            return null;
        }
        LocalTime time;
        try {
            time = LocalTime.parse(timeString);
        } catch (DateTimeParseException ignored) {
            return null;
        }
        TimestampString ts = new TimestampString(1970, 1, 1, time.getHour(), time.getMinute(), time.getSecond());
        if (time.getNano() > 0) {
            ts = ts.withNanos(time.getNano());
        }
        return rexBuilder.makeTimestampLiteral(ts, 9);
    }

    private static String extractVarcharLiteral(RexNode operand) {
        // Recursively unwrap single-operand RexCalls (OperatorAnnotation wrappers,
        // Calcite's TIME(varchar) constructor, the adapter-renamed LOCAL_TIME_OP)
        // until we reach a VARCHAR RexLiteral or give up.
        RexNode current = operand;
        for (int depth = 0; depth < 5; depth++) {
            if (current instanceof RexLiteral lit && lit.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
                return lit.getValueAs(String.class);
            }
            if (current instanceof RexCall call && call.getOperands().size() == 1) {
                current = call.getOperands().get(0);
                continue;
            }
            return null;
        }
        return null;
    }
}
