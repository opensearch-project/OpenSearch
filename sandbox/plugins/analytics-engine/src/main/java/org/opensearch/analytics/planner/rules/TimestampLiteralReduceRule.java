/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

/**
 * Folds {@code TIMESTAMP(string_literal)} into a {@code TIMESTAMP(3)} literal in filter conditions.
 *
 * <p>TODO: Also handle TIMESTAMP literals in Project (e.g., {@code | eval ts = TIMESTAMP('2024-01-01')}).
 * Add a second rule matching {@code Project.class} or generalize to match any RelNode.
 *
 * @opensearch.internal
 */
public class TimestampLiteralReduceRule extends RelOptRule {

    public static final TimestampLiteralReduceRule INSTANCE = new TimestampLiteralReduceRule();

    private static final String TIMESTAMP_FUNCTION_NAME = "TIMESTAMP";

    private TimestampLiteralReduceRule() {
        super(operand(Filter.class, any()), "TimestampLiteralReduceRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        RexNode rewritten = filter.getCondition().accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                RexCall visited = (RexCall) super.visitCall(call);
                if (visited.getOperands().size() == 1
                    && TIMESTAMP_FUNCTION_NAME.equals(visited.getOperator().getName())
                    && visited.getOperands().get(0) instanceof RexLiteral literal
                    && literal.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
                    String value = literal.getValueAs(String.class);
                    if (value != null) {
                        return rexBuilder.makeTimestampLiteral(parseTimestamp(value), 3);
                    }
                }
                return visited;
            }
        });
        if (rewritten != filter.getCondition()) {
            call.transformTo(filter.copy(filter.getTraitSet(), filter.getInput(), rewritten));
        }
    }

    /**
     * Parses a timestamp string into Calcite's {@link TimestampString}.
     * Handles: ISO-8601 with T/Z ({@code 2024-01-01T00:00:00Z}), date-only ({@code 2024-01-01}),
     * timezone offsets ({@code 2024-01-01T10:00:00+05:30}), and sub-second precision.
     */
    static TimestampString parseTimestamp(String input) {
        // Try date-only (yyyy-MM-dd)
        try {
            LocalDate date = LocalDate.parse(input);
            return toTimestampString(date.atStartOfDay());
        } catch (DateTimeParseException ignored) {}

        // Try with timezone offset (converts to UTC via Instant)
        try {
            OffsetDateTime odt = OffsetDateTime.parse(input);
            return toTimestampString(LocalDateTime.ofInstant(odt.toInstant(), ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {}

        // Try Instant (handles Z suffix)
        try {
            Instant instant = Instant.parse(input);
            return toTimestampString(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {}

        // Try local datetime with T separator
        try {
            LocalDateTime ldt = LocalDateTime.parse(input);
            return toTimestampString(ldt);
        } catch (DateTimeParseException ignored) {}

        // Fallback: assume already in Calcite format
        return new TimestampString(input);
    }

    private static TimestampString toTimestampString(LocalDateTime ldt) {
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
