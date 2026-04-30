/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Converts {@code TIMESTAMP(varchar_literal)} into a {@code TIMESTAMP} literal with
 * precision derived from the field's mapping type (date→3, date_nanos→9).
 *
 * <p>Registered as a {@link ScalarFunctionAdapter} for {@code ScalarFunction.TIMESTAMP}.
 * {@link org.opensearch.analytics.planner.dag.BackendPlanAdapter} calls this after plan
 * forking, passing the {@code TIMESTAMP(varchar)} RexCall directly.
 *
 * @opensearch.internal
 */
class TimestampFunctionAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1
            || !(original.getOperands().get(0) instanceof RexLiteral literal)
            || literal.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            return original;
        }
        int precision = resolveTimestampPrecision(original, fieldStorage);
        if (precision < 0) {
            return original;
        }
        String value = literal.getValueAs(String.class);
        if (value == null) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        return rexBuilder.makeTimestampLiteral(parseTimestamp(value), precision);
    }

    /**
     * Resolves timestamp precision from field storage. Scans all fields for date/date_nanos
     * since the TIMESTAMP(varchar) call itself has no field reference — the field ref is
     * in the parent comparison (e.g., $0 in >($0, TIMESTAMP('...'))).
     */
    private int resolveTimestampPrecision(RexCall call, List<FieldStorageInfo> fieldStorage) {
        for (FieldStorageInfo field : fieldStorage) {
            String mappingType = field.getMappingType();
            if ("date_nanos".equals(mappingType)) return 9;
            if ("date".equals(mappingType)) return 3;
        }
        return -1;
    }

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
