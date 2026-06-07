/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * Plan-time validation for string-literal datetime operands. Surfaces malformed inputs as
 * {@link IllegalArgumentException} with the legacy SQL-plugin format-hint message
 * ({@code <kind>:<value> in unsupported format, please use '<pattern>'}) before the literal
 * reaches DataFusion's CAST kernel; the native Arrow parser error is dropped by Flight RPC
 * serialization on the worker-to-coordinator hop, leaving users with an opaque
 * {@code StreamException} otherwise.
 *
 * <p>Accept-sets:
 * <ul>
 *   <li>{@link Kind#DATE} — accepts {@code yyyy-MM-dd}; rejects bare time.
 *   <li>{@link Kind#TIME} — accepts {@code HH:mm[:ss[.fraction]]}; rejects strings containing a date.
 *   <li>{@link Kind#TIMESTAMP} — accepts full datetime ({@code yyyy-MM-dd[ T]HH:mm:ss[.fraction]})
 *       and bare date; rejects bare time (matches legacy
 *       {@code cast('09:07:42' AS TIMESTAMP)} reject semantics).
 * </ul>
 *
 * @opensearch.internal
 */
final class DatetimeLiteralValidator {

    private DatetimeLiteralValidator() {}

    /** Validation flavor; controls accept-set and the format-hint message wording. */
    enum Kind {
        DATE("date", "yyyy-MM-dd"),
        TIME("time", "HH:mm:ss[.SSSSSSSSS]"),
        TIMESTAMP("timestamp", "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]");

        final String typeName;
        final String pattern;

        Kind(String typeName, String pattern) {
            this.typeName = typeName;
            this.pattern = pattern;
        }
    }

    /** No-op for non-RexLiteral / non-string / NULL operands. */
    static void validate(RexNode operand, Kind kind) {
        if (!(operand instanceof RexLiteral literal)) {
            return;
        }
        String value = literal.getValueAs(String.class);
        if (value == null) {
            return;
        }
        validate(value, kind);
    }

    /** Throws {@link IllegalArgumentException} with the format-hint message when {@code value} is malformed. */
    static void validate(String value, Kind kind) {
        if (kind == Kind.DATE && !isParseableAsDateOrFullDatetime(value)) {
            throw fail(value, kind);
        }
        if (kind == Kind.TIME && !isParseableAsTimeOrFullDatetime(value)) {
            throw fail(value, kind);
        }
        if (kind == Kind.TIMESTAMP && !isParseableAsTimestamp(value)) {
            throw fail(value, kind);
        }
    }

    /** TIME accepts bare time and full datetime ({@code cast('1985-10-09 12:00:00' AS TIME)} → time portion). */
    private static boolean isParseableAsTimeOrFullDatetime(String value) {
        if (isParseableAsTime(value)) {
            return true;
        }
        try {
            LocalDateTime.parse(value.replace(' ', 'T'));
            return true;
        } catch (DateTimeParseException ignored) {
            return false;
        }
    }

    /** DATE accepts bare date and full datetime ({@code cast('2023-10-01 12:00:00' AS DATE)} → date portion). */
    private static boolean isParseableAsDateOrFullDatetime(String value) {
        if (isParseableAsDate(value)) {
            return true;
        }
        try {
            LocalDateTime.parse(value.replace(' ', 'T'));
            return true;
        } catch (DateTimeParseException ignored) {
            return false;
        }
    }

    private static boolean isParseableAsDate(String value) {
        try {
            LocalDate.parse(value);
            return true;
        } catch (DateTimeParseException ignored) {
            return false;
        }
    }

    private static boolean isParseableAsTime(String value) {
        try {
            LocalTime.parse(value);
            return true;
        } catch (DateTimeParseException ignored) {
            return false;
        }
    }

    /** Accepts full timestamp or bare date; bare time rejected (matches legacy CAST behavior). */
    private static boolean isParseableAsTimestamp(String value) {
        try {
            LocalDateTime.parse(value.replace(' ', 'T'));
            return true;
        } catch (DateTimeParseException ignored) {}
        return isParseableAsDate(value);
    }

    private static IllegalArgumentException fail(String value, Kind kind) {
        // causeless to keep ErrorMessageFactory.unwrapCause from surfacing the JDK
        // DateTimeParseException stock message instead of the format-hint wording.
        return new IllegalArgumentException(
            String.format(Locale.ROOT, "%s:%s in unsupported format, please use '%s'", kind.typeName, value, kind.pattern)
        );
    }
}
