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

/** Plan-time validation for string-literal datetime operands; rejects malformed input with the format-hint message. */
final class DatetimeLiteralValidator {

    private DatetimeLiteralValidator() {}

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

    static void validate(String value, Kind kind) {
        boolean ok = switch (kind) {
            case DATE -> isParseableAsDate(value) || isParseableAsFullDatetime(value);
            case TIME -> isParseableAsTime(value) || isParseableAsFullDatetime(value);
            case TIMESTAMP -> isParseableAsFullDatetime(value) || isParseableAsDate(value);
        };
        if (!ok) {
            throw fail(value, kind);
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

    private static boolean isParseableAsFullDatetime(String value) {
        try {
            LocalDateTime.parse(value.replace(' ', 'T'));
            return true;
        } catch (DateTimeParseException ignored) {
            return false;
        }
    }

    /** Causeless: ErrorMessageFactory.unwrapCause would surface the JDK DateTimeParseException's stock message otherwise. */
    private static IllegalArgumentException fail(String value, Kind kind) {
        return new IllegalArgumentException(
            String.format(Locale.ROOT, "%s:%s in unsupported format, please use '%s'", kind.typeName, value, kind.pattern)
        );
    }
}
