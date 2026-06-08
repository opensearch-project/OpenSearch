/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.time.FormatNames;

import java.util.EnumSet;
import java.util.Set;

/**
 * Classifies an OpenSearch {@code date}/{@code date_nanos} mapping {@code format} string into the
 * SQL logical type it should surface as on the analytics-engine route: {@link SqlTypeName#DATE},
 * {@link SqlTypeName#TIME}, or {@link SqlTypeName#TIMESTAMP}.
 *
 * <p>OpenSearch stores every {@code date} field as an epoch-millisecond timestamp regardless of
 * format, but PPL (like the v2 / Calcite engine) narrows the logical type by format: a date-only
 * format such as {@code basic_date} is a SQL {@code DATE}, a time-only format such as {@code hour}
 * is a SQL {@code TIME}, and anything with both components (or a numeric/epoch format) is a
 * {@code TIMESTAMP}.
 *
 * <p>This mirrors {@code OpenSearchDateType.getExprTypeFromFormatString} in the SQL plugin. The two
 * live in separate repositories today; keep the named-format sets below in sync with
 * {@code OpenSearchDateType.SUPPORTED_NAMED_DATE_FORMATS} / {@code SUPPORTED_NAMED_TIME_FORMATS}.
 *
 * <p>TODO: extract a single shared classifier so the analytics-route and v2 paths cannot drift.
 */
public final class DateFormatClassifier {

    private DateFormatClassifier() {}

    /** OpenSearch allows multiple alternative formats separated by {@code ||}. */
    private static final String FORMAT_DELIMITER = "\\|\\|";

    // Ported from OpenSearchDateType.SUPPORTED_NAMED_DATE_FORMATS (SQL plugin).
    private static final Set<FormatNames> DATE_ONLY_NAMED = EnumSet.of(
        FormatNames.BASIC_DATE,
        FormatNames.BASIC_ORDINAL_DATE,
        FormatNames.DATE,
        FormatNames.STRICT_DATE,
        FormatNames.YEAR_MONTH_DAY,
        FormatNames.STRICT_YEAR_MONTH_DAY,
        FormatNames.ORDINAL_DATE,
        FormatNames.STRICT_ORDINAL_DATE,
        FormatNames.WEEK_DATE,
        FormatNames.STRICT_WEEK_DATE,
        FormatNames.WEEKYEAR_WEEK_DAY,
        FormatNames.STRICT_WEEKYEAR_WEEK_DAY
    );

    // Ported from OpenSearchDateType.SUPPORTED_NAMED_TIME_FORMATS (SQL plugin).
    private static final Set<FormatNames> TIME_ONLY_NAMED = EnumSet.of(
        FormatNames.BASIC_TIME,
        FormatNames.BASIC_TIME_NO_MILLIS,
        FormatNames.BASIC_T_TIME,
        FormatNames.BASIC_T_TIME_NO_MILLIS,
        FormatNames.TIME,
        FormatNames.STRICT_TIME,
        FormatNames.TIME_NO_MILLIS,
        FormatNames.STRICT_TIME_NO_MILLIS,
        FormatNames.HOUR_MINUTE_SECOND_FRACTION,
        FormatNames.STRICT_HOUR_MINUTE_SECOND_FRACTION,
        FormatNames.HOUR_MINUTE_SECOND_MILLIS,
        FormatNames.STRICT_HOUR_MINUTE_SECOND_MILLIS,
        FormatNames.HOUR_MINUTE_SECOND,
        FormatNames.STRICT_HOUR_MINUTE_SECOND,
        FormatNames.HOUR_MINUTE,
        FormatNames.STRICT_HOUR_MINUTE,
        FormatNames.HOUR,
        FormatNames.STRICT_HOUR,
        FormatNames.T_TIME,
        FormatNames.STRICT_T_TIME,
        FormatNames.T_TIME_NO_MILLIS,
        FormatNames.STRICT_T_TIME_NO_MILLIS
    );

    // java.time pattern letters that carry a date component (year/month/day/week/era/ordinal).
    private static final String DATE_PATTERN_LETTERS = "uyYMLDdQqwWEecFgG";
    // java.time pattern letters that carry a time-of-day (or zone/offset) component.
    private static final String TIME_PATTERN_LETTERS = "HhKkmsSAnNVzOXxZa";

    private enum Component {
        DATE,
        TIME,
        OTHER
    }

    /**
     * @param format the OpenSearch mapping {@code format} string (may be {@code null}/blank for the
     *     default {@code strict_date_optional_time||epoch_millis})
     * @return DATE when every alternative is date-only, TIME when every alternative is time-only,
     *     otherwise TIMESTAMP
     */
    public static SqlTypeName classify(String format) {
        if (format == null || format.isBlank()) {
            return SqlTypeName.TIMESTAMP;
        }
        boolean sawComponent = false;
        boolean allDate = true;
        boolean allTime = true;
        for (String piece : format.split(FORMAT_DELIMITER)) {
            String trimmed = piece.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            sawComponent = true;
            Component c = classifyPiece(trimmed);
            allDate &= (c == Component.DATE);
            allTime &= (c == Component.TIME);
        }
        if (!sawComponent) {
            return SqlTypeName.TIMESTAMP;
        }
        if (allDate) {
            return SqlTypeName.DATE;
        }
        if (allTime) {
            return SqlTypeName.TIME;
        }
        return SqlTypeName.TIMESTAMP;
    }

    private static Component classifyPiece(String piece) {
        FormatNames named = FormatNames.forName(piece);
        if (named != null) {
            if (DATE_ONLY_NAMED.contains(named)) {
                return Component.DATE;
            }
            if (TIME_ONLY_NAMED.contains(named)) {
                return Component.TIME;
            }
            return Component.OTHER;
        }
        // Custom pattern: scan the literal-stripped pattern for date vs time letters.
        String stripped = stripLiterals(piece);
        boolean hasDate = containsAny(stripped, DATE_PATTERN_LETTERS);
        boolean hasTime = containsAny(stripped, TIME_PATTERN_LETTERS);
        if (hasDate && !hasTime) {
            return Component.DATE;
        }
        if (hasTime && !hasDate) {
            return Component.TIME;
        }
        return Component.OTHER;
    }

    /** Removes single-quoted literal sections (e.g. the {@code 'T'} in {@code yyyy-MM-dd'T'HH}). */
    private static String stripLiterals(String pattern) {
        return pattern.replaceAll("'[^']*'", "");
    }

    private static boolean containsAny(String s, String letters) {
        for (int i = 0; i < s.length(); i++) {
            if (letters.indexOf(s.charAt(i)) >= 0) {
                return true;
            }
        }
        return false;
    }
}
