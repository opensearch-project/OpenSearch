/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Classifies the {@code format} attribute of an OpenSearch {@code date} field as
 * date-only, time-only, or full timestamp. Mirrors the SQL-plugin rules in
 * {@code OpenSearchDateType} (named-format allow-lists + custom-pattern symbol scan)
 * without taking a dependency on that package — sandbox sits below sql-core in the
 * dependency graph.
 *
 * <p>The format string follows OpenSearch's {@code DateFormatter} grammar: one or more
 * named formats or custom patterns, separated by {@code ||}. The classifier returns
 * {@link Kind#TIMESTAMP} if any component requires both date and time fields, or if
 * the components disagree (e.g. {@code "yyyy-MM-dd || HH:mm:ss"}).
 */
public final class DateFormatClassifier {

    /** Output classification for a {@code date} mapping. */
    public enum Kind {
        DATE_ONLY,
        TIME_ONLY,
        TIMESTAMP
    }

    /** Named formats that contain only year/month/day components (mirrors SQL plugin's list). */
    private static final Set<String> NAMED_DATE_FORMATS = setOf(
        "basic_date",
        "basic_ordinal_date",
        "date",
        "strict_date",
        "year_month_day",
        "strict_year_month_day",
        "ordinal_date",
        "strict_ordinal_date",
        "week_date",
        "strict_week_date",
        "weekyear_week_day",
        "strict_weekyear_week_day"
    );

    /** Named formats that contain only hour/minute/second components (mirrors SQL plugin's list). */
    private static final Set<String> NAMED_TIME_FORMATS = setOf(
        "basic_time",
        "basic_time_no_millis",
        "basic_t_time",
        "basic_t_time_no_millis",
        "time",
        "strict_time",
        "time_no_millis",
        "strict_time_no_millis",
        "hour_minute_second_fraction",
        "strict_hour_minute_second_fraction",
        "hour_minute_second_millis",
        "strict_hour_minute_second_millis",
        "hour_minute_second",
        "strict_hour_minute_second",
        "hour_minute",
        "strict_hour_minute",
        "hour",
        "strict_hour",
        "t_time",
        "strict_t_time",
        "t_time_no_millis",
        "strict_t_time_no_millis"
    );

    /** Custom-pattern symbols indicating a time component (matches SQL plugin's CUSTOM_FORMAT_TIME_SYMBOLS). */
    private static final String CUSTOM_TIME_SYMBOLS = "nNASsmHkKha";

    /** Custom-pattern symbols indicating a date component (matches SQL plugin's CUSTOM_FORMAT_DATE_SYMBOLS). */
    private static final String CUSTOM_DATE_SYMBOLS = "FecEWwYqQgdMLDyuG";

    private DateFormatClassifier() {}

    /**
     * Returns the classification for a raw OpenSearch {@code format} string. Empty / null
     * yields {@link Kind#TIMESTAMP} (the OpenSearch default behavior).
     */
    public static Kind classify(String format) {
        if (format == null || format.isEmpty()) {
            return Kind.TIMESTAMP;
        }
        boolean sawDate = false;
        boolean sawTime = false;
        for (String component : split(format)) {
            String trimmed = component.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String stripped = strip8Prefix(trimmed);
            String lower = stripped.toLowerCase(Locale.ROOT);
            // A component is named if it appears in our named-format allow-lists; otherwise
            // treat it as a custom pattern and scan for date/time symbols. Numeric / unknown
            // named formats (e.g. "epoch_millis") don't match either set and behave as
            // timestamp via the symbol scan returning neither flag — see the timestamp
            // fall-through below.
            if (NAMED_DATE_FORMATS.contains(lower)) {
                sawDate = true;
            } else if (NAMED_TIME_FORMATS.contains(lower)) {
                sawTime = true;
            } else if (isKnownDatetimeOrNumericNamedFormat(lower)) {
                return Kind.TIMESTAMP;
            } else {
                boolean[] flags = scanCustomPattern(stripped);
                sawDate |= flags[0];
                sawTime |= flags[1];
            }
            if (sawDate && sawTime) {
                return Kind.TIMESTAMP;
            }
        }
        if (sawDate && !sawTime) {
            return Kind.DATE_ONLY;
        }
        if (sawTime && !sawDate) {
            return Kind.TIME_ONLY;
        }
        return Kind.TIMESTAMP;
    }

    /** Returns {@code true} for named formats that already include both date AND time. */
    private static boolean isKnownDatetimeOrNumericNamedFormat(String lower) {
        // Any *date_time* or *date_optional_time* / *epoch_* / *_year_month* etc — broad enough
        // to short-circuit obvious-timestamp cases. Custom patterns and unknown names fall
        // through to the symbol scan.
        return lower.contains("date_time")
            || lower.contains("date_optional_time")
            || lower.startsWith("epoch_")
            || lower.equals("year_month")
            || lower.equals("strict_year_month")
            || lower.equals("year")
            || lower.equals("strict_year")
            || lower.startsWith("date_hour")
            || lower.startsWith("strict_date_hour")
            || lower.startsWith("weekyear")
            || lower.startsWith("strict_weekyear")
            || lower.startsWith("week_year")
            || lower.startsWith("strict_week_year")
            || lower.contains("ordinal_date_time")
            || lower.contains("week_date_time");
    }

    /** Returns {[hasDateSymbol, hasTimeSymbol]} for a custom DateTimeFormatter pattern. */
    private static boolean[] scanCustomPattern(String pattern) {
        boolean date = false;
        boolean time = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (!date && CUSTOM_DATE_SYMBOLS.indexOf(c) >= 0) {
                date = true;
            }
            if (!time && CUSTOM_TIME_SYMBOLS.indexOf(c) >= 0) {
                time = true;
            }
            if (date && time) {
                break;
            }
        }
        return new boolean[] { date, time };
    }

    /** Splits an OpenSearch {@code format} string on its {@code ||} separator. */
    private static String[] split(String format) {
        return format.split("\\|\\|");
    }

    /** Strips a leading {@code 8} prefix from named formats (used by OpenSearch's joda compat). */
    private static String strip8Prefix(String s) {
        return s.startsWith("8") ? s.substring(1) : s;
    }

    private static Set<String> setOf(String... values) {
        Set<String> set = new HashSet<>();
        for (String v : values) {
            set.add(v.toLowerCase(Locale.ROOT));
        }
        return set;
    }
}
