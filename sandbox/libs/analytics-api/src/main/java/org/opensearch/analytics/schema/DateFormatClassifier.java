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
 * Classifies an OpenSearch {@code date} field's {@code format} as date-only, time-only,
 * or full timestamp. Mirrors the SQL plugin's {@code OpenSearchDateType} rules
 * (named-format allow-lists + custom-pattern symbol scan); sandbox sits below sql-core in
 * the dependency graph so the lists are duplicated here. Format syntax is the OpenSearch
 * {@code DateFormatter} grammar — components separated by {@code ||}.
 */
public final class DateFormatClassifier {

    /** Classification result for a {@code date} mapping's {@code format}. */
    public enum Kind {
        DATE_ONLY,
        TIME_ONLY,
        TIMESTAMP
    }

    // TODO: replace with constants from OpenSearch core's DateFormatters registry once they're
    // exposed for external use; today we mirror the well-known names locally to avoid coupling
    // to internal APIs.
    /** Named formats with year/month/day only. */
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

    /** Named formats with hour/minute/second only. */
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

    /** Named formats forcing TIMESTAMP: full datetime + numeric epochs + incomplete date (year, year_month, …). */
    private static final Set<String> NAMED_TIMESTAMP_FORMATS = setOf(
        // datetime
        "iso8601",
        "basic_date_time",
        "basic_date_time_no_millis",
        "basic_ordinal_date_time",
        "basic_ordinal_date_time_no_millis",
        "basic_week_date_time",
        "strict_basic_week_date_time",
        "basic_week_date_time_no_millis",
        "strict_basic_week_date_time_no_millis",
        "basic_week_date",
        "strict_basic_week_date",
        "date_optional_time",
        "strict_date_optional_time",
        "strict_date_optional_time_nanos",
        "date_time",
        "strict_date_time",
        "date_time_no_millis",
        "strict_date_time_no_millis",
        "date_hour_minute_second_fraction",
        "strict_date_hour_minute_second_fraction",
        "date_hour_minute_second_millis",
        "strict_date_hour_minute_second_millis",
        "date_hour_minute_second",
        "strict_date_hour_minute_second",
        "date_hour_minute",
        "strict_date_hour_minute",
        "date_hour",
        "strict_date_hour",
        "ordinal_date_time",
        "strict_ordinal_date_time",
        "ordinal_date_time_no_millis",
        "strict_ordinal_date_time_no_millis",
        "week_date_time",
        "strict_week_date_time",
        "week_date_time_no_millis",
        "strict_week_date_time_no_millis",
        // numeric epochs
        "epoch_millis",
        "epoch_second",
        "epoch_micros",
        // incomplete date — can't be downgraded to DATE
        "year_month",
        "strict_year_month",
        "year",
        "strict_year",
        "week_year",
        "week_year_week",
        "strict_weekyear_week",
        "weekyear",
        "strict_weekyear"
    );

    /** Custom-pattern symbols for time components (matches SQL plugin's CUSTOM_FORMAT_TIME_SYMBOLS). */
    private static final String CUSTOM_TIME_SYMBOLS = "nNASsmHkKha";

    /** Custom-pattern symbols for date components (matches SQL plugin's CUSTOM_FORMAT_DATE_SYMBOLS). */
    private static final String CUSTOM_DATE_SYMBOLS = "FecEWwYqQgdMLDyuG";

    private DateFormatClassifier() {}

    /** Null / empty maps to {@link Kind#TIMESTAMP} (OpenSearch default). */
    public static Kind classify(String format) {
        if (format == null || format.isEmpty()) {
            return Kind.TIMESTAMP;
        }
        boolean sawDate = false;
        boolean sawTime = false;
        for (String component : format.split("\\|\\|")) {
            String trimmed = component.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String stripped = strip8Prefix(trimmed);
            String lower = stripped.toLowerCase(Locale.ROOT);
            if (NAMED_TIMESTAMP_FORMATS.contains(lower)) {
                return Kind.TIMESTAMP;
            }
            if (NAMED_DATE_FORMATS.contains(lower)) {
                sawDate = true;
            } else if (NAMED_TIME_FORMATS.contains(lower)) {
                sawTime = true;
            } else {
                // unknown name → treat as custom pattern; scan for symbols
                boolean[] flags = scanCustomPattern(stripped);
                sawDate |= flags[0];
                sawTime |= flags[1];
            }
            if (sawDate && sawTime) {
                return Kind.TIMESTAMP;
            }
        }
        if (sawDate && !sawTime) return Kind.DATE_ONLY;
        if (sawTime && !sawDate) return Kind.TIME_ONLY;
        return Kind.TIMESTAMP;
    }

    /** Returns {[hasDateSymbol, hasTimeSymbol]} for a custom DateTimeFormatter pattern. */
    private static boolean[] scanCustomPattern(String pattern) {
        boolean date = false;
        boolean time = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (!date && CUSTOM_DATE_SYMBOLS.indexOf(c) >= 0) date = true;
            if (!time && CUSTOM_TIME_SYMBOLS.indexOf(c) >= 0) time = true;
            if (date && time) break;
        }
        return new boolean[] { date, time };
    }

    /** Strips leading {@code 8} prefix used by OpenSearch's joda compatibility shim. */
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
