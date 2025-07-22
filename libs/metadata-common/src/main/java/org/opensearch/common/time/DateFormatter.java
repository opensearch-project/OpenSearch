/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.time;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.joda.time.DateTime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Base Date formatter
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface DateFormatter {

    /**
     * Try to parse input to a java time TemporalAccessor
     * @param input                   An arbitrary string resembling the string representation of a date or time
     * @throws DateTimeParseException If parsing fails, this exception will be thrown.
     *                                Note that it can contained suppressed exceptions when several formatters failed parse this value
     * @return                        The java time object containing the parsed input
     */
    TemporalAccessor parse(String input);

    /**
     * Parse the given input into millis-since-epoch.
     */
    default long parseMillis(String input) {
        return DateFormatters.from(parse(input)).toInstant().toEpochMilli();
    }

    /**
     * Parse the given input into a Joda {@link DateTime}.
     */
    default DateTime parseJoda(String input) {
        ZonedDateTime dateTime = ZonedDateTime.from(parse(input));
        return new DateTime(dateTime.toInstant().toEpochMilli(), DateUtils.zoneIdToDateTimeZone(dateTime.getZone()));
    }

    /**
     * Create a copy of this formatter that is configured to parse dates in the specified time zone
     *
     * @param zoneId The time zone to act on
     * @return       A copy of the date formatter this has been called on
     */
    DateFormatter withZone(ZoneId zoneId);

    /**
     * Create a copy of this formatter that is configured to parse dates in the specified locale
     *
     * @param locale The local to use for the new formatter
     * @return       A copy of the date formatter this has been called on
     */
    DateFormatter withLocale(Locale locale);

    /**
     * Print the supplied java time accessor in a string based representation according to this formatter
     *
     * @param accessor The temporal accessor used to format
     * @return         The string result for the formatting
     */
    String format(TemporalAccessor accessor);

    /**
     * Return the given millis-since-epoch formatted with this format.
     */
    default String formatMillis(long millis) {
        ZoneId zone = zone() != null ? zone() : ZoneOffset.UTC;
        return format(Instant.ofEpochMilli(millis).atZone(zone));
    }

    /**
     * Return the given Joda {@link DateTime} formatted with this format.
     */
    default String formatJoda(DateTime dateTime) {
        return format(
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateTime.getMillis()), DateUtils.dateTimeZoneToZoneId(dateTime.getZone()))
        );
    }

    /**
     * A name based format for this formatter. Can be one of the registered formatters like <code>epoch_millis</code> or
     * a configured format like <code>HH:mm:ss</code>
     *
     * @return The name of this formatter
     */
    String pattern();

    /**
     * A name based format for this formatter. Can be one of the registered formatters like <code>epoch_millis</code> or
     * a configured format like <code>HH:mm:ss</code>
     *
     * @return The name of this formatter
     */
    String printPattern();

    /**
     * Returns the configured locale of the date formatter
     *
     * @return The locale of this formatter
     */
    Locale locale();

    /**
     * Returns the configured time zone of the date formatter
     *
     * @return The time zone of this formatter
     */
    ZoneId zone();

    /**
     * Create a DateMathParser from the existing formatter
     *
     * @return The DateMathParser object
     */
    DateMathParser toDateMathParser();

    static DateFormatter forPattern(String input, String printPattern, Boolean canCacheFormatter) {

        if (Strings.hasLength(input) == false) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        // support the 6.x BWC compatible way of parsing java 8 dates
        String format = strip8Prefix(input);
        List<String> patterns = splitCombinedPatterns(format);
        List<DateFormatter> formatters = patterns.stream().map(DateFormatters::forPattern).collect(Collectors.toList());

        DateFormatter printFormatter = formatters.get(0);
        if (Strings.hasLength(printPattern)) {
            String printFormat = strip8Prefix(printPattern);
            try {
                printFormatter = DateFormatters.forPattern(printFormat);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid print format: " + e.getMessage(), e);
            }
        }
        return JavaDateFormatter.combined(input, formatters, printFormatter, canCacheFormatter);
    }

    static DateFormatter forPattern(String input) {
        return forPattern(input, null, false);
    }

    static DateFormatter forPattern(String input, String printPattern) {
        return forPattern(input, printPattern, false);
    }

    static DateFormatter forPattern(String input, Boolean canCacheFormatter) {
        return forPattern(input, null, canCacheFormatter);
    }

    static String strip8Prefix(String input) {
        if (input.startsWith("8")) {
            return input.substring(1);
        }
        return input;
    }

    static List<String> splitCombinedPatterns(String input) {
        List<String> patterns = new ArrayList<>();
        for (String pattern : Strings.delimitedListToStringArray(input, "||")) {
            if (Strings.hasLength(pattern) == false) {
                throw new IllegalArgumentException("Cannot have empty element in multi date format pattern: " + input);
            }
            patterns.add(pattern);
        }
        return patterns;
    }
}
