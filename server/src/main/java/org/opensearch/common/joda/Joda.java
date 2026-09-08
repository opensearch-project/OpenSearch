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

package org.opensearch.common.joda;

import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.FormatNames;
import org.opensearch.common.util.LazyInitializable;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.ReadablePartial;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeParserBucket;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.StrictISODateTimeFormat;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Joda class.
 *
 * @deprecated
 *
 * @opensearch.internal
 */
@Deprecated
public class Joda {
    // Joda.forPattern could be used even before the logging is initialized.
    // If LogManager.getLogger is called before logging config is loaded
    // it results in errors sent to status logger and startup to fail.
    // Hence a lazy initialization.
    private static final LazyInitializable<DeprecationLogger, RuntimeException> deprecationLogger = new LazyInitializable(
        () -> DeprecationLogger.getLogger(Joda.class)
    );

    /**
     * Parses a joda based pattern, including some named ones (similar to the built in Joda ISO ones).
     */
    public static JodaDateFormatter forPattern(String input) {
        if (Strings.hasLength(input)) {
            input = input.trim();
        }
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        FormatNames formatName = FormatNames.forName(input);
        if (formatName != null && formatName.isCamelCase(input)) {
            String msg = "Camel case format name {} is deprecated and will be removed in a future version. "
                + "Use snake case name {} instead.";
            getDeprecationLogger().deprecate(
                "camelCaseDateFormat_" + formatName.getCamelCaseName(),
                msg,
                formatName.getCamelCaseName(),
                formatName.getSnakeCaseName()
            );
        }

        // Use the already-resolved FormatNames enum value with an O(1) switch lookup
        // instead of re-scanning a linear if-else chain that performed up to 120
        // String.equals() comparisons per call on the date-format hot path.
        if (formatName != null) {
            switch (formatName) {
                case BASIC_DATE:
                    return jodaFormatter(input, ISODateTimeFormat.basicDate());
                case BASIC_DATE_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.basicDateTime());
                case BASIC_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.basicDateTimeNoMillis());
                case BASIC_ORDINAL_DATE:
                    return jodaFormatter(input, ISODateTimeFormat.basicOrdinalDate());
                case BASIC_ORDINAL_DATE_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.basicOrdinalDateTime());
                case BASIC_ORDINAL_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.basicOrdinalDateTimeNoMillis());
                case BASIC_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.basicTime());
                case BASIC_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.basicTimeNoMillis());
                case BASIC_T_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.basicTTime());
                case BASIC_T_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.basicTTimeNoMillis());
                case BASIC_WEEK_DATE:
                    return jodaFormatter(input, ISODateTimeFormat.basicWeekDate());
                case BASIC_WEEK_DATE_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.basicWeekDateTime());
                case BASIC_WEEK_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.basicWeekDateTimeNoMillis());
                case DATE:
                    return jodaFormatter(input, ISODateTimeFormat.date());
                case DATE_HOUR:
                    return jodaFormatter(input, ISODateTimeFormat.dateHour());
                case DATE_HOUR_MINUTE:
                    return jodaFormatter(input, ISODateTimeFormat.dateHourMinute());
                case DATE_HOUR_MINUTE_SECOND:
                    return jodaFormatter(input, ISODateTimeFormat.dateHourMinuteSecond());
                case DATE_HOUR_MINUTE_SECOND_FRACTION:
                    return jodaFormatter(input, ISODateTimeFormat.dateHourMinuteSecondFraction());
                case DATE_HOUR_MINUTE_SECOND_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.dateHourMinuteSecondMillis());
                case DATE_OPTIONAL_TIME:
                    // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
                    // this sucks we should use the root local by default and not be dependent on the node
                    return new JodaDateFormatter(
                        input,
                        ISODateTimeFormat.dateOptionalTimeParser().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970),
                        ISODateTimeFormat.dateTime().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970)
                    );
                case DATE_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.dateTime());
                case DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.dateTimeNoMillis());
                case HOUR:
                    return jodaFormatter(input, ISODateTimeFormat.hour());
                case HOUR_MINUTE:
                    return jodaFormatter(input, ISODateTimeFormat.hourMinute());
                case HOUR_MINUTE_SECOND:
                    return jodaFormatter(input, ISODateTimeFormat.hourMinuteSecond());
                case HOUR_MINUTE_SECOND_FRACTION:
                    return jodaFormatter(input, ISODateTimeFormat.hourMinuteSecondFraction());
                case HOUR_MINUTE_SECOND_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.hourMinuteSecondMillis());
                case ORDINAL_DATE:
                    return jodaFormatter(input, ISODateTimeFormat.ordinalDate());
                case ORDINAL_DATE_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.ordinalDateTime());
                case ORDINAL_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.ordinalDateTimeNoMillis());
                case TIME:
                    return jodaFormatter(input, ISODateTimeFormat.time());
                case TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.timeNoMillis());
                case T_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.tTime());
                case T_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.tTimeNoMillis());
                case WEEK_DATE:
                    return jodaFormatter(input, ISODateTimeFormat.weekDate());
                case WEEK_DATE_TIME:
                    return jodaFormatter(input, ISODateTimeFormat.weekDateTime());
                case WEEK_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, ISODateTimeFormat.weekDateTimeNoMillis());
                case WEEKYEAR:
                    getDeprecationLogger().deprecate(
                        "week_year_format_name",
                        "Format name \"week_year\" is deprecated and will be removed in a future version. "
                            + "Use \"weekyear\" format instead"
                    );
                    return jodaFormatter(input, ISODateTimeFormat.weekyear());
                case WEEK_YEAR:
                    return jodaFormatter(input, ISODateTimeFormat.weekyear());
                case WEEK_YEAR_WEEK:
                    return jodaFormatter(input, ISODateTimeFormat.weekyearWeek());
                case WEEKYEAR_WEEK_DAY:
                    return jodaFormatter(input, ISODateTimeFormat.weekyearWeekDay());
                case YEAR:
                    return jodaFormatter(input, ISODateTimeFormat.year());
                case YEAR_MONTH:
                    return jodaFormatter(input, ISODateTimeFormat.yearMonth());
                case YEAR_MONTH_DAY:
                    return jodaFormatter(input, ISODateTimeFormat.yearMonthDay());
                case EPOCH_SECOND:
                    return jodaFormatter(
                        input,
                        new DateTimeFormatterBuilder().append(new EpochTimePrinter(false), new EpochTimeParser(false)).toFormatter()
                    );
                case EPOCH_MILLIS:
                    return jodaFormatter(
                        input,
                        new DateTimeFormatterBuilder().append(new EpochTimePrinter(true), new EpochTimeParser(true)).toFormatter()
                    );
                // strict date formats here, must be at least 4 digits for year and two for months and two for day
                case STRICT_BASIC_WEEK_DATE:
                    return jodaFormatter(input, StrictISODateTimeFormat.basicWeekDate());
                case STRICT_BASIC_WEEK_DATE_TIME:
                    return jodaFormatter(input, StrictISODateTimeFormat.basicWeekDateTime());
                case STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, StrictISODateTimeFormat.basicWeekDateTimeNoMillis());
                case STRICT_DATE:
                    return jodaFormatter(input, StrictISODateTimeFormat.date());
                case STRICT_DATE_HOUR:
                    return jodaFormatter(input, StrictISODateTimeFormat.dateHour());
                case STRICT_DATE_HOUR_MINUTE:
                    return jodaFormatter(input, StrictISODateTimeFormat.dateHourMinute());
                case STRICT_DATE_HOUR_MINUTE_SECOND:
                    return jodaFormatter(input, StrictISODateTimeFormat.dateHourMinuteSecond());
                case STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION:
                    return jodaFormatter(input, StrictISODateTimeFormat.dateHourMinuteSecondFraction());
                case STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS:
                    return jodaFormatter(input, StrictISODateTimeFormat.dateHourMinuteSecondMillis());
                case STRICT_DATE_OPTIONAL_TIME:
                    // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
                    // this sucks we should use the root local by default and not be dependent on the node
                    return new JodaDateFormatter(
                        input,
                        StrictISODateTimeFormat.dateOptionalTimeParser().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970),
                        StrictISODateTimeFormat.dateTime().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970)
                    );
                case STRICT_DATE_TIME:
                    return jodaFormatter(input, StrictISODateTimeFormat.dateTime());
                case STRICT_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, StrictISODateTimeFormat.dateTimeNoMillis());
                case STRICT_HOUR:
                    return jodaFormatter(input, StrictISODateTimeFormat.hour());
                case STRICT_HOUR_MINUTE:
                    return jodaFormatter(input, StrictISODateTimeFormat.hourMinute());
                case STRICT_HOUR_MINUTE_SECOND:
                    return jodaFormatter(input, StrictISODateTimeFormat.hourMinuteSecond());
                case STRICT_HOUR_MINUTE_SECOND_FRACTION:
                    return jodaFormatter(input, StrictISODateTimeFormat.hourMinuteSecondFraction());
                case STRICT_HOUR_MINUTE_SECOND_MILLIS:
                    return jodaFormatter(input, StrictISODateTimeFormat.hourMinuteSecondMillis());
                case STRICT_ORDINAL_DATE:
                    return jodaFormatter(input, StrictISODateTimeFormat.ordinalDate());
                case STRICT_ORDINAL_DATE_TIME:
                    return jodaFormatter(input, StrictISODateTimeFormat.ordinalDateTime());
                case STRICT_ORDINAL_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, StrictISODateTimeFormat.ordinalDateTimeNoMillis());
                case STRICT_TIME:
                    return jodaFormatter(input, StrictISODateTimeFormat.time());
                case STRICT_TIME_NO_MILLIS:
                    return jodaFormatter(input, StrictISODateTimeFormat.timeNoMillis());
                case STRICT_T_TIME:
                    return jodaFormatter(input, StrictISODateTimeFormat.tTime());
                case STRICT_T_TIME_NO_MILLIS:
                    return jodaFormatter(input, StrictISODateTimeFormat.tTimeNoMillis());
                case STRICT_WEEK_DATE:
                    return jodaFormatter(input, StrictISODateTimeFormat.weekDate());
                case STRICT_WEEK_DATE_TIME:
                    return jodaFormatter(input, StrictISODateTimeFormat.weekDateTime());
                case STRICT_WEEK_DATE_TIME_NO_MILLIS:
                    return jodaFormatter(input, StrictISODateTimeFormat.weekDateTimeNoMillis());
                case STRICT_WEEKYEAR:
                    return jodaFormatter(input, StrictISODateTimeFormat.weekyear());
                case STRICT_WEEKYEAR_WEEK:
                    return jodaFormatter(input, StrictISODateTimeFormat.weekyearWeek());
                case STRICT_WEEKYEAR_WEEK_DAY:
                    return jodaFormatter(input, StrictISODateTimeFormat.weekyearWeekDay());
                case STRICT_YEAR:
                    return jodaFormatter(input, StrictISODateTimeFormat.year());
                case STRICT_YEAR_MONTH:
                    return jodaFormatter(input, StrictISODateTimeFormat.yearMonth());
                case STRICT_YEAR_MONTH_DAY:
                    return jodaFormatter(input, StrictISODateTimeFormat.yearMonthDay());
                default:
                    break;
            }
        }

        DateTimeFormatter formatter;
        if (Strings.hasLength(input) && input.contains("||")) {
            String[] formats = Strings.delimitedListToStringArray(input, "||");
            DateTimeParser[] parsers = new DateTimeParser[formats.length];

            if (formats.length == 1) {
                formatter = forPattern(input).parser;
            } else {
                DateTimeFormatter dateTimeFormatter = null;
                for (int i = 0; i < formats.length; i++) {
                    JodaDateFormatter currentFormatter = forPattern(formats[i]);
                    DateTimeFormatter currentParser = currentFormatter.parser;
                    if (dateTimeFormatter == null) {
                        dateTimeFormatter = currentFormatter.printer;
                    }
                    parsers[i] = currentParser.getParser();
                }

                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(
                    dateTimeFormatter.withZone(DateTimeZone.UTC).getPrinter(),
                    parsers
                );
                formatter = builder.toFormatter();
            }
        } else {
            try {
                maybeLogJodaDeprecation(input);
                formatter = DateTimeFormat.forPattern(input);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
            }
        }

        formatter = formatter.withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970);
        return new JodaDateFormatter(input, formatter, formatter);
    }

    private static JodaDateFormatter jodaFormatter(String input, DateTimeFormatter formatter) {
        formatter = formatter.withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970);
        return new JodaDateFormatter(input, formatter, formatter);
    }
        return new JodaDateFormatter(input, formatter, formatter);
    }

    public static void writeTimeZone(final StreamOutput out, final DateTimeZone timeZone) throws IOException {
        out.writeString(timeZone.getID());
    }

    public static void writeOptionalTimeZone(final StreamOutput out, final DateTimeZone timeZone) throws IOException {
        if (timeZone == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeTimeZone(out, timeZone);
        }
    }

    /**
     * Read a {@linkplain DateTimeZone} from a {@linkplain StreamInput}.
     */
    public static DateTimeZone readTimeZone(final StreamInput in) throws IOException {
        return DateTimeZone.forID(in.readString());
    }

    /**
     * Read an optional {@linkplain DateTimeZone}.
     */
    public static DateTimeZone readOptionalTimeZone(final StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return DateTimeZone.forID(in.readString());
        }
        return null;
    }

    private static void maybeLogJodaDeprecation(String format) {
        if (JodaDeprecationPatterns.isDeprecatedPattern(format)) {
            String suggestion = JodaDeprecationPatterns.formatSuggestion(format);
            getDeprecationLogger().deprecate(
                "joda-pattern-deprecation",
                suggestion + " " + JodaDeprecationPatterns.USE_NEW_FORMAT_SPECIFIERS
            );
        }
    }

    public static DateFormatter getStrictStandardDateFormatter() {
        // 2014/10/10
        DateTimeFormatter shortFormatter = new DateTimeFormatterBuilder().appendFixedDecimal(DateTimeFieldType.year(), 4)
            .appendLiteral('/')
            .appendFixedDecimal(DateTimeFieldType.monthOfYear(), 2)
            .appendLiteral('/')
            .appendFixedDecimal(DateTimeFieldType.dayOfMonth(), 2)
            .toFormatter()
            .withZoneUTC();

        // 2014/10/10 12:12:12
        DateTimeFormatter longFormatter = new DateTimeFormatterBuilder().appendFixedDecimal(DateTimeFieldType.year(), 4)
            .appendLiteral('/')
            .appendFixedDecimal(DateTimeFieldType.monthOfYear(), 2)
            .appendLiteral('/')
            .appendFixedDecimal(DateTimeFieldType.dayOfMonth(), 2)
            .appendLiteral(' ')
            .appendFixedSignedDecimal(DateTimeFieldType.hourOfDay(), 2)
            .appendLiteral(':')
            .appendFixedSignedDecimal(DateTimeFieldType.minuteOfHour(), 2)
            .appendLiteral(':')
            .appendFixedSignedDecimal(DateTimeFieldType.secondOfMinute(), 2)
            .toFormatter()
            .withZoneUTC();

        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(
            longFormatter.withZone(DateTimeZone.UTC).getPrinter(),
            new DateTimeParser[] { longFormatter.getParser(), shortFormatter.getParser(), new EpochTimeParser(true) }
        );

        DateTimeFormatter formatter = builder.toFormatter().withLocale(Locale.ROOT).withZone(DateTimeZone.UTC).withDefaultYear(1970);
        return new JodaDateFormatter("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis", formatter, formatter);
    }

    public static final DurationFieldType Quarters = new DurationFieldType("quarters") {
        @Override
        public DurationField getField(Chronology chronology) {
            return new ScaledDurationField(chronology.months(), Quarters, 3);
        }
    };

    public static final DateTimeFieldType QuarterOfYear = new DateTimeFieldType("quarterOfYear") {
        @Override
        public DurationFieldType getDurationType() {
            return Quarters;
        }

        @Override
        public DurationFieldType getRangeDurationType() {
            return DurationFieldType.years();
        }

        @Override
        public DateTimeField getField(Chronology chronology) {
            return new OffsetDateTimeField(
                new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QuarterOfYear, 3),
                1
            );
        }
    };

    /**
     * parses epcoch timers
     *
     * @opensearch.internal
     */
    public static class EpochTimeParser implements DateTimeParser {

        private static final Pattern scientificNotation = Pattern.compile("[Ee]");

        private final boolean hasMilliSecondPrecision;

        public EpochTimeParser(boolean hasMilliSecondPrecision) {
            this.hasMilliSecondPrecision = hasMilliSecondPrecision;
        }

        @Override
        public int estimateParsedLength() {
            return hasMilliSecondPrecision ? 19 : 16;
        }

        @Override
        public int parseInto(DateTimeParserBucket bucket, String text, int position) {
            boolean isPositive = text.startsWith("-") == false;
            int firstDotIndex = text.indexOf('.');
            boolean isTooLong = (firstDotIndex == -1 ? text.length() : firstDotIndex) > estimateParsedLength();

            if (bucket.getZone() != DateTimeZone.UTC) {
                String format = hasMilliSecondPrecision ? "epoch_millis" : "epoch_second";
                throw new IllegalArgumentException("time_zone must be UTC for format [" + format + "]");
            } else if (isPositive && isTooLong) {
                return -1;
            }

            int factor = hasMilliSecondPrecision ? 1 : 1000;
            try {
                long millis = new BigDecimal(text).longValue() * factor;
                // check for deprecations, but after it has parsed correctly so invalid values aren't counted as deprecated
                if (millis < 0) {
                    getDeprecationLogger().deprecate(
                        "epoch-negative",
                        "Use of negative values"
                            + " in epoch time formats is deprecated and will not be supported in the next major version of OpenSearch."
                    );
                }
                if (scientificNotation.matcher(text).find()) {
                    getDeprecationLogger().deprecate(
                        "epoch-scientific-notation",
                        "Use of scientific notation"
                            + " in epoch time formats is deprecated and will not be supported in the next major version of OpenSearch."
                    );
                }
                DateTime dt = new DateTime(millis, DateTimeZone.UTC);
                bucket.saveField(DateTimeFieldType.year(), dt.getYear());
                bucket.saveField(DateTimeFieldType.monthOfYear(), dt.getMonthOfYear());
                bucket.saveField(DateTimeFieldType.dayOfMonth(), dt.getDayOfMonth());
                bucket.saveField(DateTimeFieldType.hourOfDay(), dt.getHourOfDay());
                bucket.saveField(DateTimeFieldType.minuteOfHour(), dt.getMinuteOfHour());
                bucket.saveField(DateTimeFieldType.secondOfMinute(), dt.getSecondOfMinute());
                bucket.saveField(DateTimeFieldType.millisOfSecond(), dt.getMillisOfSecond());
                bucket.setZone(DateTimeZone.UTC);
            } catch (Exception e) {
                return -1;
            }
            return text.length();
        }
    }

    private static DeprecationLogger getDeprecationLogger() {
        return deprecationLogger.getOrCompute();
    }

    /**
     * Epoch timer printer
     *
     * @opensearch.internal
     */
    public static class EpochTimePrinter implements DateTimePrinter {

        private boolean hasMilliSecondPrecision;

        public EpochTimePrinter(boolean hasMilliSecondPrecision) {
            this.hasMilliSecondPrecision = hasMilliSecondPrecision;
        }

        @Override
        public int estimatePrintedLength() {
            return hasMilliSecondPrecision ? 19 : 16;
        }

        /**
         * We adjust the instant by displayOffset to adjust for the offset that might have been added in
         * {@link DateTimeFormatter#printTo(Appendable, long, Chronology)} when using a time zone.
         */
        @Override
        public void printTo(StringBuffer buf, long instant, Chronology chrono, int displayOffset, DateTimeZone displayZone, Locale locale) {
            if (hasMilliSecondPrecision) {
                buf.append(instant - displayOffset);
            } else {
                buf.append((instant - displayOffset) / 1000);
            }
        }

        /**
         * We adjust the instant by displayOffset to adjust for the offset that might have been added in
         * {@link DateTimeFormatter#printTo(Appendable, long, Chronology)} when using a time zone.
         */
        @Override
        public void printTo(Writer out, long instant, Chronology chrono, int displayOffset, DateTimeZone displayZone, Locale locale)
            throws IOException {
            if (hasMilliSecondPrecision) {
                out.write(String.valueOf(instant - displayOffset));
            } else {
                out.append(String.valueOf((instant - displayOffset) / 1000));
            }
        }

        @Override
        public void printTo(StringBuffer buf, ReadablePartial partial, Locale locale) {
            if (hasMilliSecondPrecision) {
                buf.append(String.valueOf(getDateTimeMillis(partial)));
            } else {
                buf.append(String.valueOf(getDateTimeMillis(partial) / 1000));
            }
        }

        @Override
        public void printTo(Writer out, ReadablePartial partial, Locale locale) throws IOException {
            if (hasMilliSecondPrecision) {
                out.append(String.valueOf(getDateTimeMillis(partial)));
            } else {
                out.append(String.valueOf(getDateTimeMillis(partial) / 1000));
            }
        }

        private long getDateTimeMillis(ReadablePartial partial) {
            int year = partial.get(DateTimeFieldType.year());
            int monthOfYear = partial.get(DateTimeFieldType.monthOfYear());
            int dayOfMonth = partial.get(DateTimeFieldType.dayOfMonth());
            int hourOfDay = partial.get(DateTimeFieldType.hourOfDay());
            int minuteOfHour = partial.get(DateTimeFieldType.minuteOfHour());
            int secondOfMinute = partial.get(DateTimeFieldType.secondOfMinute());
            int millisOfSecond = partial.get(DateTimeFieldType.millisOfSecond());
            return partial.getChronology()
                .getDateTimeMillis(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisOfSecond);
        }
    }
}
