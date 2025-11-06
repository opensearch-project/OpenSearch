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

import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.Strings;

import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

class JavaDateFormatter implements DateFormatter {

    // base fields which should be used for default parsing, when we round up for date math
    private static final Map<TemporalField, Long> ROUND_UP_GENERIC_BASE_FIELDS = new HashMap<>(4);

    {
        ROUND_UP_GENERIC_BASE_FIELDS.put(ChronoField.HOUR_OF_DAY, 23L);
        ROUND_UP_GENERIC_BASE_FIELDS.put(ChronoField.MINUTE_OF_HOUR, 59L);
        ROUND_UP_GENERIC_BASE_FIELDS.put(ChronoField.SECOND_OF_MINUTE, 59L);
        ROUND_UP_GENERIC_BASE_FIELDS.put(ChronoField.NANO_OF_SECOND, 999_999_999L);
    }

    private final String format;
    private final String printFormat;
    private final OpenSearchDateTimePrinter printer;
    private final List<OpenSearchDateTimeFormatter> parsers;
    private final JavaDateFormatter roundupParser;
    private final Boolean canCacheLastParsedFormatter;
    private volatile OpenSearchDateTimeFormatter lastParsedformatter = null;

    /**
     * A round up formatter
     *
     * @opensearch.internal
     */
    static class RoundUpFormatter extends JavaDateFormatter {

        RoundUpFormatter(String format, List<OpenSearchDateTimeFormatter> roundUpParsers) {
            super(format, firstFrom(roundUpParsers), null, roundUpParsers);
        }

        private static OpenSearchDateTimeFormatter firstFrom(List<OpenSearchDateTimeFormatter> roundUpParsers) {
            return roundUpParsers.get(0);
        }

        @Override
        JavaDateFormatter getRoundupParser() {
            throw new UnsupportedOperationException("RoundUpFormatter does not have another roundUpFormatter");
        }
    }

    // named formatters use default roundUpParser
    JavaDateFormatter(
        String format,
        String printFormat,
        OpenSearchDateTimePrinter printer,
        Boolean canCacheLastParsedFormatter,
        OpenSearchDateTimeFormatter... parsers
    ) {
        this(format, printFormat, printer, ROUND_UP_BASE_FIELDS, canCacheLastParsedFormatter, parsers);
    }

    JavaDateFormatter(String format, DateTimeFormatter printer, DateTimeFormatter... parsers) {
        this(format, format, wrapFormatter(printer), false, wrapAllFormatters(parsers));
    }

    JavaDateFormatter(String format, OpenSearchDateTimePrinter printer, OpenSearchDateTimeFormatter... parsers) {
        this(format, format, printer, false, parsers);
    }

    private static final BiConsumer<DateTimeFormatterBuilder, DateTimeFormatter> ROUND_UP_BASE_FIELDS = (builder, parser) -> {
        String parserString = parser.toString();
        if (parserString.contains(ChronoField.DAY_OF_YEAR.toString())) {
            builder.parseDefaulting(ChronoField.DAY_OF_YEAR, 1L);
        } else {
            builder.parseDefaulting(ChronoField.MONTH_OF_YEAR, 1L);
            builder.parseDefaulting(ChronoField.DAY_OF_MONTH, 1L);
        }
        ROUND_UP_GENERIC_BASE_FIELDS.forEach(builder::parseDefaulting);
    };

    // subclasses override roundUpParser
    JavaDateFormatter(
        String format,
        String printFormat,
        OpenSearchDateTimePrinter printer,
        BiConsumer<DateTimeFormatterBuilder, DateTimeFormatter> roundupParserConsumer,
        Boolean canCacheLastParsedFormatter,
        OpenSearchDateTimeFormatter... parsers
    ) {
        if (printer == null) {
            throw new IllegalArgumentException("printer may not be null");
        }
        long distinctZones = Arrays.stream(parsers).map(OpenSearchDateTimeFormatter::getZone).distinct().count();
        if (distinctZones > 1) {
            throw new IllegalArgumentException("formatters must have the same time zone");
        }
        long distinctLocales = Arrays.stream(parsers).map(OpenSearchDateTimeFormatter::getLocale).distinct().count();
        if (distinctLocales > 1) {
            throw new IllegalArgumentException("formatters must have the same locale");
        }
        this.printer = printer;
        this.format = format;
        this.printFormat = printFormat;
        this.canCacheLastParsedFormatter = canCacheLastParsedFormatter;

        if (parsers.length == 0) {
            this.parsers = Collections.singletonList((OpenSearchDateTimeFormatter) printer);
        } else {
            this.parsers = Arrays.asList(parsers);
        }
        List<DateTimeFormatter> roundUp = createRoundUpParser(format, roundupParserConsumer);
        this.roundupParser = new RoundUpFormatter(format, wrapAllFormatters(roundUp));
    }

    JavaDateFormatter(
        String format,
        DateTimeFormatter printer,
        BiConsumer<DateTimeFormatterBuilder, DateTimeFormatter> roundupParserConsumer,
        DateTimeFormatter... parsers
    ) {
        this(format, format, wrapFormatter(printer), roundupParserConsumer, false, wrapAllFormatters(parsers));
    }

    /**
     * This is when the RoundUp Formatters are created. In further merges (with ||) it will only append them to a list.
     * || is not expected to be provided as format when a RoundUp formatter is created. It will be splitted before in
     * <code>DateFormatter.forPattern</code>
     * JavaDateFormatter created with a custom format like <code>DateFormatter.forPattern("YYYY")</code> will only have one parser
     * It is however possible to have a JavaDateFormatter with multiple parsers. For instance see a "date_time" formatter in
     * <code>DateFormatters</code>.
     * This means that we need to also have multiple RoundUp parsers.
     */
    private List<DateTimeFormatter> createRoundUpParser(
        String format,
        BiConsumer<DateTimeFormatterBuilder, DateTimeFormatter> roundupParserConsumer
    ) {
        if (format.contains("||") == false) {
            List<DateTimeFormatter> roundUpParsers = new ArrayList<>();
            for (OpenSearchDateTimeFormatter customparser : this.parsers) {
                DateTimeFormatter parser = customparser.getFormatter();
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
                builder.append(parser);
                roundupParserConsumer.accept(builder, parser);
                roundUpParsers.add(builder.toFormatter(locale()));
            }
            return roundUpParsers;
        }
        return null;
    }

    public static DateFormatter combined(
        String input,
        List<DateFormatter> formatters,
        DateFormatter printFormatter,
        Boolean canCacheLastParsedFormatter
    ) {
        assert formatters.size() > 0;
        assert printFormatter != null;

        List<OpenSearchDateTimeFormatter> parsers = new ArrayList<>(formatters.size());
        List<OpenSearchDateTimeFormatter> roundUpParsers = new ArrayList<>(formatters.size());

        assert printFormatter instanceof JavaDateFormatter;
        JavaDateFormatter javaPrintFormatter = (JavaDateFormatter) printFormatter;
        OpenSearchDateTimePrinter printer = javaPrintFormatter.getPrinter();
        for (DateFormatter formatter : formatters) {
            assert formatter instanceof JavaDateFormatter;
            JavaDateFormatter javaDateFormatter = (JavaDateFormatter) formatter;
            parsers.addAll(javaDateFormatter.getParsers());
            roundUpParsers.addAll(javaDateFormatter.getRoundupParser().getParsers());
        }

        return new JavaDateFormatter(
            input,
            javaPrintFormatter.format,
            printer,
            roundUpParsers,
            parsers,
            canCacheLastParsedFormatter & FeatureFlags.isEnabled(FeatureFlags.DATETIME_FORMATTER_CACHING_SETTING)
        ); // check if caching is enabled
    }

    private JavaDateFormatter(
        String format,
        String printFormat,
        OpenSearchDateTimePrinter printer,
        List<OpenSearchDateTimeFormatter> roundUpParsers,
        List<OpenSearchDateTimeFormatter> parsers,
        Boolean canCacheLastParsedFormatter
    ) {
        this.format = format;
        this.printFormat = printFormat;
        this.printer = printer;
        this.roundupParser = roundUpParsers != null ? new RoundUpFormatter(format, roundUpParsers) : null;
        this.parsers = parsers;
        this.canCacheLastParsedFormatter = canCacheLastParsedFormatter;
    }

    private JavaDateFormatter(
        String format,
        OpenSearchDateTimePrinter printer,
        List<OpenSearchDateTimeFormatter> roundUpParsers,
        List<OpenSearchDateTimeFormatter> parsers
    ) {
        this(format, format, printer, roundUpParsers, parsers, false);
    }

    JavaDateFormatter getRoundupParser() {
        return roundupParser;
    }

    OpenSearchDateTimePrinter getPrinter() {
        return printer;
    }

    @Override
    public TemporalAccessor parse(String input) {
        if (Strings.isNullOrEmpty(input)) {
            throw new IllegalArgumentException("cannot parse empty date");
        }

        try {
            return doParse(input);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("failed to parse date field [" + input + "] with format [" + format + "]", e);
        }
    }

    /**
     * Attempt parsing the input without throwing exception. If multiple parsers are provided,
     * it will continue iterating if the previous parser failed. The pattern must fully match, meaning whole input was used.
     * This also means that this method depends on <code>DateTimeFormatter.ClassicFormat.parseObject</code>
     * which does not throw exceptions when parsing failed.
     * <p>
     * The approach with collection of parsers was taken because java-time requires ordering on optional (composite)
     * patterns. Joda does not suffer from this.
     * https://bugs.openjdk.java.net/browse/JDK-8188771
     *
     * @param input An arbitrary string resembling the string representation of a date or time
     * @return a TemporalAccessor if parsing was successful.
     * @throws DateTimeParseException when unable to parse with any parsers
     */
    private TemporalAccessor doParse(String input) {
        if (parsers.size() > 1) {
            Object object = null;
            if (canCacheLastParsedFormatter && lastParsedformatter != null) {
                ParsePosition pos = new ParsePosition(0);
                object = lastParsedformatter.parseObject(input, pos);
                if (parsingSucceeded(object, input, pos)) {
                    return (TemporalAccessor) object;
                }
            }
            for (OpenSearchDateTimeFormatter formatter : parsers) {
                ParsePosition pos = new ParsePosition(0);
                object = formatter.parseObject(input, pos);
                if (parsingSucceeded(object, input, pos)) {
                    lastParsedformatter = formatter;
                    return (TemporalAccessor) object;
                }
            }

            throw new DateTimeParseException("Failed to parse with all enclosed parsers", input, 0);
        }
        return this.parsers.get(0).parse(input);
    }

    private boolean parsingSucceeded(Object object, String input, ParsePosition pos) {
        return object != null && pos.getIndex() == input.length();
    }

    private static OpenSearchDateTimeFormatter wrapFormatter(DateTimeFormatter formatter) {
        return new OpenSearchDateTimeFormatter(formatter);
    }

    private static OpenSearchDateTimeFormatter[] wrapAllFormatters(DateTimeFormatter... formatters) {
        return Arrays.stream(formatters).map(JavaDateFormatter::wrapFormatter).toArray(OpenSearchDateTimeFormatter[]::new);
    }

    private static List<OpenSearchDateTimeFormatter> wrapAllFormatters(List<DateTimeFormatter> formatters) {
        return formatters.stream().map(JavaDateFormatter::wrapFormatter).collect(Collectors.toList());
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        // shortcurt to not create new objects unnecessarily
        if (zoneId.equals(zone())) {
            return this;
        }
        List<OpenSearchDateTimeFormatter> parsers = new ArrayList<>(
            this.parsers.stream().map(p -> p.withZone(zoneId)).collect(Collectors.toList())
        );
        List<OpenSearchDateTimeFormatter> roundUpParsers = this.roundupParser.getParsers()
            .stream()
            .map(p -> p.withZone(zoneId))
            .collect(Collectors.toList());
        return new JavaDateFormatter(format, printFormat, printer.withZone(zoneId), roundUpParsers, parsers, canCacheLastParsedFormatter);
    }

    @Override
    public DateFormatter withLocale(Locale locale) {
        // shortcurt to not create new objects unnecessarily
        if (locale.equals(locale())) {
            return this;
        }
        List<OpenSearchDateTimeFormatter> parsers = new ArrayList<>(
            this.parsers.stream().map(p -> p.withLocale(locale)).collect(Collectors.toList())
        );
        List<OpenSearchDateTimeFormatter> roundUpParsers = this.roundupParser.getParsers()
            .stream()
            .map(p -> p.withLocale(locale))
            .collect(Collectors.toList());
        return new JavaDateFormatter(format, printFormat, printer.withLocale(locale), roundUpParsers, parsers, canCacheLastParsedFormatter);
    }

    @Override
    public String format(TemporalAccessor accessor) {
        return printer.format(DateFormatters.from(accessor));
    }

    @Override
    public String pattern() {
        return format;
    }

    @Override
    public String printPattern() {
        return printFormat;
    }

    @Override
    public Locale locale() {
        return this.printer.getLocale();
    }

    @Override
    public ZoneId zone() {
        return this.printer.getZone();
    }

    @Override
    public DateMathParser toDateMathParser() {
        return new JavaDateMathParser(format, this, getRoundupParser());
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale(), printer.getZone(), format);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(this.getClass()) == false) {
            return false;
        }
        JavaDateFormatter other = (JavaDateFormatter) obj;

        return Objects.equals(format, other.format)
            && Objects.equals(locale(), other.locale())
            && Objects.equals(this.printer.getZone(), other.printer.getZone());
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "format[%s] locale[%s]", format, locale());
    }

    Collection<OpenSearchDateTimeFormatter> getParsers() {
        return parsers;
    }
}
