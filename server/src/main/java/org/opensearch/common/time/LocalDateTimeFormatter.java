/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * Defines a local datetime format where the date is mandatory and the time is optional.
 * <p>
 * The returned formatter can only be used for parsing, printing is unsupported.
 * <p>
 * This parser can parse local datetimes without zone information.
 * The parser is strict by default, thus time string {@code 24:00} cannot be parsed.
 * <p>
 * It accepts formats described by the following syntax:
 * <pre>
 * Year:
 *       YYYY (eg 1997)
 *    Year and month:
 *       YYYY-MM (eg 1997-07)
 *    Complete date:
 *       YYYY-MM-DD (eg 1997-07-16)
 *    Complete date plus hours and minutes:
 *       YYYY-MM-DDTSPhh:mm (eg 1997-07-16 19:20)
 *    Complete date plus hours, minutes and seconds:
 *       YYYY-MM-DDTSPhh:mm:ss (eg 1997-07-16 19:20:30)
 *    Complete date plus hours, minutes, seconds and a decimal fraction of a second
 *       YYYY-MM-DDTSPhh:mm:ss.s (eg 1997-07-16T19:20:30.45)
 *       YYYY-MM-DDTSPhh:mm:ss,s (eg 1997-07-16T19:20:30,45)
 * where:
 *
 *      YYYY = four-digit year
 *      MM   = two-digit month (01=January, etc.)
 *      DD   = two-digit day of month (01 through 31)
 *      hh   = two digits of hour (00 through 23) (am/pm NOT allowed)
 *      mm   = two digits of minute (00 through 59)
 *      ss   = two digits of second (00 through 59)
 *      s    = one or more(max 9) digits representing a decimal fraction of a second
 *      TSP  = date time seperator (T or " ")
 * </pre>
 */
final class LocalDateTimeFormatter extends OpenSearchDateTimeFormatter {

    private ZoneId zone;

    public LocalDateTimeFormatter(String pattern) {
        super(pattern);
    }

    public LocalDateTimeFormatter(java.time.format.DateTimeFormatter formatter) {
        super(formatter);
    }

    public LocalDateTimeFormatter(java.time.format.DateTimeFormatter formatter, ZoneId zone) {
        super(formatter);
        this.zone = zone;
    }

    @Override
    public OpenSearchDateTimeFormatter withZone(ZoneId zoneId) {
        return new LocalDateTimeFormatter(getFormatter().withZone(zoneId), zoneId);
    }

    @Override
    public OpenSearchDateTimeFormatter withLocale(Locale locale) {
        return new LocalDateTimeFormatter(getFormatter().withLocale(locale));
    }

    @Override
    public Object parseObject(String text, ParsePosition pos) {
        try {
            return parse(text);
        } catch (DateTimeException e) {
            return null;
        }
    }

    @Override
    public TemporalAccessor parse(final String dateTime) {
        LocalDateTime parsedDatetime = parse(dateTime, new ParsePosition(0)).toLocalDatetime();
        return zone == null ? parsedDatetime : parsedDatetime.atZone(zone);
    }

    public DateTime parse(String date, ParsePosition pos) {
        if (date == null) {
            throw new NullPointerException("date cannot be null");
        }

        final int len = date.length() - pos.getIndex();
        if (len <= 0) {
            throw new DateTimeParseException("out of bound parse position", date, pos.getIndex());
        }
        final char[] chars = date.substring(pos.getIndex()).toCharArray();

        // Date portion

        // YEAR
        final int years = getYear(chars, pos);
        if (4 == len) {
            return DateTime.ofYear(years);
        }

        // MONTH
        DateUtils.consumeChar(chars, pos, DateUtils.DATE_SEPARATOR);
        final int months = getMonth(chars, pos);
        if (7 == len) {
            return DateTime.ofYearMonth(years, months);
        }

        // DAY
        DateUtils.consumeChar(chars, pos, DateUtils.DATE_SEPARATOR);
        final int days = getDay(chars, pos);
        if (10 == len) {
            return DateTime.ofDate(years, months, days);
        }

        // HOURS
        DateUtils.consumeChar(chars, pos, DateUtils.SEPARATOR_UPPER, DateUtils.SEPARATOR_LOWER, DateUtils.SEPARATOR_SPACE);
        final int hours = getHour(chars, pos);

        // MINUTES
        DateUtils.consumeChar(chars, pos, DateUtils.TIME_SEPARATOR);
        final int minutes = getMinute(chars, pos);
        if (16 == len) {
            return DateTime.of(years, months, days, hours, minutes, null);
        }

        // SECONDS or TIMEZONE
        return handleTime(chars, pos, years, months, days, hours, minutes);
    }

    private static int getHour(final char[] chars, ParsePosition pos) {
        return DateUtils.readInt(chars, pos, 2);
    }

    private static int getMinute(final char[] chars, ParsePosition pos) {
        return DateUtils.readInt(chars, pos, 2);
    }

    private static int getDay(final char[] chars, ParsePosition pos) {
        return DateUtils.readInt(chars, pos, 2);
    }

    private static DateTime handleTime(char[] chars, ParsePosition pos, int year, int month, int day, int hour, int minute) {
        DateUtils.consumeChar(chars, pos, DateUtils.TIME_SEPARATOR);
        return handleSeconds(year, month, day, hour, minute, chars, pos);
    }

    private static int getMonth(final char[] chars, ParsePosition pos) {
        return DateUtils.readInt(chars, pos, 2);
    }

    private static int getYear(final char[] chars, ParsePosition pos) {
        return DateUtils.readInt(chars, pos, 4);
    }

    private static int getSeconds(final char[] chars, ParsePosition pos) {
        return DateUtils.readInt(chars, pos, 2);
    }

    private static int getFractions(final char[] chars, final ParsePosition pos, final int len) {
        final int fractions;
        fractions = DateUtils.readIntUnchecked(chars, pos, len);
        switch (len) {
            case 0:
                throw new DateTimeParseException("Must have at least 1 fraction digit", new String(chars), pos.getIndex());
            case 1:
                return fractions * 100_000_000;
            case 2:
                return fractions * 10_000_000;
            case 3:
                return fractions * 1_000_000;
            case 4:
                return fractions * 100_000;
            case 5:
                return fractions * 10_000;
            case 6:
                return fractions * 1_000;
            case 7:
                return fractions * 100;
            case 8:
                return fractions * 10;
            default:
                return fractions;
        }
    }

    private static DateTime handleSeconds(int year, int month, int day, int hour, int minute, char[] chars, ParsePosition pos) {
        // From here the specification is more lenient
        final int seconds = getSeconds(chars, pos);
        int currPos = pos.getIndex();
        final int remaining = chars.length - currPos;
        if (remaining == 0) {
            return DateTime.of(year, month, day, hour, minute, seconds, 0, null, 0);
        }

        int fractions = 0;
        int fractionDigits = 0;
        if (remaining >= 1 && DateUtils.checkPositionContains(chars, pos, DateUtils.FRACTION_SEPARATOR_1, DateUtils.FRACTION_SEPARATOR_2)) {
            // We have fractional seconds;
            DateUtils.consumeNextChar(chars, pos);
            ParsePosition initPosition = new ParsePosition(pos.getIndex());
            DateUtils.consumeDigits(chars, pos);
            // We have an end of fractions
            final int len = pos.getIndex() - initPosition.getIndex();
            fractions = getFractions(chars, initPosition, len);
            fractionDigits = len;
            DateUtils.assertNoMoreChars(chars, pos);
        } else {
            throw new DateTimeParseException("Unexpected character at position " + (pos.getIndex()), new String(chars), pos.getIndex());
        }

        return fractionDigits > 0
            ? DateTime.of(year, month, day, hour, minute, seconds, fractions, null, fractionDigits)
            : DateTime.of(year, month, day, hour, minute, seconds, null);
    }
}
