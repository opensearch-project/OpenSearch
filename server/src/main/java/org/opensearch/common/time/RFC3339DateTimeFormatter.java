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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * Defines a close profile of RFC3339 datetime format where the date is mandatory and the time is optional.
 * <p>
 * The returned formatter can only be used for parsing, printing is unsupported.
 * <p>
 * This parser can parse zoned datetimes.
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
 *       YYYY-MM-DDThh:mmTZD (eg 1997-07-16T19:20+01:00)
 *    Complete date plus hours, minutes and seconds:
 *       YYYY-MM-DDThh:mm:ssTZD (eg 1997-07-16T19:20:30+01:00)
 *    Complete date plus hours, minutes, seconds and a decimal fraction of a second
 *       YYYY-MM-DDThh:mm:ss.sTZD (eg 1997-07-16T19:20:30.45+01:00)
 *       YYYY-MM-DDThh:mm:ss,sTZD (eg 1997-07-16T19:20:30,45+01:00)
 * where:
 *
 *      YYYY = four-digit year
 *      MM   = two-digit month (01=January, etc.)
 *      DD   = two-digit day of month (01 through 31)
 *      hh   = two digits of hour (00 through 23) (am/pm NOT allowed)
 *      mm   = two digits of minute (00 through 59)
 *      ss   = two digits of second (00 through 59)
 *      s    = one or more(max 9) digits representing a decimal fraction of a second
 *      TZD  = time zone designator (Z or z or +hh:mm or -hh:mm)
 * </pre>
 */
final class RFC3339DateTimeFormatter extends OpenSearchDateTimeFormatter {

    private ZoneId zone;

    public RFC3339DateTimeFormatter(String pattern) {
        super(pattern);
    }

    public RFC3339DateTimeFormatter(java.time.format.DateTimeFormatter formatter) {
        super(formatter);
    }

    public RFC3339DateTimeFormatter(java.time.format.DateTimeFormatter formatter, ZoneId zone) {
        super(formatter);
        this.zone = zone;
    }

    @Override
    public OpenSearchDateTimeFormatter withZone(ZoneId zoneId) {
        return new RFC3339DateTimeFormatter(getFormatter().withZone(zoneId), zoneId);
    }

    @Override
    public OpenSearchDateTimeFormatter withLocale(Locale locale) {
        return new RFC3339DateTimeFormatter(getFormatter().withLocale(locale));
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
        OffsetDateTime parsedDatetime = parse(dateTime, new ParsePosition(0)).toOffsetDatetime();
        return zone == null ? parsedDatetime : parsedDatetime.atZoneSameInstant(zone);
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

    private static ZoneOffset parseTimezone(char[] chars, ParsePosition pos) {
        int offset = pos.getIndex();
        final int left = chars.length - offset;
        if (DateUtils.checkPositionContains(chars, pos, DateUtils.ZULU_LOWER, DateUtils.ZULU_UPPER)) {
            DateUtils.consumeNextChar(chars, pos);
            DateUtils.assertNoMoreChars(chars, pos);
            return ZoneOffset.UTC;
        }

        if (left != 6) {
            throw new DateTimeParseException("Invalid timezone offset", new String(chars, offset, left), offset);
        }

        final char sign = chars[offset];
        DateUtils.consumeNextChar(chars, pos);
        int hours = getHour(chars, pos);
        DateUtils.consumeChar(chars, pos, DateUtils.TIME_SEPARATOR);
        int minutes = getMinute(chars, pos);
        if (sign == DateUtils.MINUS) {
            if (hours == 0 && minutes == 0) {
                throw new DateTimeParseException("Unknown 'Local Offset Convention' date-time not allowed", new String(chars), offset);
            }
            hours = -hours;
            minutes = -minutes;
        } else if (sign != DateUtils.PLUS) {
            throw new DateTimeParseException("Invalid character starting at position " + offset, new String(chars), offset);
        }

        return ZoneOffset.ofHoursMinutes(hours, minutes);
    }

    private static DateTime handleTime(char[] chars, ParsePosition pos, int year, int month, int day, int hour, int minute) {
        switch (chars[pos.getIndex()]) {
            case DateUtils.TIME_SEPARATOR:
                DateUtils.consumeChar(chars, pos, DateUtils.TIME_SEPARATOR);
                return handleSeconds(year, month, day, hour, minute, chars, pos);

            case DateUtils.PLUS:
            case DateUtils.MINUS:
            case DateUtils.ZULU_UPPER:
            case DateUtils.ZULU_LOWER:
                final ZoneOffset zoneOffset = parseTimezone(chars, pos);
                return DateTime.of(year, month, day, hour, minute, zoneOffset);
        }
        throw new DateTimeParseException("Unexpected character " + " at position " + pos.getIndex(), new String(chars), pos.getIndex());
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

        ZoneOffset offset = null;
        int fractions = 0;
        int fractionDigits = 0;
        if (remaining == 1 && DateUtils.checkPositionContains(chars, pos, DateUtils.ZULU_LOWER, DateUtils.ZULU_UPPER)) {
            DateUtils.consumeNextChar(chars, pos);
            // Do nothing we are done
            offset = ZoneOffset.UTC;
            DateUtils.assertNoMoreChars(chars, pos);
        } else if (remaining >= 1 && DateUtils.checkPositionContains(chars, pos, DateUtils.FRACTION_SEPARATOR_1, DateUtils.FRACTION_SEPARATOR_2)) {
            // We have fractional seconds;
            DateUtils.consumeNextChar(chars, pos);
            ParsePosition initPosition = new ParsePosition(pos.getIndex());
            DateUtils.consumeDigits(chars, pos);
            if (pos.getErrorIndex() == -1) {
                // We have an end of fractions
                final int len = pos.getIndex() - initPosition.getIndex();
                fractions = getFractions(chars, initPosition, len);
                fractionDigits = len;
                offset = parseTimezone(chars, pos);
            } else {
                throw new DateTimeParseException("No timezone information", new String(chars), pos.getIndex());
            }
        } else if (remaining >= 1 && DateUtils.checkPositionContains(chars, pos, DateUtils.PLUS, DateUtils.MINUS)) {
            // No fractional sections
            offset = parseTimezone(chars, pos);
        } else {
            throw new DateTimeParseException("Unexpected character at position " + (pos.getIndex()), new String(chars), pos.getIndex());
        }

        return fractionDigits > 0
            ? DateTime.of(year, month, day, hour, minute, seconds, fractions, offset, fractionDigits)
            : DateTime.of(year, month, day, hour, minute, seconds, offset);
    }
}
