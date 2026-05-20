/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Based on code from the Internet Time Utility project (https://github.com/ethlo/itu) under the Apache License, version 2.0.
 * Copyright (C) 2017 Morten Haraldsen (ethlo)
 * Modifications (C) OpenSearch Contributors. All Rights Reserved.
 */

package org.opensearch.common.time;

import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
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
final class RFC3339CompatibleDateTimeFormatter extends OpenSearchDateTimeFormatter {
    public static final char DATE_SEPARATOR = '-';
    public static final char TIME_SEPARATOR = ':';
    public static final char SEPARATOR_UPPER = 'T';
    private static final char PLUS = '+';
    private static final char MINUS = '-';
    private static final char SEPARATOR_LOWER = 't';
    private static final char SEPARATOR_SPACE = ' ';
    private static final char FRACTION_SEPARATOR_1 = '.';
    private static final char FRACTION_SEPARATOR_2 = ',';
    private static final char ZULU_UPPER = 'Z';
    private static final char ZULU_LOWER = 'z';

    private ZoneId zone;

    public RFC3339CompatibleDateTimeFormatter(String pattern) {
        super(pattern);
    }

    public RFC3339CompatibleDateTimeFormatter(java.time.format.DateTimeFormatter formatter) {
        super(formatter);
    }

    public RFC3339CompatibleDateTimeFormatter(java.time.format.DateTimeFormatter formatter, ZoneId zone) {
        super(formatter);
        this.zone = zone;
    }

    @Override
    public OpenSearchDateTimeFormatter withZone(ZoneId zoneId) {
        return new RFC3339CompatibleDateTimeFormatter(getFormatter().withZone(zoneId), zoneId);
    }

    @Override
    public OpenSearchDateTimeFormatter withLocale(Locale locale) {
        return new RFC3339CompatibleDateTimeFormatter(getFormatter().withLocale(locale));
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
        OffsetDateTime parsedDatetime = parse(dateTime, new ParsePosition(0));
        return zone == null ? parsedDatetime : parsedDatetime.atZoneSameInstant(zone);
    }

    public OffsetDateTime parse(String date, ParsePosition pos) {
        if (date == null) {
            throw new IllegalArgumentException("date cannot be null");
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
            return OffsetDateTime.of(years, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        }

        // MONTH
        consumeChar(chars, pos, DATE_SEPARATOR);
        final int months = getMonth(chars, pos);
        if (7 == len) {
            return OffsetDateTime.of(years, months, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        }

        // DAY
        consumeChar(chars, pos, DATE_SEPARATOR);
        final int days = getDay(chars, pos);
        if (10 == len) {
            return OffsetDateTime.of(years, months, days, 0, 0, 0, 0, ZoneOffset.UTC);
        }

        // HOURS
        consumeChar(chars, pos, SEPARATOR_UPPER, SEPARATOR_LOWER, SEPARATOR_SPACE);
        final int hours = getHour(chars, pos);

        // MINUTES
        consumeChar(chars, pos, TIME_SEPARATOR);
        final int minutes = getMinute(chars, pos);
        if (16 == len) {
            throw new DateTimeParseException("No timezone offset information", new String(chars), pos.getIndex());
        }

        // SECONDS or TIMEZONE
        return handleTime(chars, pos, years, months, days, hours, minutes);
    }

    private static boolean isDigit(char c) {
        return (c >= '0' && c <= '9');
    }

    private static int digit(char c) {
        return c - '0';
    }

    private static int readInt(final char[] strNum, ParsePosition pos, int n) {
        int start = pos.getIndex(), end = start + n;
        if (end > strNum.length) {
            pos.setErrorIndex(end);
            throw new DateTimeParseException("Unexpected end of expression at position " + strNum.length, new String(strNum), end);
        }

        int result = 0;
        for (int i = start; i < end; i++) {
            final char c = strNum[i];
            if (isDigit(c) == false) {
                pos.setErrorIndex(i);
                throw new DateTimeParseException("Character " + c + " is not a digit", new String(strNum), i);
            }
            int digit = digit(c);
            result = result * 10 + digit;
        }
        pos.setIndex(end);
        return result;
    }

    private static int readIntUnchecked(final char[] strNum, ParsePosition pos, int n) {
        int start = pos.getIndex(), end = start + n;
        int result = 0;
        for (int i = start; i < end; i++) {
            final char c = strNum[i];
            int digit = digit(c);
            result = result * 10 + digit;
        }
        pos.setIndex(end);
        return result;
    }

    private static int getHour(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static int getMinute(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static int getDay(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static boolean isValidOffset(char[] chars, int offset) {
        return offset < chars.length;
    }

    private static void consumeChar(char[] chars, ParsePosition pos, char expected) {
        int offset = pos.getIndex();
        if (isValidOffset(chars, offset) == false) {
            throw new DateTimeParseException("Unexpected end of input", new String(chars), offset);
        }

        if (chars[offset] != expected) {
            throw new DateTimeParseException("Expected character " + expected + " at position " + offset, new String(chars), offset);
        }
        pos.setIndex(offset + 1);
    }

    private static void consumeNextChar(char[] chars, ParsePosition pos) {
        int offset = pos.getIndex();
        if (isValidOffset(chars, offset) == false) {
            throw new DateTimeParseException("Unexpected end of input", new String(chars), offset);
        }
        pos.setIndex(offset + 1);
    }

    private static boolean checkPositionContains(char[] chars, ParsePosition pos, char... expected) {
        int offset = pos.getIndex();
        if (offset >= chars.length) {
            throw new DateTimeParseException("Unexpected end of input", new String(chars), offset);
        }

        boolean found = false;
        for (char e : expected) {
            if (chars[offset] == e) {
                found = true;
                break;
            }
        }
        return found;
    }

    private static void consumeChar(char[] chars, ParsePosition pos, char... expected) {
        int offset = pos.getIndex();
        if (offset >= chars.length) {
            throw new DateTimeParseException("Unexpected end of input", new String(chars), offset);
        }

        boolean found = false;
        for (char e : expected) {
            if (chars[offset] == e) {
                found = true;
                pos.setIndex(offset + 1);
                break;
            }
        }
        if (!found) {
            throw new DateTimeParseException(
                "Expected character " + Arrays.toString(expected) + " at position " + offset,
                new String(chars),
                offset
            );
        }
    }

    private static void assertNoMoreChars(char[] chars, ParsePosition pos) {
        if (chars.length > pos.getIndex()) {
            throw new DateTimeParseException("Trailing junk data after position " + pos.getIndex(), new String(chars), pos.getIndex());
        }
    }

    private static ZoneOffset parseTimezone(char[] chars, ParsePosition pos) {
        int offset = pos.getIndex();
        final int left = chars.length - offset;
        if (checkPositionContains(chars, pos, ZULU_LOWER, ZULU_UPPER)) {
            consumeNextChar(chars, pos);
            assertNoMoreChars(chars, pos);
            return ZoneOffset.UTC;
        }

        if (left != 6) {
            throw new DateTimeParseException("Invalid timezone offset", new String(chars, offset, left), offset);
        }

        final char sign = chars[offset];
        consumeNextChar(chars, pos);
        int hours = getHour(chars, pos);
        consumeChar(chars, pos, TIME_SEPARATOR);
        int minutes = getMinute(chars, pos);
        if (sign == MINUS) {
            if (hours == 0 && minutes == 0) {
                throw new DateTimeParseException("Unknown 'Local Offset Convention' date-time not allowed", new String(chars), offset);
            }
            hours = -hours;
            minutes = -minutes;
        } else if (sign != PLUS) {
            throw new DateTimeParseException("Invalid character starting at position " + offset, new String(chars), offset);
        }

        return ZoneOffset.ofHoursMinutes(hours, minutes);
    }

    private static OffsetDateTime handleTime(char[] chars, ParsePosition pos, int year, int month, int day, int hour, int minute) {
        switch (chars[pos.getIndex()]) {
            case TIME_SEPARATOR:
                consumeChar(chars, pos, TIME_SEPARATOR);
                return handleSeconds(year, month, day, hour, minute, chars, pos);

            case PLUS:
            case MINUS:
            case ZULU_UPPER:
            case ZULU_LOWER:
                final ZoneOffset zoneOffset = parseTimezone(chars, pos);
                return OffsetDateTime.of(year, month, day, hour, minute, 0, 0, zoneOffset);
        }
        throw new DateTimeParseException("Unexpected character " + " at position " + pos.getIndex(), new String(chars), pos.getIndex());
    }

    private static int getMonth(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static int getYear(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 4);
    }

    private static int getSeconds(final char[] chars, ParsePosition pos) {
        return readInt(chars, pos, 2);
    }

    private static int getFractions(final char[] chars, final ParsePosition pos, final int len) {
        final int fractions;
        fractions = readIntUnchecked(chars, pos, len);
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

    public static int indexOfNonDigit(final char[] text, int offset) {
        for (int i = offset; i < text.length; i++) {
            if (isDigit(text[i]) == false) {
                return i;
            }
        }
        return -1;
    }

    public static void consumeDigits(final char[] text, ParsePosition pos) {
        final int idx = indexOfNonDigit(text, pos.getIndex());
        if (idx == -1) {
            pos.setErrorIndex(text.length);
        } else {
            pos.setIndex(idx);
        }
    }

    private static OffsetDateTime handleSeconds(int year, int month, int day, int hour, int minute, char[] chars, ParsePosition pos) {
        // From here the specification is more lenient
        final int seconds = getSeconds(chars, pos);
        int currPos = pos.getIndex();
        final int remaining = chars.length - currPos;
        if (remaining == 0) {
            // No offset
            throw new DateTimeParseException("No timezone offset information", new String(chars), pos.getIndex());
        }

        ZoneOffset offset = null;
        int fractions = 0;
        if (remaining == 1 && checkPositionContains(chars, pos, ZULU_LOWER, ZULU_UPPER)) {
            consumeNextChar(chars, pos);
            // Do nothing we are done
            offset = ZoneOffset.UTC;
            assertNoMoreChars(chars, pos);
        } else if (remaining >= 1 && checkPositionContains(chars, pos, FRACTION_SEPARATOR_1, FRACTION_SEPARATOR_2)) {
            // We have fractional seconds;
            consumeNextChar(chars, pos);
            ParsePosition initPosition = new ParsePosition(pos.getIndex());
            consumeDigits(chars, pos);
            if (pos.getErrorIndex() == -1) {
                // We have an end of fractions
                final int len = pos.getIndex() - initPosition.getIndex();
                fractions = getFractions(chars, initPosition, len);
                offset = parseTimezone(chars, pos);
            } else {
                throw new DateTimeParseException("No timezone offset information", new String(chars), pos.getIndex());
            }
        } else if (remaining >= 1 && checkPositionContains(chars, pos, PLUS, MINUS)) {
            // No fractional sections
            offset = parseTimezone(chars, pos);
        } else {
            throw new DateTimeParseException("Unexpected character at position " + (pos.getIndex()), new String(chars), pos.getIndex());
        }

        return OffsetDateTime.of(year, month, day, hour, minute, seconds, fractions, offset);
    }
}
