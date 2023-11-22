/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Objects;
import java.util.Optional;

/**
 * Container class for parsed date/date-time data.
 */
class DateTime {
    private final int year;
    private final int month;
    private final int day;
    private final int hour;
    private final int minute;
    private final int second;
    private final int nano;
    private final ZoneOffset offset;
    private final int fractionDigits;

    public DateTime(
        final int year,
        final int month,
        final int day,
        final int hour,
        final int minute,
        final int second,
        final int nano,
        final ZoneOffset offset,
        final int fractionDigits
    ) {
        this.year = year;
        this.month = assertSize(month, 1, 12, ChronoField.MONTH_OF_YEAR);
        this.day = assertSize(day, 1, 31, ChronoField.DAY_OF_MONTH);
        this.hour = assertSize(hour, 0, 23, ChronoField.HOUR_OF_DAY);
        this.minute = assertSize(minute, 0, 59, ChronoField.MINUTE_OF_HOUR);
        this.second = assertSize(second, 0, 60, ChronoField.SECOND_OF_MINUTE);
        this.nano = assertSize(nano, 0, 999_999_999, ChronoField.NANO_OF_SECOND);
        this.offset = offset;
        this.fractionDigits = fractionDigits;
    }

    /**
     * Create a new instance with minute granularity from the input parameters
     */
    public static DateTime of(int year, int month, int day, int hour, int minute, ZoneOffset offset) {
        return new DateTime(year, month, day, hour, minute, 0, 0, offset, 0);
    }

    /**
     * Create a new instance with second granularity from the input parameters
     */
    public static DateTime of(int year, int month, int day, int hour, int minute, int second, ZoneOffset offset) {
        return new DateTime(year, month, day, hour, minute, second, 0, offset, 0);
    }

    /**
     * Create a new instance with nanosecond granularity from the input parameters
     */
    public static DateTime of(
        int year,
        int month,
        int day,
        int hour,
        int minute,
        int second,
        int nanos,
        ZoneOffset offset,
        final int fractionDigits
    ) {
        return new DateTime(year, month, day, hour, minute, second, nanos, offset, fractionDigits);
    }

    /**
     * Create a new instance with year granularity from the input parameters
     */
    public static DateTime ofYear(int year) {
        return new DateTime(year, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC, 0);
    }

    /**
     * Create a new instance with year-month granularity from the input parameters
     */
    public static DateTime ofYearMonth(int years, int months) {
        return new DateTime(years, months, 1, 0, 0, 0, 0, ZoneOffset.UTC, 0);
    }

    /**
     * Create a new instance with day granularity from the input parameters
     */
    public static DateTime ofDate(int years, int months, int days) {
        return new DateTime(years, months, days, 0, 0, 0, 0, ZoneOffset.UTC, 0);
    }

    private int assertSize(int value, int min, int max, ChronoField field) {
        if (value > max) {
            throw new DateTimeException("Field " + field.name() + " out of bounds. Expected " + min + "-" + max + ", got " + value);
        }
        return value;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDayOfMonth() {
        return day;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }

    public int getSecond() {
        return second;
    }

    public int getNano() {
        return nano;
    }

    /**
     * Returns the time offset, if available
     *
     * @return the time offset, if available
     */
    public Optional<ZoneOffset> getOffset() {
        return Optional.ofNullable(offset);
    }

    /**
     * Creates an {@link OffsetDateTime}
     *
     * @return the {@link OffsetDateTime}
     */
    public OffsetDateTime toOffsetDatetime() {
        if (offset != null) {
            return OffsetDateTime.of(year, month, day, hour, minute, second, nano, offset);
        }
        throw new DateTimeException("No zone offset information found");
    }

    /**
     * * @hidden
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DateTime dateTime = (DateTime) o;
        return year == dateTime.year
            && month == dateTime.month
            && day == dateTime.day
            && hour == dateTime.hour
            && minute == dateTime.minute
            && second == dateTime.second
            && nano == dateTime.nano
            && fractionDigits == dateTime.fractionDigits
            && Objects.equals(offset, dateTime.offset);
    }

    /**
     * @hidden
     */
    @Override
    public int hashCode() {
        return Objects.hash(year, month, day, hour, minute, second, nano, offset, fractionDigits);
    }
}
