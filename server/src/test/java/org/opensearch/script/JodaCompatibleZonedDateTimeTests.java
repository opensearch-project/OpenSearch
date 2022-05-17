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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.script;

import org.opensearch.common.time.DateFormatters;
import org.opensearch.test.OpenSearchTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class JodaCompatibleZonedDateTimeTests extends OpenSearchTestCase {
    private JodaCompatibleZonedDateTime javaTime;
    private DateTime jodaTime;

    @Before
    public void setupTime() {
        long millis = randomIntBetween(0, Integer.MAX_VALUE);
        javaTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        jodaTime = new DateTime(millis, DateTimeZone.forOffsetHours(-7));
    }

    public void testEquals() {
        assertThat(javaTime, equalTo(javaTime));
    }

    public void testToString() {
        assertThat(javaTime.toString(), equalTo(jodaTime.toString()));
    }

    public void testDayOfMonth() {
        assertThat(javaTime.getDayOfMonth(), equalTo(jodaTime.getDayOfMonth()));
    }

    public void testDayOfYear() {
        assertThat(javaTime.getDayOfYear(), equalTo(jodaTime.getDayOfYear()));
    }

    public void testHour() {
        assertThat(javaTime.getHour(), equalTo(jodaTime.getHourOfDay()));
    }

    public void testLocalDate() {
        assertThat(javaTime.toLocalDate(), equalTo(LocalDate.of(jodaTime.getYear(), jodaTime.getMonthOfYear(), jodaTime.getDayOfMonth())));
    }

    public void testLocalDateTime() {
        LocalDateTime dt = LocalDateTime.of(
            jodaTime.getYear(),
            jodaTime.getMonthOfYear(),
            jodaTime.getDayOfMonth(),
            jodaTime.getHourOfDay(),
            jodaTime.getMinuteOfHour(),
            jodaTime.getSecondOfMinute(),
            jodaTime.getMillisOfSecond() * 1000000
        );
        assertThat(javaTime.toLocalDateTime(), equalTo(dt));
    }

    public void testMinute() {
        assertThat(javaTime.getMinute(), equalTo(jodaTime.getMinuteOfHour()));
    }

    public void testMonth() {
        assertThat(javaTime.getMonth(), equalTo(Month.of(jodaTime.getMonthOfYear())));
    }

    public void testMonthValue() {
        assertThat(javaTime.getMonthValue(), equalTo(jodaTime.getMonthOfYear()));
    }

    public void testNano() {
        assertThat(javaTime.getNano(), equalTo(jodaTime.getMillisOfSecond() * 1000000));
    }

    public void testSecond() {
        assertThat(javaTime.getSecond(), equalTo(jodaTime.getSecondOfMinute()));
    }

    public void testYear() {
        assertThat(javaTime.getYear(), equalTo(jodaTime.getYear()));
    }

    public void testZone() {
        assertThat(javaTime.getZone().getId(), equalTo(jodaTime.getZone().getID()));
    }

    public void testMillis() {
        assertThat(javaTime.toInstant().toEpochMilli(), equalTo(jodaTime.getMillis()));
    }

    public void testCenturyOfEra() {
        assertThat(javaTime.get(ChronoField.YEAR_OF_ERA) / 100, equalTo(jodaTime.getCenturyOfEra()));
    }

    public void testEra() {
        assertThat(javaTime.get(ChronoField.ERA), equalTo(jodaTime.getEra()));
    }

    public void testHourOfDay() {
        assertThat(javaTime.getHour(), equalTo(jodaTime.getHourOfDay()));
    }

    public void testMillisOfDay() {
        assertThat(javaTime.get(ChronoField.MILLI_OF_DAY), equalTo(jodaTime.getMillisOfDay()));
    }

    public void testMillisOfSecond() {
        assertThat(javaTime.get(ChronoField.MILLI_OF_SECOND), equalTo(jodaTime.getMillisOfSecond()));
    }

    public void testMinuteOfDay() {
        assertThat(javaTime.get(ChronoField.MINUTE_OF_DAY), equalTo(jodaTime.getMinuteOfDay()));
    }

    public void testMinuteOfHour() {
        assertThat(javaTime.getMinute(), equalTo(jodaTime.getMinuteOfHour()));
    }

    public void testMonthOfYear() {
        assertThat(javaTime.getMonthValue(), equalTo(jodaTime.getMonthOfYear()));
    }

    public void testSecondOfDay() {
        assertThat(javaTime.get(ChronoField.SECOND_OF_DAY), equalTo(jodaTime.getSecondOfDay()));
    }

    public void testSecondOfMinute() {
        assertThat(javaTime.getSecond(), equalTo(jodaTime.getSecondOfMinute()));
    }

    public void testWeekOfWeekyear() {
        assertThat(javaTime.get(DateFormatters.WEEK_FIELDS_ROOT.weekOfWeekBasedYear()), equalTo(jodaTime.getWeekOfWeekyear()));
    }

    public void testWeekyear() {
        assertThat(javaTime.get(DateFormatters.WEEK_FIELDS_ROOT.weekBasedYear()), equalTo(jodaTime.getWeekyear()));
    }

    public void testYearOfCentury() {
        assertThat(javaTime.get(ChronoField.YEAR_OF_ERA) % 100, equalTo(jodaTime.getYearOfCentury()));
    }

    public void testYearOfEra() {
        assertThat(javaTime.get(ChronoField.YEAR_OF_ERA), equalTo(jodaTime.getYearOfEra()));
    }

    public void testToString2() {
        assertThat(DateTimeFormatter.ofPattern("EEE", Locale.GERMANY).format(javaTime), equalTo(jodaTime.toString("EEE", Locale.GERMANY)));
    }

    public void testDayOfWeek() {
        assertThat(javaTime.getDayOfWeekEnum().getValue(), equalTo(jodaTime.getDayOfWeek()));
    }

    public void testDayOfWeekEnum() {
        assertThat(javaTime.getDayOfWeekEnum(), equalTo(DayOfWeek.of(jodaTime.getDayOfWeek())));
    }

    public void testIsEqual() {
        assertTrue(javaTime.isEqual(javaTime));
    }

    public void testIsAfter() {
        long millis = randomLongBetween(0, Integer.MAX_VALUE / 2);
        JodaCompatibleZonedDateTime beforeTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        millis = randomLongBetween(millis + 1, Integer.MAX_VALUE);
        JodaCompatibleZonedDateTime afterTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        assertTrue(afterTime.isAfter(beforeTime));
    }

    public void testIsBefore() {
        long millis = randomLongBetween(0, Integer.MAX_VALUE / 2);
        JodaCompatibleZonedDateTime beforeTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        millis = randomLongBetween(millis + 1, Integer.MAX_VALUE);
        JodaCompatibleZonedDateTime afterTime = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(-7));
        assertTrue(beforeTime.isBefore(afterTime));
    }
}
