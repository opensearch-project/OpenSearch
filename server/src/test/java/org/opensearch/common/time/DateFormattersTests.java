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

import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DateFormattersTests extends OpenSearchTestCase {

    public void testWeekBasedDates() {
        // as per WeekFields.ISO first week starts on Monday and has minimum 4 days
        DateFormatter dateFormatter = DateFormatters.forPattern("YYYY-ww");

        // first week of 2016 starts on Monday 2016-01-04 as previous week in 2016 has only 3 days
        assertThat(
            DateFormatters.from(dateFormatter.parse("2016-01")),
            equalTo(ZonedDateTime.of(2016, 01, 04, 0, 0, 0, 0, ZoneOffset.UTC))
        );

        // first week of 2015 starts on Monday 2014-12-29 because 4days belong to 2019
        assertThat(
            DateFormatters.from(dateFormatter.parse("2015-01")),
            equalTo(ZonedDateTime.of(2014, 12, 29, 0, 0, 0, 0, ZoneOffset.UTC))
        );

        // as per WeekFields.ISO first week starts on Monday and has minimum 4 days
        dateFormatter = DateFormatters.forPattern("YYYY");

        // first week of 2016 starts on Monday 2016-01-04 as previous week in 2016 has only 3 days
        assertThat(DateFormatters.from(dateFormatter.parse("2016")), equalTo(ZonedDateTime.of(2016, 01, 04, 0, 0, 0, 0, ZoneOffset.UTC)));

        // first week of 2015 starts on Monday 2014-12-29 because 4days belong to 2019
        assertThat(DateFormatters.from(dateFormatter.parse("2015")), equalTo(ZonedDateTime.of(2014, 12, 29, 0, 0, 0, 0, ZoneOffset.UTC)));
    }

    // this is not in the duelling tests, because the epoch millis parser in joda time drops the milliseconds after the comma
    // but is able to parse the rest
    // as this feature is supported it also makes sense to make it exact
    public void testEpochMillisParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_millis");
        {
            Instant instant = Instant.from(formatter.parse("12345"));
            assertThat(instant.getEpochSecond(), is(12L));
            assertThat(instant.getNano(), is(345_000_000));
            assertThat(formatter.format(instant), is("12345"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("0"));
            assertThat(instant.getEpochSecond(), is(0L));
            assertThat(instant.getNano(), is(0));
            assertThat(formatter.format(instant), is("0"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-123000.123456"));
            assertThat(instant.getEpochSecond(), is(-124L));
            assertThat(instant.getNano(), is(999876544));
            assertThat(formatter.format(instant), is("-123000.123456"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("123.123456"));
            assertThat(instant.getEpochSecond(), is(0L));
            assertThat(instant.getNano(), is(123123456));
            assertThat(formatter.format(instant), is("123.123456"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-123.123456"));
            assertThat(instant.getEpochSecond(), is(-1L));
            assertThat(instant.getNano(), is(876876544));
            assertThat(formatter.format(instant), is("-123.123456"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6789123.123456"));
            assertThat(instant.getEpochSecond(), is(-6790L));
            assertThat(instant.getNano(), is(876876544));
            assertThat(formatter.format(instant), is("-6789123.123456"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("6789123.123456"));
            assertThat(instant.getEpochSecond(), is(6789L));
            assertThat(instant.getNano(), is(123123456));
            assertThat(formatter.format(instant), is("6789123.123456"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000430768.25"));
            assertThat(instant.getEpochSecond(), is(-6250000431L));
            assertThat(instant.getNano(), is(231750000));
            assertThat(formatter.format(instant), is("-6250000430768.25"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000430768.75"));
            assertThat(instant.getEpochSecond(), is(-6250000431L));
            assertThat(instant.getNano(), is(231250000));
            assertThat(formatter.format(instant), is("-6250000430768.75"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000430768.00"));
            assertThat(instant.getEpochSecond(), is(-6250000431L));
            assertThat(instant.getNano(), is(232000000));
            assertThat(formatter.format(instant), is("-6250000430768")); // remove .00 precision
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000431000.250000"));
            assertThat(instant.getEpochSecond(), is(-6250000432L));
            assertThat(instant.getNano(), is(999750000));
            assertThat(formatter.format(instant), is("-6250000431000.25"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000431000.000001"));
            assertThat(instant.getEpochSecond(), is(-6250000432L));
            assertThat(instant.getNano(), is(999999999));
            assertThat(formatter.format(instant), is("-6250000431000.000001"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000431000.75"));
            assertThat(instant.getEpochSecond(), is(-6250000432L));
            assertThat(instant.getNano(), is(999250000));
            assertThat(formatter.format(instant), is("-6250000431000.75"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000431000.00"));
            assertThat(instant.getEpochSecond(), is(-6250000431L));
            assertThat(instant.getNano(), is(0));
            assertThat(formatter.format(instant), is("-6250000431000"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000431000"));
            assertThat(instant.getEpochSecond(), is(-6250000431L));
            assertThat(instant.getNano(), is(0));
            assertThat(formatter.format(instant), is("-6250000431000"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-6250000430768"));
            assertThat(instant.getEpochSecond(), is(-6250000431L));
            assertThat(instant.getNano(), is(232000000));
            assertThat(formatter.format(instant), is("-6250000430768"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("1680000430768"));
            assertThat(instant.getEpochSecond(), is(1680000430L));
            assertThat(instant.getNano(), is(768000000));
            assertThat(formatter.format(instant), is("1680000430768"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
        {
            Instant instant = Instant.from(formatter.parse("-0.12345"));
            assertThat(instant.getEpochSecond(), is(-1L));
            assertThat(instant.getNano(), is(999876550));
            assertThat(formatter.format(instant), is("-0.12345"));
            assertThat(Instant.from(formatter.parse(formatter.format(instant))), is(instant));
        }
    }

    public void testInvalidEpochMilliParser() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_millis");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("invalid"));
        assertThat(e.getMessage(), containsString("failed to parse date field [invalid] with format [epoch_millis]"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("123.1234567"));
        assertThat(e.getMessage(), containsString("failed to parse date field [123.1234567] with format [epoch_millis]"));
    }

    // this is not in the duelling tests, because the epoch second parser in joda time drops the milliseconds after the comma
    // but is able to parse the rest
    // as this feature is supported it also makes sense to make it exact
    public void testEpochSecondParserWithFraction() {
        DateFormatter formatter = DateFormatters.forPattern("epoch_second");

        TemporalAccessor accessor = formatter.parse("1234.1");
        Instant instant = DateFormatters.from(accessor).toInstant();
        assertThat(instant.getEpochSecond(), is(1234L));
        assertThat(DateFormatters.from(accessor).toInstant().getNano(), is(100_000_000));

        accessor = formatter.parse("1234");
        instant = DateFormatters.from(accessor).toInstant();
        assertThat(instant.getEpochSecond(), is(1234L));
        assertThat(instant.getNano(), is(0));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("abc"));
        assertThat(e.getMessage(), is("failed to parse date field [abc] with format [epoch_second]"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("1234.abc"));
        assertThat(e.getMessage(), is("failed to parse date field [1234.abc] with format [epoch_second]"));

        e = expectThrows(IllegalArgumentException.class, () -> formatter.parse("1234.1234567890"));
        assertThat(e.getMessage(), is("failed to parse date field [1234.1234567890] with format [epoch_second]"));
    }

    public void testEpochMilliParsersWithDifferentFormatters() {
        DateFormatter formatter = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");
        TemporalAccessor accessor = formatter.parse("123");
        assertThat(DateFormatters.from(accessor).toInstant().toEpochMilli(), is(123L));
        assertThat(formatter.pattern(), is("strict_date_optional_time||epoch_millis"));
    }

    public void testParsersWithMultipleInternalFormats() throws Exception {
        ZonedDateTime first = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("2018-05-15T17:14:56+0100")
        );
        ZonedDateTime second = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("2018-05-15T17:14:56+01:00")
        );
        assertThat(first, is(second));
    }

    public void testNanoOfSecondWidth() throws Exception {
        ZonedDateTime first = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("1970-01-01T00:00:00.1")
        );
        assertThat(first.getNano(), is(100000000));
        ZonedDateTime second = DateFormatters.from(
            DateFormatters.forPattern("strict_date_optional_time_nanos").parse("1970-01-01T00:00:00.000000001")
        );
        assertThat(second.getNano(), is(1));
    }

    public void testLocales() {
        assertThat(DateFormatters.forPattern("strict_date_optional_time").locale(), is(Locale.ROOT));
        Locale locale = randomLocale(random());
        assertThat(DateFormatters.forPattern("strict_date_optional_time").withLocale(locale).locale(), is(locale));
    }

    public void testTimeZones() {
        // zone is null by default due to different behaviours between java8 and above
        assertThat(DateFormatters.forPattern("strict_date_optional_time").zone(), is(nullValue()));
        ZoneId zoneId = randomZone();
        assertThat(DateFormatters.forPattern("strict_date_optional_time").withZone(zoneId).zone(), is(zoneId));
    }

    public void testEqualsAndHashcode() {
        assertThat(
            DateFormatters.forPattern("strict_date_optional_time"),
            sameInstance(DateFormatters.forPattern("strict_date_optional_time"))
        );
        assertThat(DateFormatters.forPattern("YYYY"), equalTo(DateFormatters.forPattern("YYYY")));
        assertThat(DateFormatters.forPattern("YYYY").hashCode(), is(DateFormatters.forPattern("YYYY").hashCode()));

        // different timezone, thus not equals
        assertThat(DateFormatters.forPattern("YYYY").withZone(ZoneId.of("CET")), not(equalTo(DateFormatters.forPattern("YYYY"))));

        // different locale, thus not equals
        DateFormatter f1 = DateFormatters.forPattern("YYYY").withLocale(Locale.CANADA);
        DateFormatter f2 = f1.withLocale(Locale.FRENCH);
        assertThat(f1, not(equalTo(f2)));

        // different pattern, thus not equals
        assertThat(DateFormatters.forPattern("YYYY"), not(equalTo(DateFormatters.forPattern("YY"))));

        DateFormatter epochSecondFormatter = DateFormatters.forPattern("epoch_second");
        assertThat(epochSecondFormatter, sameInstance(DateFormatters.forPattern("epoch_second")));
        assertThat(epochSecondFormatter, equalTo(DateFormatters.forPattern("epoch_second")));
        assertThat(epochSecondFormatter.hashCode(), is(DateFormatters.forPattern("epoch_second").hashCode()));

        DateFormatter epochMillisFormatter = DateFormatters.forPattern("epoch_millis");
        assertThat(epochMillisFormatter.hashCode(), is(DateFormatters.forPattern("epoch_millis").hashCode()));
        assertThat(epochMillisFormatter, sameInstance(DateFormatters.forPattern("epoch_millis")));
        assertThat(epochMillisFormatter, equalTo(DateFormatters.forPattern("epoch_millis")));
    }

    public void testSupportBackwardsJava8Format() {
        assertThat(DateFormatter.forPattern("8yyyy-MM-dd"), instanceOf(JavaDateFormatter.class));
        // named formats too
        assertThat(DateFormatter.forPattern("8date_optional_time"), instanceOf(JavaDateFormatter.class));
        // named formats too
        DateFormatter formatter = DateFormatter.forPattern("8date_optional_time||ww-MM-dd");
        assertThat(formatter, instanceOf(JavaDateFormatter.class));
    }

    public void testEpochFormatting() {
        long seconds = randomLongBetween(0, 130L * 365 * 86400); // from 1970 epoch till around 2100
        long nanos = randomLongBetween(0, 999_999_999L);
        Instant instant = Instant.ofEpochSecond(seconds, nanos);
        {
            DateFormatter millisFormatter = DateFormatter.forPattern("epoch_millis");
            String millis = millisFormatter.format(instant);
            Instant millisInstant = Instant.from(millisFormatter.parse(millis));
            assertThat(millisInstant.toEpochMilli(), is(instant.toEpochMilli()));
            assertThat(millisFormatter.format(Instant.ofEpochSecond(42, 0)), is("42000"));
            assertThat(millisFormatter.format(Instant.ofEpochSecond(42, 123456789L)), is("42123.456789"));

            DateFormatter secondsFormatter = DateFormatter.forPattern("epoch_second");
            String formattedSeconds = secondsFormatter.format(instant);
            Instant secondsInstant = Instant.from(secondsFormatter.parse(formattedSeconds));
            assertThat(secondsInstant.getEpochSecond(), is(instant.getEpochSecond()));

            assertThat(secondsFormatter.format(Instant.ofEpochSecond(42, 0)), is("42"));
        }
        {
            DateFormatter isoFormatter = DateFormatters.forPattern("strict_date_optional_time_nanos");
            DateFormatter millisFormatter = DateFormatter.forPattern("epoch_millis");
            String millis = millisFormatter.format(instant);
            String iso8601 = isoFormatter.format(instant);

            Instant millisInstant = Instant.from(millisFormatter.parse(millis));
            Instant isoInstant = Instant.from(isoFormatter.parse(iso8601));

            assertThat(millisInstant.toEpochMilli(), is(isoInstant.toEpochMilli()));
            assertThat(millisInstant.getEpochSecond(), is(isoInstant.getEpochSecond()));
            assertThat(millisInstant.getNano(), is(isoInstant.getNano()));
        }
    }

    public void testEpochFormattingNegativeEpoch() {
        long seconds = randomLongBetween(-130L * 365 * 86400, 0); // around 1840 till 1970 epoch
        long nanos = randomLongBetween(0, 999_999_999L);
        Instant instant = Instant.ofEpochSecond(seconds, nanos);

        {
            DateFormatter millisFormatter = DateFormatter.forPattern("epoch_millis");
            String millis = millisFormatter.format(instant);
            Instant millisInstant = Instant.from(millisFormatter.parse(millis));
            assertThat(millisInstant.toEpochMilli(), is(instant.toEpochMilli()));
            assertThat(millisFormatter.format(Instant.ofEpochSecond(-42, 0)), is("-42000"));
            assertThat(millisFormatter.format(Instant.ofEpochSecond(-42, 123456789L)), is("-41876.543211"));

            DateFormatter secondsFormatter = DateFormatter.forPattern("epoch_second");
            String formattedSeconds = secondsFormatter.format(instant);
            Instant secondsInstant = Instant.from(secondsFormatter.parse(formattedSeconds));
            assertThat(secondsInstant.getEpochSecond(), is(instant.getEpochSecond()));

            assertThat(secondsFormatter.format(Instant.ofEpochSecond(42, 0)), is("42"));
        }
        {
            DateFormatter isoFormatter = DateFormatters.forPattern("strict_date_optional_time_nanos");
            DateFormatter millisFormatter = DateFormatter.forPattern("epoch_millis");
            String millis = millisFormatter.format(instant);
            String iso8601 = isoFormatter.format(instant);

            Instant millisInstant = Instant.from(millisFormatter.parse(millis));
            Instant isoInstant = Instant.from(isoFormatter.parse(iso8601));

            assertThat(millisInstant.toEpochMilli(), is(isoInstant.toEpochMilli()));
            assertThat(millisInstant.getEpochSecond(), is(isoInstant.getEpochSecond()));
            assertThat(millisInstant.getNano(), is(isoInstant.getNano()));
        }
    }

    public void testParsingStrictNanoDates() {
        DateFormatter formatter = DateFormatters.forPattern("strict_date_optional_time_nanos");
        formatter.format(formatter.parse("2016-01-01T00:00:00.000"));
        formatter.format(formatter.parse("2018-05-15T17:14:56"));
        formatter.format(formatter.parse("2018-05-15T17:14:56Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56+01:00"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789+01:00"));
    }

    public void testIso8601Parsing() {
        DateFormatter formatter = DateFormatters.forPattern("iso8601");

        // timezone not allowed with just date
        formatter.format(formatter.parse("2018-05-15"));

        formatter.format(formatter.parse("2018-05-15T17"));
        formatter.format(formatter.parse("2018-05-15T17Z"));
        formatter.format(formatter.parse("2018-05-15T17+0100"));
        formatter.format(formatter.parse("2018-05-15T17+01:00"));

        formatter.format(formatter.parse("2018-05-15T17:14"));
        formatter.format(formatter.parse("2018-05-15T17:14Z"));
        formatter.format(formatter.parse("2018-05-15T17:14-0100"));
        formatter.format(formatter.parse("2018-05-15T17:14-01:00"));

        formatter.format(formatter.parse("2018-05-15T17:14:56"));
        formatter.format(formatter.parse("2018-05-15T17:14:56Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56+01:00"));

        // milliseconds can be separated using comma or decimal point
        formatter.format(formatter.parse("2018-05-15T17:14:56.123"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123-0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123-01:00"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123+01:00"));

        // microseconds can be separated using comma or decimal point
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456+01:00"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456-0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456-01:00"));

        // nanoseconds can be separated using comma or decimal point
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789-0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56.123456789-01:00"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456789"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456789Z"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456789+0100"));
        formatter.format(formatter.parse("2018-05-15T17:14:56,123456789+01:00"));
    }

    public void testRoundupFormatterWithEpochDates() {
        assertRoundupFormatter("epoch_millis", "1234567890", 1234567890L);
        // also check nanos of the epoch_millis formatter if it is rounded up to the nano second
        JavaDateFormatter roundUpFormatter = ((JavaDateFormatter) DateFormatter.forPattern("8epoch_millis")).getRoundupParser();
        Instant epochMilliInstant = DateFormatters.from(roundUpFormatter.parse("1234567890")).toInstant();
        assertThat(epochMilliInstant.getLong(ChronoField.NANO_OF_SECOND), is(890_999_999L));

        assertRoundupFormatter("strict_date_optional_time||epoch_millis", "2018-10-10T12:13:14.123Z", 1539173594123L);
        assertRoundupFormatter("strict_date_optional_time||epoch_millis", "1234567890", 1234567890L);
        assertRoundupFormatter("strict_date_optional_time||epoch_millis", "2018-10-10", 1539215999999L);
        assertRoundupFormatter("strict_date_optional_time||epoch_millis", "2019-01-25T15:37:17.346928Z", 1548430637346L);
        assertRoundupFormatter("uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_millis", "2018-10-10T12:13:14.123", 1539173594123L);
        assertRoundupFormatter("uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_millis", "1234567890", 1234567890L);

        assertRoundupFormatter("epoch_second", "1234567890", 1234567890999L);
        // also check nanos of the epoch_millis formatter if it is rounded up to the nano second
        JavaDateFormatter epochSecondRoundupParser = ((JavaDateFormatter) DateFormatter.forPattern("8epoch_second")).getRoundupParser();
        Instant epochSecondInstant = DateFormatters.from(epochSecondRoundupParser.parse("1234567890")).toInstant();
        assertThat(epochSecondInstant.getLong(ChronoField.NANO_OF_SECOND), is(999_999_999L));

        assertRoundupFormatter("strict_date_optional_time||epoch_second", "2018-10-10T12:13:14.123Z", 1539173594123L);
        assertRoundupFormatter("strict_date_optional_time||epoch_second", "1234567890", 1234567890999L);
        assertRoundupFormatter("strict_date_optional_time||epoch_second", "2018-10-10", 1539215999999L);
        assertRoundupFormatter("uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_second", "2018-10-10T12:13:14.123", 1539173594123L);
        assertRoundupFormatter("uuuu-MM-dd'T'HH:mm:ss.SSS||epoch_second", "1234567890", 1234567890999L);
    }

    private void assertRoundupFormatter(String format, String input, long expectedMilliSeconds) {
        JavaDateFormatter dateFormatter = (JavaDateFormatter) DateFormatter.forPattern(format);
        dateFormatter.parse(input);
        JavaDateFormatter roundUpFormatter = dateFormatter.getRoundupParser();
        long millis = DateFormatters.from(roundUpFormatter.parse(input)).toInstant().toEpochMilli();
        assertThat(millis, is(expectedMilliSeconds));
    }

    public void testRoundupFormatterZone() {
        ZoneId zoneId = randomZone();
        String format = randomFrom(
            "epoch_second",
            "epoch_millis",
            "strict_date_optional_time",
            "uuuu-MM-dd'T'HH:mm:ss.SSS",
            "strict_date_optional_time||date_optional_time"
        );
        JavaDateFormatter formatter = (JavaDateFormatter) DateFormatter.forPattern(format).withZone(zoneId);
        JavaDateFormatter roundUpFormatter = formatter.getRoundupParser();
        assertThat(roundUpFormatter.zone(), is(zoneId));
        assertThat(formatter.zone(), is(zoneId));
    }

    public void testRoundupFormatterLocale() {
        Locale locale = randomLocale(random());
        String format = randomFrom(
            "epoch_second",
            "epoch_millis",
            "strict_date_optional_time",
            "uuuu-MM-dd'T'HH:mm:ss.SSS",
            "strict_date_optional_time||date_optional_time"
        );
        JavaDateFormatter formatter = (JavaDateFormatter) DateFormatter.forPattern(format).withLocale(locale);
        JavaDateFormatter roundupParser = formatter.getRoundupParser();
        assertThat(roundupParser.locale(), is(locale));
        assertThat(formatter.locale(), is(locale));
    }

    public void test0MillisAreFormatted() {
        DateFormatter formatter = DateFormatter.forPattern("strict_date_time");
        Clock clock = Clock.fixed(ZonedDateTime.of(2019, 02, 8, 11, 43, 00, 0, ZoneOffset.UTC).toInstant(), ZoneOffset.UTC);
        String formatted = formatter.formatMillis(clock.millis());
        assertThat(formatted, is("2019-02-08T11:43:00.000Z"));
    }

    public void testFractionalSeconds() {
        DateFormatter formatter = DateFormatters.forPattern("strict_date_optional_time");
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.1Z"));
            assertThat(instant.getNano(), is(100_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.12Z"));
            assertThat(instant.getNano(), is(120_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.123Z"));
            assertThat(instant.getNano(), is(123_000_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.1234Z"));
            assertThat(instant.getNano(), is(123_400_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.12345Z"));
            assertThat(instant.getNano(), is(123_450_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.123456Z"));
            assertThat(instant.getNano(), is(123_456_000));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.1234567Z"));
            assertThat(instant.getNano(), is(123_456_700));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.12345678Z"));
            assertThat(instant.getNano(), is(123_456_780));
        }
        {
            Instant instant = Instant.from(formatter.parse("2019-05-06T14:52:37.123456789Z"));
            assertThat(instant.getNano(), is(123_456_789));
        }
    }

    public void testWeek_yearDeprecation() {
        DateFormatter.forPattern("week_year");
        assertWarnings(
            "Format name \"week_year\" is deprecated and will be removed in a future version. " + "Use \"weekyear\" format instead"
        );
    }

    public void testTimezoneParsing() {
        /** this testcase won't work in joda. See comment in {@link #testPartialTimeParsing()}
         *  assertSameDateAs("2016-11-30T+01", "strict_date_optional_time", "strict_date_optional_time");
         */
        assertSameDateAs("2016-11-30T00+01", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T00+0100", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T00+01:00", "strict_date_optional_time");
    }

    public void testPartialTimeParsing() {
        /*
        This does not work in Joda as it reports 2016-11-30T01:00:00Z
        because strict_date_optional_time confuses +01 with an hour (which is a signed fixed length digit)
        assertSameDateAs("2016-11-30T+01", "strict_date_optional_time", "strict_date_optional_time");
        ES java.time implementation does not suffer from this,
        but we intentionally not allow parsing timezone without an time part as it is not allowed in iso8601
        */
        assertParseException("2016-11-30T+01", "strict_date_optional_time");

        assertSameDateAs("2016-11-30T12+01", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00+01", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00:00+01", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00:00.000+01", "strict_date_optional_time");

        // without timezone
        assertSameDateAs("2016-11-30T", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00:00", "strict_date_optional_time");
        assertSameDateAs("2016-11-30T12:00:00.000", "strict_date_optional_time");
    }

    // date_optional part of a parser names "strict_date_optional_time" or "date_optional"time
    // means that date part can be partially parsed.
    public void testPartialDateParsing() {
        assertSameDateAs("2001", "strict_date_optional_time_nanos");
        assertSameDateAs("2001-01", "strict_date_optional_time_nanos");
        assertSameDateAs("2001-01-01", "strict_date_optional_time_nanos");

        assertSameDateAs("2001", "strict_date_optional_time");
        assertSameDateAs("2001-01", "strict_date_optional_time");
        assertSameDateAs("2001-01-01", "strict_date_optional_time");

        assertSameDateAs("2001", "date_optional_time");
        assertSameDateAs("2001-01", "date_optional_time");
        assertSameDateAs("2001-01-01", "date_optional_time");

        assertSameDateAs("2001", "iso8601");
        assertSameDateAs("2001-01", "iso8601");
        assertSameDateAs("2001-01-01", "iso8601");

        assertSameDateAs("9999", "date_optional_time||epoch_second");
    }

    public void testCompositeDateMathParsing() {
        // in all these examples the second pattern will be used
        assertDateMathEquals("2014-06-06T12:01:02.123", "2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS");
        assertDateMathEquals("2014-06-06T12:01:02.123", "2014-06-06T12:01:02.123", "strict_date_time_no_millis||yyyy-MM-dd'T'HH:mm:ss.SSS");
        assertDateMathEquals(
            "2014-06-06T12:01:02.123",
            "2014-06-06T12:01:02.123",
            "yyyy-MM-dd'T'HH:mm:ss+HH:MM||yyyy-MM-dd'T'HH:mm:ss.SSS"
        );
    }

    public void testExceptionWhenCompositeParsingFailsDateMath() {
        // both parsing failures should contain pattern and input text in exception
        // both patterns fail parsing the input text due to only 2 digits of millis. Hence full text was not parsed.
        String pattern = "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SS";
        String text = "2014-06-06T12:01:02.123";
        OpenSearchParseException e1 = expectThrows(
            OpenSearchParseException.class,
            () -> dateMathToMillis(text, DateFormatter.forPattern(pattern))
        );
        assertThat(e1.getMessage(), containsString(pattern));
        assertThat(e1.getMessage(), containsString(text));
    }

    // these parsers should allow both ',' and '.' as a decimal point
    public void testDecimalPointParsing() {
        assertSameDateAs("2001-01-01T00:00:00.123Z", "strict_date_optional_time");
        assertSameDateAs("2001-01-01T00:00:00,123Z", "strict_date_optional_time");

        assertSameDateAs("2001-01-01T00:00:00.123Z", "date_optional_time");
        assertSameDateAs("2001-01-01T00:00:00,123Z", "date_optional_time");

        // only java.time has nanos parsing, but the results for 3digits should be the same
        DateFormatter javaFormatter = DateFormatter.forPattern("strict_date_optional_time_nanos");
        assertSameDate("2001-01-01T00:00:00.123Z", javaFormatter);
        assertSameDate("2001-01-01T00:00:00,123Z", javaFormatter);

        assertParseException("2001-01-01T00:00:00.123,456Z", "strict_date_optional_time");
        assertParseException("2001-01-01T00:00:00.123,456Z", "date_optional_time");
        // This should fail, but java is ok with this because the field has the same value
        // assertJavaTimeParseException("2001-01-01T00:00:00.123,123Z", "strict_date_optional_time_nanos");
    }

    public void testTimeZoneFormatting() {
        assertSameDateAs("2001-01-01T00:00:00Z", "date_time_no_millis");
        // the following fail under java 8 but work under java 10, needs investigation
        assertSameDateAs("2001-01-01T00:00:00-0800", "date_time_no_millis");
        assertSameDateAs("2001-01-01T00:00:00+1030", "date_time_no_millis");
        assertSameDateAs("2001-01-01T00:00:00-08", "date_time_no_millis");
        assertSameDateAs("2001-01-01T00:00:00+10:30", "date_time_no_millis");

        // different timezone parsing styles require a different number of letters
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSXXX", Locale.ROOT);
        formatter.parse("20181126T121212.123Z");
        formatter.parse("20181126T121212.123-08:30");

        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSXXXX", Locale.ROOT);
        formatter2.parse("20181126T121212.123+1030");
        formatter2.parse("20181126T121212.123-0830");

        // ... and can be combined, note that this is not an XOR, so one could append both timezones with this example
        DateTimeFormatter formatter3 = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS[XXXX][XXX]", Locale.ROOT);
        formatter3.parse("20181126T121212.123Z");
        formatter3.parse("20181126T121212.123-08:30");
        formatter3.parse("20181126T121212.123+1030");
        formatter3.parse("20181126T121212.123-0830");
    }

    public void testCustomTimeFormats() {
        assertSameDateAs("2010 12 06 11:05:15", "yyyy dd MM HH:mm:ss");
        assertSameDateAs("12/06", "dd/MM");
        assertSameDateAs("Nov 24 01:29:01 -0800", "MMM dd HH:mm:ss Z");
    }

    public void testFormatsValidParsing() {
        assertSameDateAs("1522332219", "epoch_second");
        assertSameDateAs("0", "epoch_second");
        assertSameDateAs("1", "epoch_second");
        assertSameDateAs("1522332219321", "epoch_millis");
        assertSameDateAs("0", "epoch_millis");
        assertSameDateAs("1", "epoch_millis");

        assertSameDateAs("20181126", "basic_date");
        assertSameDateAs("20181126T121212.123Z", "basic_date_time");
        assertSameDateAs("20181126T121212.123+10:00", "basic_date_time");
        assertSameDateAs("20181126T121212.123-0800", "basic_date_time");

        assertSameDateAs("20181126T121212Z", "basic_date_time_no_millis");
        assertSameDateAs("20181126T121212+01:00", "basic_date_time_no_millis");
        assertSameDateAs("20181126T121212+0100", "basic_date_time_no_millis");
        assertSameDateAs("2018363", "basic_ordinal_date");
        assertSameDateAs("2018363T121212.1Z", "basic_ordinal_date_time");
        assertSameDateAs("2018363T121212.123Z", "basic_ordinal_date_time");
        assertSameDateAs("2018363T121212.123456789Z", "basic_ordinal_date_time");
        assertSameDateAs("2018363T121212.123+0100", "basic_ordinal_date_time");
        assertSameDateAs("2018363T121212.123+01:00", "basic_ordinal_date_time");
        assertSameDateAs("2018363T121212Z", "basic_ordinal_date_time_no_millis");
        assertSameDateAs("2018363T121212+0100", "basic_ordinal_date_time_no_millis");
        assertSameDateAs("2018363T121212+01:00", "basic_ordinal_date_time_no_millis");
        assertSameDateAs("121212.1Z", "basic_time");
        assertSameDateAs("121212.123Z", "basic_time");
        assertSameDateAs("121212.123456789Z", "basic_time");
        assertSameDateAs("121212.1+0100", "basic_time");
        assertSameDateAs("121212.123+0100", "basic_time");
        assertSameDateAs("121212.123+01:00", "basic_time");
        assertSameDateAs("121212Z", "basic_time_no_millis");
        assertSameDateAs("121212+0100", "basic_time_no_millis");
        assertSameDateAs("121212+01:00", "basic_time_no_millis");
        assertSameDateAs("T121212.1Z", "basic_t_time");
        assertSameDateAs("T121212.123Z", "basic_t_time");
        assertSameDateAs("T121212.123456789Z", "basic_t_time");
        assertSameDateAs("T121212.1+0100", "basic_t_time");
        assertSameDateAs("T121212.123+0100", "basic_t_time");
        assertSameDateAs("T121212.123+01:00", "basic_t_time");
        assertSameDateAs("T121212Z", "basic_t_time_no_millis");
        assertSameDateAs("T121212+0100", "basic_t_time_no_millis");
        assertSameDateAs("T121212+01:00", "basic_t_time_no_millis");
        assertSameDateAs("2018W313", "basic_week_date");
        assertSameDateAs("1W313", "basic_week_date");
        assertSameDateAs("18W313", "basic_week_date");
        assertSameDateAs("2018W313T121212.1Z", "basic_week_date_time");
        assertSameDateAs("2018W313T121212.123Z", "basic_week_date_time");
        assertSameDateAs("2018W313T121212.123456789Z", "basic_week_date_time");
        assertSameDateAs("2018W313T121212.123+0100", "basic_week_date_time");
        assertSameDateAs("2018W313T121212.123+01:00", "basic_week_date_time");
        assertSameDateAs("2018W313T121212Z", "basic_week_date_time_no_millis");
        assertSameDateAs("2018W313T121212+0100", "basic_week_date_time_no_millis");
        assertSameDateAs("2018W313T121212+01:00", "basic_week_date_time_no_millis");

        assertSameDateAs("2018-12-31", "date");
        assertSameDateAs("18-5-6", "date");
        assertSameDateAs("10000-5-6", "date");

        assertSameDateAs("2018-12-31T12", "date_hour");
        assertSameDateAs("2018-12-31T8", "date_hour");

        assertSameDateAs("2018-12-31T12:12", "date_hour_minute");
        assertSameDateAs("2018-12-31T8:3", "date_hour_minute");

        assertSameDateAs("2018-12-31T12:12:12", "date_hour_minute_second");
        assertSameDateAs("2018-12-31T12:12:1", "date_hour_minute_second");

        assertSameDateAs("2018-12-31T12:12:12.1", "date_hour_minute_second_fraction");
        assertSameDateAs("2018-12-31T12:12:12.123", "date_hour_minute_second_fraction");
        assertSameDateAs("2018-12-31T12:12:12.123456789", "date_hour_minute_second_fraction");
        assertSameDateAs("2018-12-31T12:12:12.1", "date_hour_minute_second_millis");
        assertSameDateAs("2018-12-31T12:12:12.123", "date_hour_minute_second_millis");
        assertParseException("2018-12-31T12:12:12.123456789", "date_hour_minute_second_millis");
        assertSameDateAs("2018-12-31T12:12:12.1", "date_hour_minute_second_millis");
        assertSameDateAs("2018-12-31T12:12:12.1", "date_hour_minute_second_fraction");

        assertSameDateAs("2018-05", "date_optional_time");
        assertSameDateAs("2018-05-30", "date_optional_time");
        assertSameDateAs("2018-05-30T20", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.1", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.123", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.123456789", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.123Z", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.123456789Z", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.1+0100", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.123+0100", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.1+01:00", "date_optional_time");
        assertSameDateAs("2018-05-30T20:21:23.123+01:00", "date_optional_time");
        assertSameDateAs("2018-12-1", "date_optional_time");
        assertSameDateAs("2018-12-31T10:15:30", "date_optional_time");
        assertSameDateAs("2018-12-31T10:15:3", "date_optional_time");
        assertSameDateAs("2018-12-31T10:5:30", "date_optional_time");
        assertSameDateAs("2018-12-31T1:15:30", "date_optional_time");

        assertSameDateAs("2018-12-31T10:15:30.1Z", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.123Z", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.123456789Z", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.1+0100", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.123+0100", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.123+01:00", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.1+01:00", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.11Z", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.11+0100", "date_time");
        assertSameDateAs("2018-12-31T10:15:30.11+01:00", "date_time");
        assertSameDateAs("2018-12-31T10:15:3.1Z", "date_time");
        assertSameDateAs("2018-12-31T10:15:3.123Z", "date_time");
        assertSameDateAs("2018-12-31T10:15:3.123456789Z", "date_time");
        assertSameDateAs("2018-12-31T10:15:3.1+0100", "date_time");
        assertSameDateAs("2018-12-31T10:15:3.123+0100", "date_time");
        assertSameDateAs("2018-12-31T10:15:3.123+01:00", "date_time");
        assertSameDateAs("2018-12-31T10:15:3.1+01:00", "date_time");

        assertSameDateAs("2018-12-31T10:15:30Z", "date_time_no_millis");
        assertSameDateAs("2018-12-31T10:15:30+0100", "date_time_no_millis");
        assertSameDateAs("2018-12-31T10:15:30+01:00", "date_time_no_millis");
        assertSameDateAs("2018-12-31T10:5:30Z", "date_time_no_millis");
        assertSameDateAs("2018-12-31T10:5:30+0100", "date_time_no_millis");
        assertSameDateAs("2018-12-31T10:5:30+01:00", "date_time_no_millis");
        assertSameDateAs("2018-12-31T10:15:3Z", "date_time_no_millis");
        assertSameDateAs("2018-12-31T10:15:3+0100", "date_time_no_millis");
        assertSameDateAs("2018-12-31T10:15:3+01:00", "date_time_no_millis");
        assertSameDateAs("2018-12-31T1:15:30Z", "date_time_no_millis");
        assertSameDateAs("2018-12-31T1:15:30+0100", "date_time_no_millis");
        assertSameDateAs("2018-12-31T1:15:30+01:00", "date_time_no_millis");

        assertSameDateAs("12", "hour");
        assertSameDateAs("01", "hour");
        assertSameDateAs("1", "hour");

        assertSameDateAs("12:12", "hour_minute");
        assertSameDateAs("12:01", "hour_minute");
        assertSameDateAs("12:1", "hour_minute");

        assertSameDateAs("12:12:12", "hour_minute_second");
        assertSameDateAs("12:12:01", "hour_minute_second");
        assertSameDateAs("12:12:1", "hour_minute_second");

        assertSameDateAs("12:12:12.123", "hour_minute_second_fraction");
        assertSameDateAs("12:12:12.123456789", "hour_minute_second_fraction");
        assertSameDateAs("12:12:12.1", "hour_minute_second_fraction");
        assertParseException("12:12:12", "hour_minute_second_fraction");
        assertSameDateAs("12:12:12.123", "hour_minute_second_millis");
        assertParseException("12:12:12.123456789", "hour_minute_second_millis");
        assertSameDateAs("12:12:12.1", "hour_minute_second_millis");
        assertParseException("12:12:12", "hour_minute_second_millis");

        assertSameDateAs("2018-128", "ordinal_date");
        assertSameDateAs("2018-1", "ordinal_date");

        assertSameDateAs("2018-128T10:15:30.1Z", "ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.123Z", "ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.123456789Z", "ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.123+0100", "ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.123+01:00", "ordinal_date_time");
        assertSameDateAs("2018-1T10:15:30.1Z", "ordinal_date_time");
        assertSameDateAs("2018-1T10:15:30.123Z", "ordinal_date_time");
        assertSameDateAs("2018-1T10:15:30.123456789Z", "ordinal_date_time");
        assertSameDateAs("2018-1T10:15:30.123+0100", "ordinal_date_time");
        assertSameDateAs("2018-1T10:15:30.123+01:00", "ordinal_date_time");

        assertSameDateAs("2018-128T10:15:30Z", "ordinal_date_time_no_millis");
        assertSameDateAs("2018-128T10:15:30+0100", "ordinal_date_time_no_millis");
        assertSameDateAs("2018-128T10:15:30+01:00", "ordinal_date_time_no_millis");
        assertSameDateAs("2018-1T10:15:30Z", "ordinal_date_time_no_millis");
        assertSameDateAs("2018-1T10:15:30+0100", "ordinal_date_time_no_millis");
        assertSameDateAs("2018-1T10:15:30+01:00", "ordinal_date_time_no_millis");

        assertSameDateAs("10:15:30.1Z", "time");
        assertSameDateAs("10:15:30.123Z", "time");
        assertSameDateAs("10:15:30.123456789Z", "time");
        assertSameDateAs("10:15:30.123+0100", "time");
        assertSameDateAs("10:15:30.123+01:00", "time");
        assertSameDateAs("1:15:30.1Z", "time");
        assertSameDateAs("1:15:30.123Z", "time");
        assertSameDateAs("1:15:30.123+0100", "time");
        assertSameDateAs("1:15:30.123+01:00", "time");
        assertSameDateAs("10:1:30.1Z", "time");
        assertSameDateAs("10:1:30.123Z", "time");
        assertSameDateAs("10:1:30.123+0100", "time");
        assertSameDateAs("10:1:30.123+01:00", "time");
        assertSameDateAs("10:15:3.1Z", "time");
        assertSameDateAs("10:15:3.123Z", "time");
        assertSameDateAs("10:15:3.123+0100", "time");
        assertSameDateAs("10:15:3.123+01:00", "time");
        assertParseException("10:15:3.1", "time");
        assertParseException("10:15:3Z", "time");

        assertSameDateAs("10:15:30Z", "time_no_millis");
        assertSameDateAs("10:15:30+0100", "time_no_millis");
        assertSameDateAs("10:15:30+01:00", "time_no_millis");
        assertSameDateAs("01:15:30Z", "time_no_millis");
        assertSameDateAs("01:15:30+0100", "time_no_millis");
        assertSameDateAs("01:15:30+01:00", "time_no_millis");
        assertSameDateAs("1:15:30Z", "time_no_millis");
        assertSameDateAs("1:15:30+0100", "time_no_millis");
        assertSameDateAs("1:15:30+01:00", "time_no_millis");
        assertSameDateAs("10:5:30Z", "time_no_millis");
        assertSameDateAs("10:5:30+0100", "time_no_millis");
        assertSameDateAs("10:5:30+01:00", "time_no_millis");
        assertSameDateAs("10:15:3Z", "time_no_millis");
        assertSameDateAs("10:15:3+0100", "time_no_millis");
        assertSameDateAs("10:15:3+01:00", "time_no_millis");
        assertParseException("10:15:3", "time_no_millis");

        assertSameDateAs("T10:15:30.1Z", "t_time");
        assertSameDateAs("T10:15:30.123Z", "t_time");
        assertSameDateAs("T10:15:30.123456789Z", "t_time");
        assertSameDateAs("T10:15:30.1+0100", "t_time");
        assertSameDateAs("T10:15:30.123+0100", "t_time");
        assertSameDateAs("T10:15:30.123+01:00", "t_time");
        assertSameDateAs("T10:15:30.1+01:00", "t_time");
        assertSameDateAs("T1:15:30.123Z", "t_time");
        assertSameDateAs("T1:15:30.123+0100", "t_time");
        assertSameDateAs("T1:15:30.123+01:00", "t_time");
        assertSameDateAs("T10:1:30.123Z", "t_time");
        assertSameDateAs("T10:1:30.123+0100", "t_time");
        assertSameDateAs("T10:1:30.123+01:00", "t_time");
        assertSameDateAs("T10:15:3.123Z", "t_time");
        assertSameDateAs("T10:15:3.123+0100", "t_time");
        assertSameDateAs("T10:15:3.123+01:00", "t_time");
        assertParseException("T10:15:3.1", "t_time");
        assertParseException("T10:15:3Z", "t_time");

        assertSameDateAs("T10:15:30Z", "t_time_no_millis");
        assertSameDateAs("T10:15:30+0100", "t_time_no_millis");
        assertSameDateAs("T10:15:30+01:00", "t_time_no_millis");
        assertSameDateAs("T1:15:30Z", "t_time_no_millis");
        assertSameDateAs("T1:15:30+0100", "t_time_no_millis");
        assertSameDateAs("T1:15:30+01:00", "t_time_no_millis");
        assertSameDateAs("T10:1:30Z", "t_time_no_millis");
        assertSameDateAs("T10:1:30+0100", "t_time_no_millis");
        assertSameDateAs("T10:1:30+01:00", "t_time_no_millis");
        assertSameDateAs("T10:15:3Z", "t_time_no_millis");
        assertSameDateAs("T10:15:3+0100", "t_time_no_millis");
        assertSameDateAs("T10:15:3+01:00", "t_time_no_millis");
        assertParseException("T10:15:3", "t_time_no_millis");

        assertSameDateAs("2012-W48-6", "week_date");
        assertSameDateAs("2012-W01-6", "week_date");
        assertSameDateAs("2012-W1-6", "week_date");
        assertParseException("2012-W1-8", "week_date");

        assertSameDateAs("2012-W48-6T10:15:30.1Z", "week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.123Z", "week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.123456789Z", "week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.1+0100", "week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.123+0100", "week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.1+01:00", "week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.123+01:00", "week_date_time");
        assertSameDateAs("2012-W1-6T10:15:30.1Z", "week_date_time");
        assertSameDateAs("2012-W1-6T10:15:30.123Z", "week_date_time");
        assertSameDateAs("2012-W1-6T10:15:30.1+0100", "week_date_time");
        assertSameDateAs("2012-W1-6T10:15:30.123+0100", "week_date_time");
        assertSameDateAs("2012-W1-6T10:15:30.1+01:00", "week_date_time");
        assertSameDateAs("2012-W1-6T10:15:30.123+01:00", "week_date_time");

        assertSameDateAs("2012-W48-6T10:15:30Z", "week_date_time_no_millis");
        assertSameDateAs("2012-W48-6T10:15:30+0100", "week_date_time_no_millis");
        assertSameDateAs("2012-W48-6T10:15:30+01:00", "week_date_time_no_millis");
        assertSameDateAs("2012-W1-6T10:15:30Z", "week_date_time_no_millis");
        assertSameDateAs("2012-W1-6T10:15:30+0100", "week_date_time_no_millis");
        assertSameDateAs("2012-W1-6T10:15:30+01:00", "week_date_time_no_millis");

        assertSameDateAs("2012", "year");
        assertSameDateAs("1", "year");
        assertSameDateAs("-2000", "year");

        assertSameDateAs("2012-12", "year_month");
        assertSameDateAs("1-1", "year_month");

        assertSameDateAs("2012-12-31", "year_month_day");
        assertSameDateAs("1-12-31", "year_month_day");
        assertSameDateAs("2012-1-31", "year_month_day");
        assertSameDateAs("2012-12-1", "year_month_day");

        assertSameDateAs("2018", "weekyear");
        assertSameDateAs("1", "weekyear");
        assertSameDateAs("2017", "weekyear");

        assertSameDateAs("2018-W29", "weekyear_week");
        assertSameDateAs("2018-W1", "weekyear_week");

        assertSameDateAs("2012-W31-5", "weekyear_week_day");
        assertSameDateAs("2012-W1-1", "weekyear_week_day");
    }

    public void testCompositeParsing() {
        // in all these examples the second pattern will be used
        assertSameDateAs("2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SSS");
        assertSameDateAs("2014-06-06T12:01:02.123", "strict_date_time_no_millis||yyyy-MM-dd'T'HH:mm:ss.SSS");
        assertSameDateAs("2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss+HH:MM||yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    public void testStrictParsing() {
        assertSameDateAs("2018W313", "strict_basic_week_date");
        assertParseException("18W313", "strict_basic_week_date");
        assertSameDateAs("2018W313T121212.1Z", "strict_basic_week_date_time");
        assertSameDateAs("2018W313T121212.123Z", "strict_basic_week_date_time");
        assertSameDateAs("2018W313T121212.123456789Z", "strict_basic_week_date_time");
        assertSameDateAs("2018W313T121212.1+0100", "strict_basic_week_date_time");
        assertSameDateAs("2018W313T121212.123+0100", "strict_basic_week_date_time");
        assertSameDateAs("2018W313T121212.1+01:00", "strict_basic_week_date_time");
        assertSameDateAs("2018W313T121212.123+01:00", "strict_basic_week_date_time");
        assertParseException("2018W313T12128.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12128.123456789Z", "strict_basic_week_date_time");
        assertParseException("2018W313T81212.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12812.123Z", "strict_basic_week_date_time");
        assertParseException("2018W313T12812.1Z", "strict_basic_week_date_time");
        assertSameDateAs("2018W313T121212Z", "strict_basic_week_date_time_no_millis");
        assertSameDateAs("2018W313T121212+0100", "strict_basic_week_date_time_no_millis");
        assertSameDateAs("2018W313T121212+01:00", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12128Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12128+0100", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12128+01:00", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T81212Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T81212+0100", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T81212+01:00", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12812Z", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12812+0100", "strict_basic_week_date_time_no_millis");
        assertParseException("2018W313T12812+01:00", "strict_basic_week_date_time_no_millis");
        assertSameDateAs("2018-12-31", "strict_date");
        assertParseException("10000-12-31", "strict_date");
        assertParseException("2018-8-31", "strict_date");
        assertSameDateAs("2018-12-31T12", "strict_date_hour");
        assertParseException("2018-12-31T8", "strict_date_hour");
        assertSameDateAs("2018-12-31T12:12", "strict_date_hour_minute");
        assertParseException("2018-12-31T8:3", "strict_date_hour_minute");
        assertSameDateAs("2018-12-31T12:12:12", "strict_date_hour_minute_second");
        assertParseException("2018-12-31T12:12:1", "strict_date_hour_minute_second");
        assertSameDateAs("2018-12-31T12:12:12.1", "strict_date_hour_minute_second_fraction");
        assertSameDateAs("2018-12-31T12:12:12.123", "strict_date_hour_minute_second_fraction");
        assertSameDateAs("2018-12-31T12:12:12.123456789", "strict_date_hour_minute_second_fraction");
        assertSameDateAs("2018-12-31T12:12:12.123", "strict_date_hour_minute_second_millis");
        assertSameDateAs("2018-12-31T12:12:12.1", "strict_date_hour_minute_second_millis");
        assertSameDateAs("2018-12-31T12:12:12.1", "strict_date_hour_minute_second_fraction");
        assertParseException("2018-12-31T12:12:12", "strict_date_hour_minute_second_millis");
        assertParseException("2018-12-31T12:12:12", "strict_date_hour_minute_second_fraction");
        assertSameDateAs("2018-12-31", "strict_date_optional_time");
        assertParseException("2018-12-1", "strict_date_optional_time");
        assertParseException("2018-1-31", "strict_date_optional_time");
        assertParseException("10000-01-31", "strict_date_optional_time");
        assertSameDateAs("2010-01-05T02:00", "strict_date_optional_time");
        assertSameDateAs("2018-12-31T10:15:30", "strict_date_optional_time");
        assertSameDateAs("2018-12-31T10:15:30Z", "strict_date_optional_time");
        assertSameDateAs("2018-12-31T10:15:30+0100", "strict_date_optional_time");
        assertSameDateAs("2018-12-31T10:15:30+01:00", "strict_date_optional_time");
        assertParseException("2018-12-31T10:15:3", "strict_date_optional_time");
        assertParseException("2018-12-31T10:5:30", "strict_date_optional_time");
        assertParseException("2018-12-31T9:15:30", "strict_date_optional_time");
        assertSameDateAs("2015-01-04T00:00Z", "strict_date_optional_time");
        assertSameDateAs("2018-12-31T10:15:30.1Z", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.123Z", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.123456789Z", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.1+0100", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.123+0100", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.1+01:00", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.123+01:00", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.11Z", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.11+0100", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30.11+01:00", "strict_date_time");
        assertParseException("2018-12-31T10:15:3.123Z", "strict_date_time");
        assertParseException("2018-12-31T10:5:30.123Z", "strict_date_time");
        assertParseException("2018-12-31T1:15:30.123Z", "strict_date_time");
        assertSameDateAs("2018-12-31T10:15:30Z", "strict_date_time_no_millis");
        assertSameDateAs("2018-12-31T10:15:30+0100", "strict_date_time_no_millis");
        assertSameDateAs("2018-12-31T10:15:30+01:00", "strict_date_time_no_millis");
        assertParseException("2018-12-31T10:5:30Z", "strict_date_time_no_millis");
        assertParseException("2018-12-31T10:15:3Z", "strict_date_time_no_millis");
        assertParseException("2018-12-31T1:15:30Z", "strict_date_time_no_millis");
        assertSameDateAs("12", "strict_hour");
        assertSameDateAs("01", "strict_hour");
        assertParseException("1", "strict_hour");
        assertSameDateAs("12:12", "strict_hour_minute");
        assertSameDateAs("12:01", "strict_hour_minute");
        assertParseException("12:1", "strict_hour_minute");
        assertSameDateAs("12:12:12", "strict_hour_minute_second");
        assertSameDateAs("12:12:01", "strict_hour_minute_second");
        assertParseException("12:12:1", "strict_hour_minute_second");
        assertSameDateAs("12:12:12.123", "strict_hour_minute_second_fraction");
        assertSameDateAs("12:12:12.123456789", "strict_hour_minute_second_fraction");
        assertSameDateAs("12:12:12.1", "strict_hour_minute_second_fraction");
        assertParseException("12:12:12", "strict_hour_minute_second_fraction");
        assertSameDateAs("12:12:12.123", "strict_hour_minute_second_millis");
        assertSameDateAs("12:12:12.1", "strict_hour_minute_second_millis");
        assertParseException("12:12:12", "strict_hour_minute_second_millis");
        assertSameDateAs("2018-128", "strict_ordinal_date");
        assertParseException("2018-1", "strict_ordinal_date");

        assertSameDateAs("2018-128T10:15:30.1Z", "strict_ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.123Z", "strict_ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.123456789Z", "strict_ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.1+0100", "strict_ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.123+0100", "strict_ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.1+01:00", "strict_ordinal_date_time");
        assertSameDateAs("2018-128T10:15:30.123+01:00", "strict_ordinal_date_time");
        assertParseException("2018-1T10:15:30.123Z", "strict_ordinal_date_time");

        assertSameDateAs("2018-128T10:15:30Z", "strict_ordinal_date_time_no_millis");
        assertSameDateAs("2018-128T10:15:30+0100", "strict_ordinal_date_time_no_millis");
        assertSameDateAs("2018-128T10:15:30+01:00", "strict_ordinal_date_time_no_millis");
        assertParseException("2018-1T10:15:30Z", "strict_ordinal_date_time_no_millis");

        assertSameDateAs("10:15:30.1Z", "strict_time");
        assertSameDateAs("10:15:30.123Z", "strict_time");
        assertSameDateAs("10:15:30.123456789Z", "strict_time");
        assertSameDateAs("10:15:30.123+0100", "strict_time");
        assertSameDateAs("10:15:30.123+01:00", "strict_time");
        assertParseException("1:15:30.123Z", "strict_time");
        assertParseException("10:1:30.123Z", "strict_time");
        assertParseException("10:15:3.123Z", "strict_time");
        assertParseException("10:15:3.1", "strict_time");
        assertParseException("10:15:3Z", "strict_time");

        assertSameDateAs("10:15:30Z", "strict_time_no_millis");
        assertSameDateAs("10:15:30+0100", "strict_time_no_millis");
        assertSameDateAs("10:15:30+01:00", "strict_time_no_millis");
        assertSameDateAs("01:15:30Z", "strict_time_no_millis");
        assertSameDateAs("01:15:30+0100", "strict_time_no_millis");
        assertSameDateAs("01:15:30+01:00", "strict_time_no_millis");
        assertParseException("1:15:30Z", "strict_time_no_millis");
        assertParseException("10:5:30Z", "strict_time_no_millis");
        assertParseException("10:15:3Z", "strict_time_no_millis");
        assertParseException("10:15:3", "strict_time_no_millis");

        assertSameDateAs("T10:15:30.1Z", "strict_t_time");
        assertSameDateAs("T10:15:30.123Z", "strict_t_time");
        assertSameDateAs("T10:15:30.123456789Z", "strict_t_time");
        assertSameDateAs("T10:15:30.1+0100", "strict_t_time");
        assertSameDateAs("T10:15:30.123+0100", "strict_t_time");
        assertSameDateAs("T10:15:30.1+01:00", "strict_t_time");
        assertSameDateAs("T10:15:30.123+01:00", "strict_t_time");
        assertParseException("T1:15:30.123Z", "strict_t_time");
        assertParseException("T10:1:30.123Z", "strict_t_time");
        assertParseException("T10:15:3.123Z", "strict_t_time");
        assertParseException("T10:15:3.1", "strict_t_time");
        assertParseException("T10:15:3Z", "strict_t_time");

        assertSameDateAs("T10:15:30Z", "strict_t_time_no_millis");
        assertSameDateAs("T10:15:30+0100", "strict_t_time_no_millis");
        assertSameDateAs("T10:15:30+01:00", "strict_t_time_no_millis");
        assertParseException("T1:15:30Z", "strict_t_time_no_millis");
        assertParseException("T10:1:30Z", "strict_t_time_no_millis");
        assertParseException("T10:15:3Z", "strict_t_time_no_millis");
        assertParseException("T10:15:3", "strict_t_time_no_millis");

        assertSameDateAs("2012-W48-6", "strict_week_date");
        assertSameDateAs("2012-W01-6", "strict_week_date");
        assertParseException("2012-W1-6", "strict_week_date");
        assertParseException("2012-W1-8", "strict_week_date");

        assertSameDateAs("2012-W48-6", "strict_week_date");
        assertSameDateAs("2012-W01-6", "strict_week_date");
        assertParseException("2012-W1-6", "strict_week_date");
        assertParseException("2012-W01-8", "strict_week_date");

        assertSameDateAs("2012-W48-6T10:15:30.1Z", "strict_week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.123Z", "strict_week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.123456789Z", "strict_week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.1+0100", "strict_week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.123+0100", "strict_week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.1+01:00", "strict_week_date_time");
        assertSameDateAs("2012-W48-6T10:15:30.123+01:00", "strict_week_date_time");
        assertParseException("2012-W1-6T10:15:30.123Z", "strict_week_date_time");

        assertSameDateAs("2012-W48-6T10:15:30Z", "strict_week_date_time_no_millis");
        assertSameDateAs("2012-W48-6T10:15:30+0100", "strict_week_date_time_no_millis");
        assertSameDateAs("2012-W48-6T10:15:30+01:00", "strict_week_date_time_no_millis");
        assertParseException("2012-W1-6T10:15:30Z", "strict_week_date_time_no_millis");

        assertSameDateAs("2012", "strict_year");
        assertParseException("1", "strict_year");
        assertSameDateAs("-2000", "strict_year");

        assertSameDateAs("2012-12", "strict_year_month");
        assertParseException("1-1", "strict_year_month");

        assertSameDateAs("2012-12-31", "strict_year_month_day");
        assertParseException("1-12-31", "strict_year_month_day");
        assertParseException("2012-1-31", "strict_year_month_day");
        assertParseException("2012-12-1", "strict_year_month_day");

        assertSameDateAs("2018", "strict_weekyear");
        assertParseException("1", "strict_weekyear");

        assertSameDateAs("2018", "strict_weekyear");
        assertSameDateAs("2017", "strict_weekyear");
        assertParseException("1", "strict_weekyear");

        assertSameDateAs("2018-W29", "strict_weekyear_week");
        assertSameDateAs("2018-W01", "strict_weekyear_week");
        assertParseException("2018-W1", "strict_weekyear_week");

        assertSameDateAs("2012-W31-5", "strict_weekyear_week_day");
        assertParseException("2012-W1-1", "strict_weekyear_week_day");
    }

    public void testSeveralTimeFormats() {
        {
            String format = "year_month_day||ordinal_date";
            DateFormatter javaFormatter = DateFormatter.forPattern(format);
            assertSameDate("2018-12-12", javaFormatter);
            assertSameDate("2018-128", javaFormatter);
        }
        {
            String format = "strict_date_optional_time||dd-MM-yyyy";
            DateFormatter javaFormatter = DateFormatter.forPattern(format);
            assertSameDate("31-01-2014", javaFormatter);
        }
    }

    public void testParsingLocalDateFromYearOfEra() {
        // with strict resolving, YearOfEra expect an era, otherwise it won't resolve to a date
        assertSameDate("2018363", DateFormatter.forPattern("uuuuDDD"));
    }

    public void testDateFormatterWithLocale() {
        Locale locale = randomLocale(random());
        String pattern = randomBoolean() ? "strict_date_optional_time||date_time" : "date_time||strict_date_optional_time";
        DateFormatter formatter = DateFormatter.forPattern(pattern).withLocale(locale);
        assertThat(formatter.pattern(), is(pattern));
        assertThat(formatter.locale(), is(locale));
    }

    public void testParsingMissingTimezone() {
        long millisJava = DateFormatter.forPattern("8yyyy-MM-dd HH:mm:ss").parseMillis("2018-02-18 17:47:17");
        long millisJoda = DateFormatter.forPattern("yyyy-MM-dd HH:mm:ss").parseMillis("2018-02-18 17:47:17");
        assertThat(millisJava, is(millisJoda));
    }

    // the iso 8601 parser is available via Joda.forPattern(), so we have to test this slightly differently
    public void testIso8601Parsers() {
        String format = "iso8601";
        DateFormatter javaFormatter = DateFormatter.forPattern(format);

        assertSameDate("2018-10-10", javaFormatter);
        assertSameDate("2018-10-10T", javaFormatter);
        assertSameDate("2018-10-10T10", javaFormatter);
        assertSameDate("2018-10-10T10+0430", javaFormatter);
        assertSameDate("2018-10-10T10:11", javaFormatter);
        assertSameDate("2018-10-10T10:11-08:00", javaFormatter);
        assertSameDate("2018-10-10T10:11Z", javaFormatter);
        assertSameDate("2018-10-10T10:11:12", javaFormatter);
        assertSameDate("2018-10-10T10:11:12+0100", javaFormatter);
        assertSameDate("2018-10-10T10:11:12.123", javaFormatter);
        assertSameDate("2018-10-10T10:11:12.123Z", javaFormatter);
        assertSameDate("2018-10-10T10:11:12.123+0000", javaFormatter);
        assertSameDate("2018-10-10T10:11:12,123", javaFormatter);
        assertSameDate("2018-10-10T10:11:12,123Z", javaFormatter);
        assertSameDate("2018-10-10T10:11:12,123+05:30", javaFormatter);
    }

    public void testExceptionWhenCompositeParsingFails() {
        assertParseException("2014-06-06T12:01:02.123", "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss.SS");
    }

    private void assertSameDateAs(String input, String javaPattern) {
        DateFormatter javaFormatter = DateFormatter.forPattern(javaPattern);
        assertSameDate(input, javaFormatter);
    }

    private void assertSameDate(String input, DateFormatter javaFormatter) {
        TemporalAccessor javaTimeAccessor = javaFormatter.parse(input);
        ZonedDateTime zonedDateTime = DateFormatters.from(javaTimeAccessor);
        assertNotNull(zonedDateTime);
    }

    private void assertDateMathEquals(String text, String expected, String pattern) {
        Locale locale = randomLocale(random());
        assertDateMathEquals(text, expected, pattern, locale);
    }

    private void assertDateMathEquals(String text, String expected, String pattern, Locale locale) {
        long gotMillisJava = dateMathToMillis(text, DateFormatter.forPattern(pattern), locale);
        long expectedMillis = DateFormatters.from(DateFormatter.forPattern("strict_date_optional_time").withLocale(locale).parse(expected))
            .toInstant()
            .toEpochMilli();

        assertThat(gotMillisJava, equalTo(expectedMillis));
    }

    private long dateMathToMillis(final String text, final DateFormatter dateFormatter) {
        DateFormatter javaFormatter = dateFormatter.withLocale(randomLocale(random()));
        DateMathParser javaDateMath = javaFormatter.toDateMathParser();
        return javaDateMath.parse(text, () -> 0, true, null).toEpochMilli();
    }

    private long dateMathToMillis(final String text, final DateFormatter dateFormatter, final Locale locale) {
        DateFormatter javaFormatter = dateFormatter.withLocale(locale);
        DateMathParser javaDateMath = javaFormatter.toDateMathParser();
        return javaDateMath.parse(text, () -> 0, true, null).toEpochMilli();
    }

    private void assertParseException(String input, String format) {
        DateFormatter javaTimeFormatter = DateFormatter.forPattern(format);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> javaTimeFormatter.parse(input));
        assertThat(e.getMessage(), containsString(input));
        assertThat(e.getMessage(), containsString(format));
    }

}
