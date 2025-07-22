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
import org.opensearch.core.common.Strings;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalQueries;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * A parser for date/time formatted text with optional date math.
 * <p>
 * The format of the datetime is configurable, and unix timestamps can also be used. Datemath
 * is appended to a datetime with the following syntax:
 * <code>||[+-/](\d+)?[yMwdhHms]</code>.
 *
 * @opensearch.internal
 */
public class JavaDateMathParser implements DateMathParser {

    private final JavaDateFormatter formatter;
    private final String format;
    private final JavaDateFormatter roundupParser;

    JavaDateMathParser(String format, JavaDateFormatter formatter, JavaDateFormatter roundupParser) {
        this.format = format;
        this.roundupParser = roundupParser;
        Objects.requireNonNull(formatter);
        this.formatter = formatter;
    }

    @Override
    public Instant parse(String text, LongSupplier now, boolean roundUpProperty, ZoneId timeZone) {
        Instant time;
        String mathString;
        if (text.startsWith("now")) {
            try {
                // TODO only millisecond granularity here!
                time = Instant.ofEpochMilli(now.getAsLong());
            } catch (Exception e) {
                throw new OpenSearchParseException("could not read the current timestamp", e);
            }
            mathString = text.substring("now".length());
        } else {
            int index = text.indexOf("||");
            if (index == -1) {
                return parseDateTime(text, timeZone, roundUpProperty);
            }
            time = parseDateTime(text.substring(0, index), timeZone, false);
            mathString = text.substring(index + 2);
        }

        return parseMath(mathString, time, roundUpProperty, timeZone);
    }

    private Instant parseMath(final String mathString, final Instant time, final boolean roundUpProperty, ZoneId timeZone)
        throws OpenSearchParseException {
        if (timeZone == null) {
            timeZone = ZoneOffset.UTC;
        }
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(time, timeZone);
        for (int i = 0; i < mathString.length();) {
            char c = mathString.charAt(i++);
            final boolean round;
            final int sign;
            if (c == '/') {
                round = true;
                sign = 1;
            } else {
                round = false;
                if (c == '+') {
                    sign = 1;
                } else if (c == '-') {
                    sign = -1;
                } else {
                    throw new OpenSearchParseException("operator not supported for date math [{}]", mathString);
                }
            }

            if (i >= mathString.length()) {
                throw new OpenSearchParseException("truncated date math [{}]", mathString);
            }

            final int num;
            if (!Character.isDigit(mathString.charAt(i))) {
                num = 1;
            } else {
                int numFrom = i;
                while (i < mathString.length() && Character.isDigit(mathString.charAt(i))) {
                    i++;
                }
                if (i >= mathString.length()) {
                    throw new OpenSearchParseException("truncated date math [{}]", mathString);
                }
                num = Integer.parseInt(mathString.substring(numFrom, i));
            }
            if (round) {
                if (num != 1) {
                    throw new OpenSearchParseException("rounding `/` can only be used on single unit types [{}]", mathString);
                }
            }
            char unit = mathString.charAt(i++);
            switch (unit) {
                case 'y':
                    if (round) {
                        dateTime = dateTime.withDayOfYear(1).with(LocalTime.MIN);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusYears(1);
                        }
                    } else {
                        dateTime = dateTime.plusYears(sign * num);
                    }
                    break;
                case 'M':
                    if (round) {
                        dateTime = dateTime.withDayOfMonth(1).with(LocalTime.MIN);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusMonths(1);
                        }
                    } else {
                        dateTime = dateTime.plusMonths(sign * num);
                    }
                    break;
                case 'w':
                    if (round) {
                        dateTime = dateTime.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).with(LocalTime.MIN);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusWeeks(1);
                        }
                    } else {
                        dateTime = dateTime.plusWeeks(sign * num);
                    }
                    break;
                case 'd':
                    if (round) {
                        dateTime = dateTime.with(LocalTime.MIN);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusDays(1);
                        }
                    } else {
                        dateTime = dateTime.plusDays(sign * num);
                    }
                    break;
                case 'h':
                case 'H':
                    if (round) {
                        dateTime = dateTime.withMinute(0).withSecond(0).withNano(0);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusHours(1);
                        }
                    } else {
                        dateTime = dateTime.plusHours(sign * num);
                    }
                    break;
                case 'm':
                    if (round) {
                        dateTime = dateTime.withSecond(0).withNano(0);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusMinutes(1);
                        }
                    } else {
                        dateTime = dateTime.plusMinutes(sign * num);
                    }
                    break;
                case 's':
                    if (round) {
                        dateTime = dateTime.withNano(0);
                        if (roundUpProperty) {
                            dateTime = dateTime.plusSeconds(1);
                        }
                    } else {
                        dateTime = dateTime.plusSeconds(sign * num);
                    }
                    break;
                default:
                    throw new OpenSearchParseException("unit [{}] not supported for date math [{}]", unit, mathString);
            }
            if (round && roundUpProperty) {
                // subtract 1 millisecond to get the largest inclusive value
                dateTime = dateTime.minus(1, ChronoField.MILLI_OF_SECOND.getBaseUnit());
            }
        }
        return dateTime.toInstant();
    }

    private Instant parseDateTime(String value, ZoneId timeZone, boolean roundUpIfNoTime) {
        if (Strings.isNullOrEmpty(value)) {
            throw new OpenSearchParseException("cannot parse empty date");
        }

        DateFormatter formatter = roundUpIfNoTime ? this.roundupParser : this.formatter;
        try {
            if (timeZone == null) {
                return DateFormatters.from(formatter.parse(value)).toInstant();
            } else {
                TemporalAccessor accessor = formatter.parse(value);
                ZoneId zoneId = TemporalQueries.zone().queryFrom(accessor);
                if (zoneId != null) {
                    timeZone = zoneId;
                }

                return DateFormatters.from(accessor).withZoneSameLocal(timeZone).toInstant();
            }
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw new OpenSearchParseException("failed to parse date field [{}] with format [{}]: [{}]", e, value, format, e.getMessage());
        }
    }
}
