/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi.schedule;

import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * Schedule XContent parser.
 */
public class ScheduleParser {
    public static Schedule parse(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        while(!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case CronSchedule.CRON_FIELD:
                    String expression = null;
                    ZoneId timezone = null;
                    Long cronDelay = null;
                    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
                        String cronField = parser.currentName();
                        parser.nextToken();
                        switch (cronField) {
                            case CronSchedule.EXPRESSION_FIELD: expression = parser.text();
                                break;
                            case CronSchedule.TIMEZONE_FIELD: timezone = ZoneId.of(parser.text());
                                break;
                            case Schedule.DELAY_FIELD: cronDelay = parser.longValue();
                                break;
                            default:
                                throw new IllegalArgumentException(
                                        String.format(Locale.ROOT, "Unknown cron field %s", cronField));
                        }
                    }
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(),
                            parser);
                    parser.nextToken();
                    return cronDelay == null ? new CronSchedule(expression, timezone) : new CronSchedule(expression, timezone, cronDelay);
                case IntervalSchedule.INTERVAL_FIELD:
                    Instant startTime = null;
                    int period = 0;
                    ChronoUnit unit = null;
                    Long intervalDelay = null;
                    while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
                        String intervalField = parser.currentName();
                        parser.nextToken();
                        switch (intervalField) {
                            case IntervalSchedule.START_TIME_FIELD:
                                startTime = Instant.ofEpochMilli(parser.longValue());
                                break;
                            case IntervalSchedule.PERIOD_FIELD:
                                period = parser.intValue();
                                break;
                            case IntervalSchedule.UNIT_FIELD:
                                unit = ChronoUnit.valueOf(parser.text().toUpperCase(Locale.ROOT));
                                break;
                            case Schedule.DELAY_FIELD:
                                intervalDelay = parser.longValue();
                                break;
                            default:
                                throw new IllegalArgumentException(
                                        String.format(Locale.ROOT, "Unknown interval field %s", intervalField));
                        }
                    }
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(),
                            parser);
                    parser.nextToken();
                    return intervalDelay == null ? new IntervalSchedule(startTime, period, unit) : new IntervalSchedule(startTime, period, unit, intervalDelay);
                default:
                    throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "Unknown schedule type %s", fieldName));
            }
        }
        throw new IllegalArgumentException("Invalid schedule document object.");
    }
}
