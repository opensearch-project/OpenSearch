/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi.schedule;

import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class ScheduleParserTests extends OpenSearchTestCase {

    public void testParseCronSchedule() throws IOException {
        String cronScheduleJsonStr = "{\"cron\":{\"expression\":\"* * * * *\",\"timezone\":\"PST8PDT\", \"schedule_delay\":1234}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(cronScheduleJsonStr));
        parser.nextToken();
        Schedule schedule = ScheduleParser.parse(parser);

        Assert.assertTrue(schedule instanceof CronSchedule);
        Assert.assertEquals("* * * * *", ((CronSchedule)schedule).getCronExpression());
        Assert.assertEquals(ZoneId.of("PST8PDT"), ((CronSchedule)schedule).getTimeZone());
    }

    public void testParseIntervalSchedule() throws IOException {
        String intervalScheduleJsonStr = "{\"interval\":{\"start_time\":1546329600000,\"period\":1,\"unit\":\"Minutes\"" +
                ", \"schedule_delay\":1234}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(intervalScheduleJsonStr));
        parser.nextToken();
        Schedule schedule = ScheduleParser.parse(parser);

        Assert.assertTrue(schedule instanceof IntervalSchedule);
        Assert.assertEquals(Instant.ofEpochMilli(1546329600000L).plusMillis(1234), ((IntervalSchedule)schedule).getStartTime());
        Assert.assertEquals(1, ((IntervalSchedule)schedule).getInterval());
        Assert.assertEquals(ChronoUnit.MINUTES, ((IntervalSchedule)schedule).getUnit());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testUnknownScheduleType() throws IOException {
        String scheduleJsonStr = "{\"unknown_type\":{\"field\":\"value\"}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(scheduleJsonStr));
        parser.nextToken();
        ScheduleParser.parse(parser);
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_unknownFieldInCronSchedule() throws IOException {
        String cronScheduleJsonStr = "{\"cron\":{\"expression\":\"* * * * *\",\"unknown_field\":\"value\"}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(cronScheduleJsonStr));
        parser.nextToken();
        ScheduleParser.parse(parser);
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_unknownFiledInIntervalSchedule() throws IOException {
        String intervalScheduleJsonStr = "{\"interval\":{\"start_time\":1546329600000,\"period\":1,\"unknown_filed\":\"value\"}}";

        XContentParser parser = this.createParser(XContentType.JSON.xContent(), new BytesArray(intervalScheduleJsonStr));
        parser.nextToken();
        ScheduleParser.parse(parser);
    }
}
