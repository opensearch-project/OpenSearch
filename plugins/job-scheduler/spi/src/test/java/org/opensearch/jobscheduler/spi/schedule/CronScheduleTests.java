/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi.schedule;

import com.cronutils.model.time.ExecutionTime;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public class CronScheduleTests extends OpenSearchTestCase {

    private CronSchedule cronSchedule;
    private CronSchedule cronScheduleDelay;
    private final long DELAY = 15000;

    @Before
    public void setup() {
        this.cronSchedule = new CronSchedule("* * * * *", ZoneId.systemDefault());
        this.cronScheduleDelay = new CronSchedule("* * * * *", ZoneId.systemDefault(), DELAY);
    }

    public void testDifferentClocks() {
        Instant now = Instant.now();
        Clock pdtClock = Clock.fixed(now, ZoneId.of("America/Los_Angeles"));
        Clock utcClock = Clock.fixed(now, ZoneId.of("UTC"));
        CronSchedule pdtClockCronSchedule = new CronSchedule("* * * * *", ZoneId.of("America/Los_Angeles"));
        pdtClockCronSchedule.setClock(pdtClock);
        CronSchedule utcClockCronSchedule = new CronSchedule("* * * * *", ZoneId.of("UTC"));
        utcClockCronSchedule.setClock(utcClock);
        assertEquals("Next execution time based on different clock should be same.",
            pdtClockCronSchedule.getNextExecutionTime(null),
            utcClockCronSchedule.getNextExecutionTime(null));
    }

    public void testNextTimeToExecute() {
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);
        this.cronScheduleDelay.setClock(testClock);

        Instant nextMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60 + 60);
        Instant nextMinuteDelay = Instant.ofEpochSecond((now.minusMillis(DELAY)).getEpochSecond() / 60 * 60 + 60).plusMillis(DELAY);

        Duration expected = Duration.between(now, nextMinute);
        Duration duration = this.cronSchedule.nextTimeToExecute();
        Duration expectedDelay = Duration.between(now, nextMinuteDelay);
        Duration durationDelay = this.cronScheduleDelay.nextTimeToExecute();

        Assert.assertEquals(expected, duration);
        Assert.assertEquals(expectedDelay, durationDelay);

        Assert.assertEquals(this.cronSchedule.nextTimeToExecute(),
                Duration.between(now, this.cronSchedule.getNextExecutionTime(now)));
        Assert.assertEquals(this.cronScheduleDelay.nextTimeToExecute(),
                Duration.between(now, this.cronScheduleDelay.getNextExecutionTime(now)));
    }

    public void testGetPeriodStartingAt() {
        Instant now = Instant.now();
        Instant currentMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);
        Instant nextMinute = currentMinute.plus(1L, ChronoUnit.MINUTES);

        Tuple<Instant, Instant> period = this.cronSchedule.getPeriodStartingAt(currentMinute);

        Assert.assertEquals(currentMinute, period.v1());
        Assert.assertEquals(nextMinute, period.v2());
    }

    public void testGetPeriodStartingAtWithDelay() {
        Instant now = Instant.now();
        // If it is 10:01:07 with a delay of 15 seconds, we want the current minute to be 10:00:15 and next to be 10:01:15
        Instant currentMinute = Instant.ofEpochSecond(now.minusMillis(DELAY).getEpochSecond() / 60 * 60).plusMillis(DELAY);
        Instant nextMinute = currentMinute.plus(1L, ChronoUnit.MINUTES);

        Tuple<Instant, Instant> period = this.cronScheduleDelay.getPeriodStartingAt(currentMinute);

        Assert.assertEquals(currentMinute, period.v1());
        Assert.assertEquals(nextMinute, period.v2());
    }

    public void testGetPeriodStartingAt_nullStartTime() {
        Instant now = Instant.now();

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);

        Instant currentMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);
        Instant nextMinute = currentMinute.plus(1L, ChronoUnit.MINUTES);

        Tuple<Instant, Instant> period = this.cronSchedule.getPeriodStartingAt(null);

        Assert.assertEquals(currentMinute, period.v1());
        Assert.assertEquals(nextMinute, period.v2());
    }

    public void testGetPeriodStartingAtWithDelay_nullStartTime() {
        Instant now = Instant.now();

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronScheduleDelay.setClock(testClock);

        Instant currentMinute = Instant.ofEpochSecond(now.minusMillis(DELAY).getEpochSecond() / 60 * 60).plusMillis(DELAY);
        Instant nextMinute = currentMinute.plus(1L, ChronoUnit.MINUTES);

        Tuple<Instant, Instant> period = this.cronScheduleDelay.getPeriodStartingAt(null);

        Assert.assertEquals(currentMinute, period.v1());
        Assert.assertEquals(nextMinute, period.v2());
    }

    public void testGetPeriodStartingAt_noLastExecution() {
        ExecutionTime mockExecution = Mockito.mock(ExecutionTime.class);
        this.cronSchedule.setExecutionTime(mockExecution);
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);

        Mockito.when(mockExecution.lastExecution(ZonedDateTime.ofInstant(now, ZoneId.systemDefault()))).thenReturn(Optional.empty());

        Tuple<Instant, Instant> period = this.cronSchedule.getPeriodStartingAt(null);

        Assert.assertEquals(now, period.v1());
        Assert.assertEquals(now, period.v2());
        Mockito.verify(mockExecution).lastExecution(ZonedDateTime.ofInstant(now, ZoneId.systemDefault()));
    }

    public void testRunningOnTime() {
        Instant now = Instant.now();

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);

        Instant currentMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);

        Instant lastExecutionTime = currentMinute.minus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.cronSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.plus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.cronSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.plus(10, ChronoUnit.SECONDS);
        Assert.assertFalse(this.cronSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.minus(10, ChronoUnit.SECONDS);
        Assert.assertFalse(this.cronSchedule.runningOnTime(lastExecutionTime));
    }

    public void testRunningOnTimeWithDelay() {
        Instant now = Instant.now();

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronScheduleDelay.setClock(testClock);

        // Subtract by delay first in case that goes to the previous minute/iteration
        Instant currentMinutePlusDelay = Instant.ofEpochSecond((now.minusMillis(DELAY)).getEpochSecond() / 60 * 60).plusMillis(DELAY);

        Instant lastExecutionTime = currentMinutePlusDelay.minus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.cronScheduleDelay.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinutePlusDelay.plus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.cronScheduleDelay.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinutePlusDelay.plus(10, ChronoUnit.SECONDS);
        Assert.assertFalse(this.cronScheduleDelay.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinutePlusDelay.minus(10, ChronoUnit.SECONDS);
        Assert.assertFalse(this.cronScheduleDelay.runningOnTime(lastExecutionTime));
    }

    public void testRunningOnTime_nullParam() {
        Assert.assertTrue(this.cronSchedule.runningOnTime(null));
        Assert.assertTrue(this.cronScheduleDelay.runningOnTime(null));
    }

    public void testRunningOnTime_noLastExecution() {
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.cronSchedule.setClock(testClock);
        this.cronScheduleDelay.setClock(testClock);
        ExecutionTime mockExecutionTime = Mockito.mock(ExecutionTime.class);
        this.cronSchedule.setExecutionTime(mockExecutionTime);
        this.cronScheduleDelay.setExecutionTime(mockExecutionTime);

        Mockito.when(mockExecutionTime.lastExecution(ZonedDateTime.ofInstant(now, ZoneId.systemDefault())))
                .thenReturn(Optional.empty());
        Mockito.when(mockExecutionTime.lastExecution(ZonedDateTime.ofInstant(now.minusMillis(DELAY), ZoneId.systemDefault())))
                .thenReturn(Optional.empty());

        Assert.assertFalse(this.cronSchedule.runningOnTime(now));
        Assert.assertFalse(this.cronScheduleDelay.runningOnTime(now));
    }

    public void testToXContent() throws IOException {
        CronSchedule schedule = new CronSchedule("* * * * *", ZoneId.of("PST8PDT"));
        String expectedJsonStr = "{\"cron\":{\"expression\":\"* * * * *\",\"timezone\":\"PST8PDT\"}}";
        Assert.assertEquals(expectedJsonStr,
                XContentHelper.toXContent(schedule, XContentType.JSON, false).utf8ToString());

        CronSchedule scheduleDelay = new CronSchedule("* * * * *", ZoneId.of("PST8PDT"), 1234);
        String expectedJsonStrDelay = "{\"cron\":{\"expression\":\"* * * * *\",\"timezone\":\"PST8PDT\",\"schedule_delay\":1234}}";
        Assert.assertEquals(expectedJsonStrDelay,
                XContentHelper.toXContent(scheduleDelay, XContentType.JSON, false).utf8ToString());
    }

    public void testCronScheduleEqualsAndHashCode() {
        CronSchedule cronScheduleOne = new CronSchedule("* * * * *", ZoneId.of("PST8PDT"));
        CronSchedule cronScheduleTwo = new CronSchedule("* * * * *", ZoneId.of("PST8PDT"));
        CronSchedule cronScheduleThree = new CronSchedule("1 * * * *", ZoneId.of("PST8PDT"));
        CronSchedule cronScheduleFour = new CronSchedule("1 * * * *", ZoneId.of("PST8PDT"), DELAY);
        CronSchedule cronScheduleFive = new CronSchedule("1 * * * *", ZoneId.of("PST8PDT"), DELAY);

        Assert.assertEquals("Identical cron schedules were not equal", cronScheduleOne, cronScheduleTwo);
        Assert.assertNotEquals("Different cron schedules were called equal", cronScheduleOne, cronScheduleThree);
        Assert.assertEquals("Identical cron schedules had different hash codes", cronScheduleOne.hashCode(), cronScheduleTwo.hashCode());
        Assert.assertNotEquals("Different cron schedules were called equal", cronScheduleThree, cronScheduleFour);
        Assert.assertNotEquals("Different cron schedules had the same hash code", cronScheduleThree.hashCode(), cronScheduleFour.hashCode());
        Assert.assertEquals("Identical cron schedules were not equal", cronScheduleFour, cronScheduleFive);
        Assert.assertEquals("Identical cron schedules had different hash codes", cronScheduleFour.hashCode(), cronScheduleFive.hashCode());
    }

    public void testCronScheduleAsStream() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        cronSchedule.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        CronSchedule newCronSchedule = new CronSchedule(input);
        assertEquals(cronSchedule, newCronSchedule);
    }

    public void testCronScheduleAsStreamDelay() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        CronSchedule schedule = new CronSchedule("* * * * *", ZoneId.of("PST8PDT"), DELAY);
        schedule.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        CronSchedule newCronSchedule = new CronSchedule(input);
        assertEquals(schedule, newCronSchedule);
    }
}
