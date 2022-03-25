/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi.schedule;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

public class IntervalScheduleTests extends OpenSearchTestCase {

    private IntervalSchedule intervalSchedule;
    private IntervalSchedule intervalScheduleDelay;
    private Instant startTime;
    private final long DELAY = 15000;

    @Before
    public void setup() throws ParseException {
        startTime = new SimpleDateFormat("MM/dd/yyyy", Locale.ROOT).parse("01/01/2019").toInstant();
        this.intervalSchedule = new IntervalSchedule(startTime, 1, ChronoUnit.MINUTES);
        this.intervalScheduleDelay = new IntervalSchedule(startTime, 3, ChronoUnit.MINUTES, DELAY);
    }

    @Test (expected =  IllegalArgumentException.class)
    public void testConstructor_notSupportedTimeUnit() throws ParseException {
        Instant startTime = new SimpleDateFormat("MM/dd/yyyy").parse("01/01/2019").toInstant();
        new IntervalSchedule(startTime, 1, ChronoUnit.MILLIS);
    }

    public void testNextTimeToExecution() {
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);
        this.intervalScheduleDelay.setClock(testClock);

        Instant nextMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60 + 60);
        Duration expected = Duration.of(nextMinute.toEpochMilli() - now.toEpochMilli(), ChronoUnit.MILLIS);

        Instant nextIntervalPlusDelay = Instant.ofEpochSecond(((now.minusMillis(DELAY).getEpochSecond()) / 180 * 180) + 180).plusMillis(DELAY);
        Duration expectedDelay = Duration.of(nextIntervalPlusDelay.toEpochMilli() - now.toEpochMilli(), ChronoUnit.MILLIS);

        Assert.assertEquals(expected, this.intervalSchedule.nextTimeToExecute());
        Assert.assertEquals(expectedDelay, this.intervalScheduleDelay.nextTimeToExecute());

        Assert.assertEquals(this.intervalSchedule.nextTimeToExecute(),
                Duration.between(now, this.intervalSchedule.getNextExecutionTime(now)));
        Assert.assertEquals(this.intervalScheduleDelay.nextTimeToExecute(),
                Duration.between(now, this.intervalScheduleDelay.getNextExecutionTime(now)));
    }

    public void testGetPeriodStartingAt() {
        Instant now = Instant.now();
        Instant oneMinLater = now.plus(1L, ChronoUnit.MINUTES);

        Tuple<Instant, Instant> period = this.intervalSchedule.getPeriodStartingAt(now);

        Assert.assertEquals(now, period.v1());
        Assert.assertEquals(oneMinLater, period.v2());
    }

    public void testGetPeriodStartingAt_nullParam() {
        Instant now = Instant.now();
        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        Instant oneMinLater = now.plus(1L, ChronoUnit.MINUTES);
        Tuple<Instant, Instant> period = this.intervalSchedule.getPeriodStartingAt(null);

        Assert.assertEquals(now, period.v1());
        Assert.assertEquals(oneMinLater, period.v2());
    }

    public void testRunningOnTime() {
        Instant now = Instant.now();
        if(now.toEpochMilli() % (60 * 1000) == 0) {
            // test "now" is not execution time case
            now = now.plus(10, ChronoUnit.SECONDS);
        }
        Instant currentMinute = Instant.ofEpochSecond(now.getEpochSecond() / 60 * 60);

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        Instant lastExecutionTime = currentMinute.plus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.intervalSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.minus(2, ChronoUnit.SECONDS);
        Assert.assertFalse(this.intervalSchedule.runningOnTime(lastExecutionTime));

        // test "now" is execution time case
        now = currentMinute;
        testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        lastExecutionTime = currentMinute.minus(59500, ChronoUnit.MILLIS);
        Assert.assertTrue(this.intervalSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.minus(58500, ChronoUnit.MILLIS);
        Assert.assertFalse(this.intervalSchedule.runningOnTime(lastExecutionTime));

        // test "now" is in the first second, and last run in current second of current second
        now = currentMinute.plus(500, ChronoUnit.MILLIS);
        testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalSchedule.setClock(testClock);

        lastExecutionTime = currentMinute;
        Assert.assertTrue(this.intervalSchedule.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentMinute.minus(2, ChronoUnit.SECONDS);
        Assert.assertFalse(this.intervalSchedule.runningOnTime(lastExecutionTime));
    }

    public void testRunningOnTimeWithDelay() {
        Instant now = Instant.now();
        if(now.minusMillis(DELAY).toEpochMilli() % (180 * 1000) == 0) {
            // test "now" is not execution time case
            now = now.plus(10, ChronoUnit.SECONDS);
        }
        Instant currentThreeMinutePlusDelay = Instant.ofEpochSecond(now.getEpochSecond() / 180 * 180).plusMillis(DELAY);

        Clock testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalScheduleDelay.setClock(testClock);

        Instant lastExecutionTime = currentThreeMinutePlusDelay.plus(10, ChronoUnit.MILLIS);
        Assert.assertTrue(this.intervalScheduleDelay.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentThreeMinutePlusDelay.minus(2, ChronoUnit.SECONDS);
        Assert.assertFalse(this.intervalScheduleDelay.runningOnTime(lastExecutionTime));

        // test "now" is execution time case
        now = currentThreeMinutePlusDelay;
        testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalScheduleDelay.setClock(testClock);

        lastExecutionTime = currentThreeMinutePlusDelay.minus(179500, ChronoUnit.MILLIS);
        Assert.assertTrue(this.intervalScheduleDelay.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentThreeMinutePlusDelay.minus(178500, ChronoUnit.MILLIS);
        Assert.assertFalse(this.intervalScheduleDelay.runningOnTime(lastExecutionTime));

        // test "now" is in the first second, and last run in current second of current second
        now = currentThreeMinutePlusDelay.plus(500, ChronoUnit.MILLIS);
        testClock = Clock.fixed(now, ZoneId.systemDefault());
        this.intervalScheduleDelay.setClock(testClock);

        lastExecutionTime = currentThreeMinutePlusDelay;
        Assert.assertTrue(this.intervalScheduleDelay.runningOnTime(lastExecutionTime));

        lastExecutionTime = currentThreeMinutePlusDelay.minus(2, ChronoUnit.SECONDS);
        Assert.assertFalse(this.intervalScheduleDelay.runningOnTime(lastExecutionTime));
    }

    public void testRunningOnTime_nullLastExetime() {
        Assert.assertTrue(this.intervalSchedule.runningOnTime(null));
        Assert.assertTrue(this.intervalScheduleDelay.runningOnTime(null));
    }

    public void testToXContent() throws IOException {
        long epochMillis = this.startTime.toEpochMilli();
        String xContentJsonStr = "{\"interval\":{\"start_time\":" + epochMillis + ",\"period\":1,\"unit\":\"Minutes\"}}";
                XContentHelper.toXContent(this.intervalSchedule, XContentType.JSON, false)
                .utf8ToString();
        Assert.assertEquals(xContentJsonStr, XContentHelper.toXContent(this.intervalSchedule, XContentType.JSON, false)
                .utf8ToString());

        String xContentJsonStrDelay = "{\"interval\":{\"start_time\":" + epochMillis + ",\"period\":3,\"unit\":\"Minutes\",\"schedule_delay\":15000}}";
        XContentHelper.toXContent(this.intervalScheduleDelay, XContentType.JSON, false)
                .utf8ToString();
        Assert.assertEquals(xContentJsonStrDelay, XContentHelper.toXContent(this.intervalScheduleDelay, XContentType.JSON, false)
                .utf8ToString());
    }

    public void testIntervalScheduleEqualsAndHashCode() {
        Long epochMilli = Instant.now().toEpochMilli();
        IntervalSchedule intervalScheduleOne = new IntervalSchedule(Instant.ofEpochMilli(epochMilli), 5, ChronoUnit.MINUTES);
        IntervalSchedule intervalScheduleTwo = new IntervalSchedule(Instant.ofEpochMilli(epochMilli), 5, ChronoUnit.MINUTES);
        IntervalSchedule intervalScheduleThree = new IntervalSchedule(Instant.ofEpochMilli(epochMilli), 4, ChronoUnit.MINUTES);
        IntervalSchedule intervalScheduleFour = new IntervalSchedule(Instant.ofEpochMilli(epochMilli), 4, ChronoUnit.MINUTES, 5000);
        IntervalSchedule intervalScheduleFive = new IntervalSchedule(Instant.ofEpochMilli(epochMilli), 4, ChronoUnit.MINUTES, 5000);

        Assert.assertEquals("Identical interval schedules were not equal", intervalScheduleOne, intervalScheduleTwo);
        Assert.assertNotEquals("Different interval schedules were called equal", intervalScheduleOne, intervalScheduleThree);
        Assert.assertEquals("Identical interval schedules had different hash codes", intervalScheduleOne.hashCode(), intervalScheduleTwo.hashCode());
        Assert.assertNotEquals("Different interval schedules were called equal", intervalScheduleOne, intervalScheduleFour);
        Assert.assertEquals("Identical interval schedules were not equal", intervalScheduleFour, intervalScheduleFive);
        Assert.assertEquals("Identical interval schedules had different hash codes", intervalScheduleFour.hashCode(), intervalScheduleFive.hashCode());
    }

    public void testIntervalScheduleAsStream() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        intervalSchedule.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        IntervalSchedule newIntervalSchedule = new IntervalSchedule(input);
        assertEquals(intervalSchedule, newIntervalSchedule);

        BytesStreamOutput outDelay = new BytesStreamOutput();
        intervalScheduleDelay.writeTo(outDelay);
        StreamInput inputDelay = outDelay.bytes().streamInput();
        IntervalSchedule newIntervalScheduleDelay = new IntervalSchedule(inputDelay);
        assertEquals(intervalScheduleDelay, newIntervalScheduleDelay);
    }

}
