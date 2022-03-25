/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi.schedule;

import com.cronutils.utils.VisibleForTesting;
import org.opensearch.common.Strings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * {@link Schedule} defined by interval (interval value &amp; interval unit). Currently the finest unit supported is minute.
 */
public class IntervalSchedule implements Schedule {

    static final String START_TIME_FIELD = "start_time";
    static final String INTERVAL_FIELD = "interval";
    static final String PERIOD_FIELD = "period";
    static final String UNIT_FIELD = "unit";

    private static final Set<ChronoUnit> SUPPORTED_UNITS;

    static {
        HashSet<ChronoUnit> set = new HashSet<>();
        set.add(ChronoUnit.MINUTES);
        set.add(ChronoUnit.HOURS);
        set.add(ChronoUnit.DAYS);
        SUPPORTED_UNITS = Collections.unmodifiableSet(set);
    }

    private Instant initialStartTime;
    private Instant startTimeWithDelay;
    private int interval;
    private ChronoUnit unit;
    private transient long intervalInMillis;
    private Clock clock;
    private Long scheduleDelay;

    public IntervalSchedule(Instant startTime, int interval, ChronoUnit unit) {
        if (!SUPPORTED_UNITS.contains(unit)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Interval unit %s is not supported, expects %s",
                            unit, SUPPORTED_UNITS));
        }
        this.initialStartTime = startTime;
        this.startTimeWithDelay = startTime;
        this.interval = interval;
        this.unit = unit;
        this.intervalInMillis = Duration.of(interval, this.unit).toMillis();
        this.clock = Clock.system(ZoneId.systemDefault());
    }

    public IntervalSchedule(Instant startTime, int interval, ChronoUnit unit, long scheduleDelay) {
        this(startTime, interval, unit);
        this.startTimeWithDelay = startTime.plusMillis(scheduleDelay);
        this.scheduleDelay = scheduleDelay;
    }

    public IntervalSchedule(StreamInput input) throws IOException {
        initialStartTime = input.readInstant();
        interval = input.readInt();
        unit = input.readEnum(ChronoUnit.class);
        scheduleDelay = input.readOptionalLong();
        startTimeWithDelay = scheduleDelay == null ? initialStartTime : initialStartTime.plusMillis(scheduleDelay);
        intervalInMillis = Duration.of(interval, unit).toMillis();
        clock = Clock.system(ZoneId.systemDefault());
    }

    public Instant getStartTime() {
        return this.startTimeWithDelay;
    }

    public int getInterval() {
        return this.interval;
    }

    public ChronoUnit getUnit() {
        return this.unit;
    }

    public Long getDelay() { return this.scheduleDelay; }

    @Override
    public Instant getNextExecutionTime(Instant time) {
        Instant baseTime = time == null ? this.clock.instant() : time;
        long delta = (baseTime.toEpochMilli() - this.startTimeWithDelay.toEpochMilli());
        if (delta >= 0) {
            long remaining = this.intervalInMillis - (delta % this.intervalInMillis);
            return baseTime.plus(remaining, ChronoUnit.MILLIS);
        } else {
            return this.startTimeWithDelay;
        }
    }

    @Override
    public Duration nextTimeToExecute() {
        long enabledTimeEpochMillis = this.startTimeWithDelay.toEpochMilli();
        Instant currentTime = this.clock.instant();
        long delta = currentTime.toEpochMilli() - enabledTimeEpochMillis;
        if (delta >= 0) {
            long remainingScheduleTime = intervalInMillis - (delta % intervalInMillis);
            return Duration.of(remainingScheduleTime, ChronoUnit.MILLIS);
        } else {
            return Duration.ofMillis(enabledTimeEpochMillis - currentTime.toEpochMilli());
        }
    }

    @Override
    public Tuple<Instant, Instant> getPeriodStartingAt(Instant startTime) {
        Instant realStartTime = startTime == null ? this.clock.instant() : startTime;
        Instant newEndTime = realStartTime.plusMillis(this.intervalInMillis);
        return new Tuple<>(realStartTime, newEndTime);
    }

    @SuppressForbidden(reason = "Ignore forbidden api Math.abs()")
    @Override
    public Boolean runningOnTime(Instant lastExecutionTime) {
        if (lastExecutionTime == null) {
            return true;
        }

        long enabledTimeEpochMillis = this.startTimeWithDelay.toEpochMilli();
        Instant now = this.clock.instant();
        long expectedMillisSinceLastExecution = (now.toEpochMilli() - enabledTimeEpochMillis) % this.intervalInMillis;
        if (expectedMillisSinceLastExecution < 1000) {
            expectedMillisSinceLastExecution = this.intervalInMillis + expectedMillisSinceLastExecution;
        }
        long expectedLastExecutionTime = now.toEpochMilli() - expectedMillisSinceLastExecution;
        long expectedCurrentExecutionTime = expectedLastExecutionTime + this.intervalInMillis;
        return Math.abs(lastExecutionTime.toEpochMilli() - expectedLastExecutionTime) < 1000
                || Math.abs(lastExecutionTime.toEpochMilli() - expectedCurrentExecutionTime) < 1000;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return this.scheduleDelay == null ? toXContentNoDelay(builder) : toXContentWithDelay(builder);
    }

    private XContentBuilder toXContentNoDelay(XContentBuilder builder) throws IOException {
        builder.startObject()
                .startObject(INTERVAL_FIELD)
                .field(START_TIME_FIELD, this.initialStartTime.toEpochMilli())
                .field(PERIOD_FIELD, this.interval)
                .field(UNIT_FIELD, this.unit)
                .endObject()
                .endObject();
        return builder;
    }

    private XContentBuilder toXContentWithDelay(XContentBuilder builder) throws IOException {
        builder.startObject()
                .startObject(INTERVAL_FIELD)
                .field(START_TIME_FIELD, this.initialStartTime.toEpochMilli())
                .field(PERIOD_FIELD, this.interval)
                .field(UNIT_FIELD, this.unit)
                .field(DELAY_FIELD, this.scheduleDelay)
                .endObject()
                .endObject();
        return builder;
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String toString() {
        return Strings.toString(this, false, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntervalSchedule intervalSchedule = (IntervalSchedule) o;
        return initialStartTime.equals(intervalSchedule.initialStartTime) &&
                interval == intervalSchedule.interval &&
                unit == intervalSchedule.unit &&
                intervalInMillis == intervalSchedule.intervalInMillis &&
                Objects.equals(scheduleDelay, intervalSchedule.scheduleDelay);
    }

    @Override
    public int hashCode() {
        return scheduleDelay == null ? Objects.hash(initialStartTime, interval, unit, intervalInMillis) :
                Objects.hash(initialStartTime, interval, unit, intervalInMillis, scheduleDelay);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInstant(initialStartTime);
        out.writeInt(interval);
        out.writeEnum(unit);
        out.writeOptionalLong(scheduleDelay);
    }
}
