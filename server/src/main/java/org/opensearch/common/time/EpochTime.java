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

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.time.temporal.ValueRange;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * This class provides {@link DateTimeFormatter}s capable of parsing epoch seconds, milliseconds, and microseconds.
 * <p>
 * The seconds formatter is provided by {@link #SECONDS_FORMATTER}.
 * The milliseconds formatter is provided by {@link #MILLIS_FORMATTER}.
 * The microseconds formatter is provided by {@link #MICROS_FORMATTER}.
 * <p>
 * All formatters support fractional time, up to nanosecond precision.
 *
 * @opensearch.internal
 */
class EpochTime {

    private static final ValueRange LONG_POSITIVE_RANGE = ValueRange.of(0, Long.MAX_VALUE);
    private static final ValueRange LONG_RANGE = ValueRange.of(Long.MIN_VALUE, Long.MAX_VALUE);

    private static final EpochField SECONDS = new EpochField(ChronoUnit.SECONDS, ChronoUnit.FOREVER, LONG_RANGE) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS);
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            return temporal.getLong(ChronoField.INSTANT_SECONDS);
        }

        @Override
        public TemporalAccessor resolve(
            Map<TemporalField, Long> fieldValues,
            TemporalAccessor partialTemporal,
            ResolverStyle resolverStyle
        ) {
            long seconds = fieldValues.remove(this);
            fieldValues.put(ChronoField.INSTANT_SECONDS, seconds);
            Long nanos = fieldValues.remove(NANOS_OF_SECOND);
            if (nanos != null) {
                fieldValues.put(ChronoField.NANO_OF_SECOND, nanos);
            }
            return null;
        }
    };

    private static final EpochField NANOS_OF_SECOND = new EpochField(ChronoUnit.NANOS, ChronoUnit.SECONDS, ValueRange.of(0, 999_999_999)) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.NANO_OF_SECOND);
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            return temporal.getLong(ChronoField.NANO_OF_SECOND);
        }
    };

    private static final long NEGATIVE = 0;
    private static final long POSITIVE = 1;
    private static final EpochField SIGN = new EpochField(ChronoUnit.FOREVER, ChronoUnit.FOREVER, ValueRange.of(NEGATIVE, POSITIVE)) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS);
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            return temporal.getLong(ChronoField.INSTANT_SECONDS) < 0 ? NEGATIVE : POSITIVE;
        }
    };

    private static class AbsoluteEpochField extends EpochField {
        private final long unitsPerSecond;
        private final long nanosPerUnit;
        private final ChronoField unitField;
        private final EpochField nanosOfUnitField;

        private AbsoluteEpochField(
            TemporalUnit baseUnit,
            long unitsPerSecond,
            long nanosPerUnit,
            ChronoField unitField,
            EpochField nanosOfUnitField
        ) {
            super(baseUnit, ChronoUnit.FOREVER, LONG_POSITIVE_RANGE);
            this.unitsPerSecond = unitsPerSecond;
            this.nanosPerUnit = nanosPerUnit;
            this.unitField = unitField;
            this.nanosOfUnitField = nanosOfUnitField;
        }

        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS)
                && (temporal.isSupported(ChronoField.NANO_OF_SECOND) || temporal.isSupported(unitField));
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            long instantSeconds = temporal.getLong(ChronoField.INSTANT_SECONDS);
            if (instantSeconds < Long.MIN_VALUE / unitsPerSecond || instantSeconds > Long.MAX_VALUE / unitsPerSecond) {
                // Multiplying would yield integer overflow
                return Long.MAX_VALUE;
            }
            long instantSecondsInUnits = instantSeconds * unitsPerSecond;
            if (instantSecondsInUnits >= 0) {
                if (temporal.isSupported(ChronoField.NANO_OF_SECOND)) {
                    return instantSecondsInUnits + (temporal.getLong(ChronoField.NANO_OF_SECOND) / nanosPerUnit);
                } else {
                    return instantSecondsInUnits + temporal.getLong(unitField);
                }
            } else { // negative timestamp
                if (temporal.isSupported(ChronoField.NANO_OF_SECOND)) {
                    long units = instantSecondsInUnits;
                    long nanos = temporal.getLong(ChronoField.NANO_OF_SECOND);
                    if (nanos % nanosPerUnit != 0) {
                        // Fractional negative timestamp.
                        // Add 1 unit towards positive infinity because the fraction leads
                        // the output's integral part to be an off-by-one when the
                        // `(nanos / nanosPerUnit)` is added below.
                        units += 1;
                    }
                    units += (nanos / nanosPerUnit);
                    return -units;
                } else {
                    long unitsOfSecond = temporal.getLong(unitField);
                    return -(instantSecondsInUnits + unitsOfSecond);
                }
            }
        }

        @Override
        public TemporalAccessor resolve(
            Map<TemporalField, Long> fieldValues,
            TemporalAccessor partialTemporal,
            ResolverStyle resolverStyle
        ) {
            Long sign = Optional.ofNullable(fieldValues.remove(SIGN)).orElse(POSITIVE);

            Long nanosOfUnit = fieldValues.remove(nanosOfUnitField);
            long secondsAndUnits = fieldValues.remove(this);

            long seconds;
            long nanos;
            if (sign == NEGATIVE) {
                secondsAndUnits = -secondsAndUnits;
                seconds = secondsAndUnits / unitsPerSecond;
                nanos = secondsAndUnits % unitsPerSecond * nanosPerUnit;
                // `secondsAndUnits < 0` implies negative timestamp; so `nanos < 0`
                if (nanosOfUnit != null) {
                    // aggregate fractional part of the input; subtract b/c `nanos < 0`
                    nanos -= nanosOfUnit;
                }
                if (nanos != 0) {
                    // nanos must be positive. B/c the timestamp is represented by the
                    // (seconds, nanos) tuple, seconds moves 1s toward negative-infinity
                    // and nanos moves 1s toward positive-infinity
                    seconds -= 1;
                    nanos = 1_000_000_000 + nanos;
                }
            } else {
                seconds = secondsAndUnits / unitsPerSecond;
                nanos = secondsAndUnits % unitsPerSecond * nanosPerUnit;

                if (nanosOfUnit != null) {
                    // aggregate fractional part of the input
                    nanos += nanosOfUnit;
                }
            }
            fieldValues.put(ChronoField.INSTANT_SECONDS, seconds);
            fieldValues.put(ChronoField.NANO_OF_SECOND, nanos);
            // if there is already a milli of second, we need to overwrite it
            if (fieldValues.containsKey(ChronoField.MILLI_OF_SECOND)) {
                fieldValues.put(ChronoField.MILLI_OF_SECOND, nanos / 1_000_000);
            }
            if (fieldValues.containsKey(ChronoField.MICRO_OF_SECOND)) {
                fieldValues.put(ChronoField.MICRO_OF_SECOND, nanos / 1000);
            }
            return null;
        }
    }

    private static final EpochField NANOS_OF_MICRO = new EpochField(ChronoUnit.NANOS, ChronoUnit.MICROS, ValueRange.of(0, 999)) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS)
                && temporal.isSupported(ChronoField.NANO_OF_SECOND)
                && temporal.getLong(ChronoField.NANO_OF_SECOND) % 1_000 != 0;
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            if (temporal.getLong(ChronoField.INSTANT_SECONDS) < 0) {
                return (1_000_000_000 - temporal.getLong(ChronoField.NANO_OF_SECOND)) % 1_000;
            } else {
                return temporal.getLong(ChronoField.NANO_OF_SECOND) % 1_000;
            }
        }
    };

    private static final EpochField NANOS_OF_MILLI = new EpochField(ChronoUnit.NANOS, ChronoUnit.MILLIS, ValueRange.of(0, 999_999)) {
        @Override
        public boolean isSupportedBy(TemporalAccessor temporal) {
            return temporal.isSupported(ChronoField.INSTANT_SECONDS)
                && temporal.isSupported(ChronoField.NANO_OF_SECOND)
                && temporal.getLong(ChronoField.NANO_OF_SECOND) % 1_000_000 != 0;
        }

        @Override
        public long getFrom(TemporalAccessor temporal) {
            if (temporal.getLong(ChronoField.INSTANT_SECONDS) < 0) {
                return (1_000_000_000 - temporal.getLong(ChronoField.NANO_OF_SECOND)) % 1_000_000;
            } else {
                return temporal.getLong(ChronoField.NANO_OF_SECOND) % 1_000_000;
            }
        }
    };

    // Millis as absolute values. Negative millis are encoded by having a NEGATIVE SIGN.
    private static final EpochField MILLIS_ABS = new AbsoluteEpochField(
        ChronoUnit.MILLIS,
        1_000L,
        1_000_000L,
        ChronoField.MILLI_OF_SECOND,
        NANOS_OF_MILLI
    );

    private static final EpochField MICROS = new AbsoluteEpochField(
        ChronoUnit.MICROS,
        1_000_000L,
        1_000L,
        ChronoField.MICRO_OF_SECOND,
        NANOS_OF_MICRO
    );

    // this supports seconds without any fraction
    private static final DateTimeFormatter SECONDS_FORMATTER1 = new DateTimeFormatterBuilder().appendValue(SECONDS, 1, 19, SignStyle.NORMAL)
        .optionalStart() // optional is used so isSupported will be called when printing
        .appendFraction(NANOS_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // this supports seconds ending in dot
    private static final DateTimeFormatter SECONDS_FORMATTER2 = new DateTimeFormatterBuilder().appendValue(SECONDS, 1, 19, SignStyle.NORMAL)
        .appendLiteral('.')
        .toFormatter(Locale.ROOT);

    private static final Map<Long, String> SIGN_FORMATTER_LOOKUP = new HashMap<Long, String>() {
        {
            put(POSITIVE, "");
            put(NEGATIVE, "-");
        }
    };

    // this supports milliseconds
    private static final DateTimeFormatter MILLISECONDS_FORMATTER1 = new DateTimeFormatterBuilder().optionalStart()
        .appendText(SIGN, SIGN_FORMATTER_LOOKUP) // field is only created in the presence of a '-' char.
        .optionalEnd()
        .appendValue(MILLIS_ABS, 1, 19, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendFraction(NANOS_OF_MILLI, 0, 6, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // this supports milliseconds ending in dot
    private static final DateTimeFormatter MILLISECONDS_FORMATTER2 = new DateTimeFormatterBuilder().append(MILLISECONDS_FORMATTER1)
        .appendLiteral('.')
        .toFormatter(Locale.ROOT);

    // this supports microseconds
    private static final DateTimeFormatter MICROSECONDS_FORMATTER1 = new DateTimeFormatterBuilder().optionalStart()
        .appendText(SIGN, SIGN_FORMATTER_LOOKUP) // field is only created in the presence of a '-' char.
        .optionalEnd()
        .appendValue(MICROS, 1, 19, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendFraction(NANOS_OF_MICRO, 0, 3, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // this supports microseconds ending in dot
    private static final DateTimeFormatter MICROSECONDS_FORMATTER2 = new DateTimeFormatterBuilder().append(MICROSECONDS_FORMATTER1)
        .appendLiteral('.')
        .toFormatter(Locale.ROOT);

    static final DateFormatter SECONDS_FORMATTER = new JavaDateFormatter(
        "epoch_second",
        SECONDS_FORMATTER1,
        (builder, parser) -> builder.parseDefaulting(ChronoField.NANO_OF_SECOND, 999_999_999L),
        SECONDS_FORMATTER1,
        SECONDS_FORMATTER2
    );

    static final DateFormatter MILLIS_FORMATTER = new JavaDateFormatter(
        "epoch_millis",
        MILLISECONDS_FORMATTER1,
        (builder, parser) -> builder.parseDefaulting(EpochTime.NANOS_OF_MILLI, 999_999L),
        MILLISECONDS_FORMATTER1,
        MILLISECONDS_FORMATTER2
    );

    static final DateFormatter MICROS_FORMATTER = new JavaDateFormatter(
        "epoch_micros",
        MICROSECONDS_FORMATTER1,
        (builder, parser) -> builder.parseDefaulting(EpochTime.NANOS_OF_MICRO, 999L),
        MICROSECONDS_FORMATTER1,
        MICROSECONDS_FORMATTER2
    );

    /**
     * Base class for an epoch field
     *
     * @opensearch.internal
     */
    private abstract static class EpochField implements TemporalField {

        private final TemporalUnit baseUnit;
        private final TemporalUnit rangeUnit;
        private final ValueRange range;

        private EpochField(TemporalUnit baseUnit, TemporalUnit rangeUnit, ValueRange range) {
            this.baseUnit = baseUnit;
            this.rangeUnit = rangeUnit;
            this.range = range;
        }

        @Override
        public String getDisplayName(Locale locale) {
            return toString();
        }

        @Override
        public String toString() {
            return "Epoch" + baseUnit.toString() + (rangeUnit != ChronoUnit.FOREVER ? "Of" + rangeUnit.toString() : "");
        }

        @Override
        public TemporalUnit getBaseUnit() {
            return baseUnit;
        }

        @Override
        public TemporalUnit getRangeUnit() {
            return rangeUnit;
        }

        @Override
        public ValueRange range() {
            return range;
        }

        @Override
        public boolean isDateBased() {
            return false;
        }

        @Override
        public boolean isTimeBased() {
            return true;
        }

        @Override
        public ValueRange rangeRefinedBy(TemporalAccessor temporal) {
            return range();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <R extends Temporal> R adjustInto(R temporal, long newValue) {
            return (R) temporal.with(this, newValue);
        }
    }
}
