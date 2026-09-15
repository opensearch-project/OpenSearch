/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query.range;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.dsl.converter.ConversionException;

import java.time.Instant;
import java.time.ZoneId;

/**
 * Utilities for resolving date-valued range bounds: type detection, date-math parsing, and inclusivity-keyed rounding.
 */
public final class RangeDateBounds {

    /** Maximum nanosecond-representable instant: 2262-04-11T23:47:16.854775807Z. */
    static final Instant MAX_NANOSECOND_INSTANT = Instant.ofEpochSecond(9223372036L, 854775807L);

    private RangeDateBounds() {}

    /**
     * Resolution for date field parsing, mirroring legacy DateFieldMapper.Resolution vocabulary.
     */
    public enum DateResolution {
        /** Millisecond resolution: returns epoch-millis with no range guards. */
        MILLISECONDS {
            @Override
            long convertEpochMillis(long millis, String strValue) {
                return millis;
            }

            @Override
            long convertInstant(Instant instant) {
                return instant.toEpochMilli();
            }
        },
        /** Nanosecond resolution: rejects negative epoch-millis, clamps to MAX_NANOSECOND_INSTANT, returns epoch-nanos. */
        NANOSECONDS {
            @Override
            long convertEpochMillis(long millis, String strValue) throws ConversionException {
                // Legacy DateUtils.toNanoSeconds: rejects negative, rejects past max
                if (millis < 0) {
                    throw new ConversionException(
                        "Failed to parse epoch_millis value '" + strValue + "': value before epoch not representable in nanos"
                    );
                }
                long maxMillis = MAX_NANOSECOND_INSTANT.getEpochSecond() * 1000 + MAX_NANOSECOND_INSTANT.getNano() / 1_000_000;
                if (millis > maxMillis) {
                    return instantToNanos(MAX_NANOSECOND_INSTANT);
                }
                return millis * 1_000_000L;
            }

            @Override
            long convertInstant(Instant instant) {
                // Clamp per legacy DateUtils.clampToNanosRange
                instant = clampToNanosRange(instant);
                return instantToNanos(instant);
            }
        };

        /** Converts a parsed epoch-millis value to the resolution-appropriate long. */
        abstract long convertEpochMillis(long millis, String strValue) throws ConversionException;

        /** Converts a parsed Instant to the resolution-appropriate long. */
        abstract long convertInstant(Instant instant);
    }

    /** Returns true if the SqlTypeName represents a date/timestamp family type. */
    static boolean isDateType(SqlTypeName typeName) {
        return typeName == SqlTypeName.TIMESTAMP
            || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            || typeName == SqlTypeName.DATE
            || typeName == SqlTypeName.TIME;
    }

    /**
     * Determines if a string value is a date-math expression.
     * Date-math expressions start with "now" or contain "||" (anchored date-math).
     */
    static boolean isDateMathExpression(String value) {
        return value.startsWith("now") || value.contains("||");
    }

    /**
     * Parses a string value as a date using DateMathParser at the given resolution.
     *
     * @param strValue the string to parse
     * @param format optional date format pattern (e.g., "dd/MM/yyyy")
     * @param timeZone optional timezone ID (e.g., "America/New_York", defaults to "UTC")
     * @param roundUp whether to round up to end of time unit (true) or down to start (false)
     * @param resolution the target date resolution (MILLISECONDS or NANOSECONDS)
     * @return epoch milliseconds or nanoseconds depending on resolution
     * @throws ConversionException if date parsing fails
     */
    public static Long parseDateValue(String strValue, String format, String timeZone, boolean roundUp, DateResolution resolution)
        throws ConversionException {
        try {
            if ("epoch_millis".equals(format)) {
                // epoch_millis: parse as raw long, timezone is irrelevant (epoch is absolute)
                try {
                    long millis = Long.parseLong(strValue);
                    return resolution.convertEpochMillis(millis, strValue);
                } catch (NumberFormatException e) {
                    throw new ConversionException("Failed to parse epoch_millis value '" + strValue + "': not a valid number");
                }
            }

            DateFormatter formatter = format != null
                ? DateFormatter.forPattern(format)
                : DateFormatter.forPattern("strict_date_optional_time");
            ZoneId zoneId = timeZone != null ? ZoneId.of(timeZone) : ZoneId.of("UTC");

            Instant instant = formatter.toDateMathParser().parse(strValue, System::currentTimeMillis, roundUp, zoneId);
            return resolution.convertInstant(instant);
        } catch (ConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new ConversionException("Failed to parse date value '" + strValue + "': " + e.getMessage());
        }
    }

    /**
     * Clamps an Instant to the nanosecond-representable range [Epoch, MAX_NANOSECOND_INSTANT],
     * mirroring legacy {@code DateUtils.clampToNanosRange}.
     */
    static Instant clampToNanosRange(Instant instant) {
        if (instant.isBefore(Instant.EPOCH)) {
            return Instant.EPOCH;
        }
        if (instant.isAfter(MAX_NANOSECOND_INSTANT)) {
            return MAX_NANOSECOND_INSTANT;
        }
        return instant;
    }

    /**
     * Converts an Instant to nanos-since-epoch, mirroring legacy {@code DateUtils.toLong}:
     * {@code epochSec * 1_000_000_000 + nano}.
     */
    static long instantToNanos(Instant instant) {
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }
}
