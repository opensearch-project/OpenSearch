/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.dsl.converter.ConversionException;

import java.time.Instant;
import java.time.ZoneId;

/**
 * Date parsing and rounding helpers used by {@link RangeQueryTranslator} for processing
 * date-valued range bounds. Handles date-math expressions, format conversion, timezone
 * application, and inclusivity-keyed rounding per legacy {@code DateFieldMapper.dateRangeQuery}.
 */
final class RangeDateParsing {

    /** Maximum nanosecond-representable instant: 2262-04-11T23:47:16.854775807Z. */
    static final Instant MAX_NANOSECOND_INSTANT = Instant.ofEpochSecond(9223372036L, 854775807L);

    private RangeDateParsing() {}

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
     * Parses a string value as a date using DateMathParser at millisecond resolution.
     * Handles epoch_millis format specially: timezone is ignored since epoch is absolute.
     *
     * @param strValue the string to parse
     * @param format optional date format pattern (e.g., "dd/MM/yyyy")
     * @param timeZone optional timezone ID (e.g., "America/New_York", defaults to "UTC")
     * @param roundUp whether to round up to end of time unit (true) or down to start (false)
     * @return epoch milliseconds
     * @throws ConversionException if date parsing fails
     */
    static Long parseDateValueMillis(String strValue, String format, String timeZone, boolean roundUp) throws ConversionException {
        try {
            if ("epoch_millis".equals(format)) {
                // epoch_millis: parse as raw long, timezone is irrelevant (epoch is absolute)
                try {
                    return Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw new ConversionException("Failed to parse epoch_millis value '" + strValue + "': not a valid number");
                }
            }

            DateFormatter formatter = format != null
                ? DateFormatter.forPattern(format)
                : DateFormatter.forPattern("strict_date_optional_time");
            ZoneId zoneId = timeZone != null ? ZoneId.of(timeZone) : ZoneId.of("UTC");

            return formatter.toDateMathParser().parse(strValue, System::currentTimeMillis, roundUp, zoneId).toEpochMilli();
        } catch (ConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new ConversionException("Failed to parse date value '" + strValue + "': " + e.getMessage());
        }
    }

    /**
     * Parses a string value as a date at nanosecond resolution, mirroring legacy
     * {@code DateFieldMapper.Resolution.NANOSECONDS} semantics.
     * <p>
     * The parsed Instant is clamped to [Epoch, 2262-04-11T23:47:16.854775807Z] per
     * legacy {@code DateUtils.clampToNanosRange}, then converted to nanos-since-epoch
     * via {@code epochSec * 1_000_000_000 + nano} (legacy {@code DateUtils.toLong}).
     *
     * @param strValue the string to parse
     * @param format optional date format pattern
     * @param timeZone optional timezone ID (defaults to "UTC")
     * @param roundUp whether to round up per DateMathParser rounding semantics
     * @return nanoseconds since epoch
     * @throws ConversionException if date parsing fails
     */
    static Long parseDateValueNanos(String strValue, String format, String timeZone, boolean roundUp) throws ConversionException {
        try {
            if ("epoch_millis".equals(format)) {
                try {
                    long millis = Long.parseLong(strValue);
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
                } catch (NumberFormatException e) {
                    throw new ConversionException("Failed to parse epoch_millis value '" + strValue + "': not a valid number");
                }
            }

            DateFormatter formatter = format != null
                ? DateFormatter.forPattern(format)
                : DateFormatter.forPattern("strict_date_optional_time");
            ZoneId zoneId = timeZone != null ? ZoneId.of(timeZone) : ZoneId.of("UTC");

            Instant instant = formatter.toDateMathParser().parse(strValue, System::currentTimeMillis, roundUp, zoneId);
            // Clamp per legacy DateUtils.clampToNanosRange
            instant = clampToNanosRange(instant);
            return instantToNanos(instant);
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
