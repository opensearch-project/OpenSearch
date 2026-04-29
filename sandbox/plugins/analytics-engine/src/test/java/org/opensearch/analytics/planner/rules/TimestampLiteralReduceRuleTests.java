/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.util.TimestampString;
import org.opensearch.test.OpenSearchTestCase;

public class TimestampLiteralReduceRuleTests extends OpenSearchTestCase {

    public void testIsoWithTAndZ() {
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-01T00:00:00Z");
        assertEquals("2024-01-01 00:00:00", ts.toString());
    }

    public void testIsoWithTNoZ() {
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-15T10:30:00");
        assertEquals("2024-01-15 10:30:00", ts.toString());
    }

    public void testDateOnly() {
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-01");
        assertEquals("2024-01-01 00:00:00", ts.toString());
    }

    public void testTimezoneOffsetPositive() {
        // 2024-01-01T10:00:00+05:30 = 2024-01-01T04:30:00 UTC
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-01T10:00:00+05:30");
        assertEquals("2024-01-01 04:30:00", ts.toString());
    }

    public void testTimezoneOffsetNegative() {
        // 2024-01-01T10:00:00-05:00 = 2024-01-01T15:00:00 UTC
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-01T10:00:00-05:00");
        assertEquals("2024-01-01 15:00:00", ts.toString());
    }

    public void testWithMilliseconds() {
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-01T10:30:00.123Z");
        assertEquals("2024-01-01 10:30:00.123", ts.toString());
    }

    public void testWithNanoseconds() {
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-01T10:30:00.123456789Z");
        assertEquals("2024-01-01 10:30:00.123456789", ts.toString());
    }

    public void testWithMillisAndTimezone() {
        // 2024-01-01T10:30:00.500+05:30 = 2024-01-01T05:00:00.500 UTC
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-01T10:30:00.500+05:30");
        assertEquals("2024-01-01 05:00:00.5", ts.toString());
    }

    public void testSpaceSeparatorPassthrough() {
        TimestampString ts = TimestampLiteralReduceRule.parseTimestamp("2024-01-01 10:30:00");
        assertEquals("2024-01-01 10:30:00", ts.toString());
    }
}
