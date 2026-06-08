/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

/** Unit tests for {@link DateFormatClassifier#classify(String)}. */
public class DateFormatClassifierTests extends OpenSearchTestCase {

    public void testNamedDateOnlyFormatsAreDate() {
        for (String f : new String[] {
            "basic_date",
            "basic_ordinal_date",
            "date",
            "strict_date",
            "ordinal_date",
            "week_date",
            "weekyear_week_day",
            "year_month_day" }) {
            assertEquals("format " + f, SqlTypeName.DATE, DateFormatClassifier.classify(f));
        }
    }

    public void testNamedTimeOnlyFormatsAreTime() {
        for (String f : new String[] { "hour", "basic_time", "hour_minute_second", "time" }) {
            assertEquals("format " + f, SqlTypeName.TIME, DateFormatClassifier.classify(f));
        }
    }

    public void testDatetimeAndEpochFormatsAreTimestamp() {
        for (String f : new String[] {
            "date_optional_time",
            "strict_date_optional_time",
            "epoch_millis",
            "epoch_second",
            "date_hour_minute_second" }) {
            assertEquals("format " + f, SqlTypeName.TIMESTAMP, DateFormatClassifier.classify(f));
        }
    }

    public void testCustomDateOnlyPatternIsDate() {
        assertEquals(SqlTypeName.DATE, DateFormatClassifier.classify("yyyy-MM-dd"));
        assertEquals(SqlTypeName.DATE, DateFormatClassifier.classify("yyyy/MM/dd"));
    }

    public void testCustomTimeOnlyPatternIsTime() {
        assertEquals(SqlTypeName.TIME, DateFormatClassifier.classify("HH:mm:ss"));
    }

    public void testCustomDatetimePatternIsTimestamp() {
        assertEquals(SqlTypeName.TIMESTAMP, DateFormatClassifier.classify("yyyy-MM-dd HH:mm:ss"));
        // The 'T' literal must be stripped before letter scanning, leaving date + time components.
        assertEquals(SqlTypeName.TIMESTAMP, DateFormatClassifier.classify("yyyy-MM-dd'T'HH:mm:ss"));
    }

    public void testCombinedAlternativesAllDateOnlyIsDate() {
        assertEquals(SqlTypeName.DATE, DateFormatClassifier.classify("yyyy-MM-dd||date"));
        assertEquals(SqlTypeName.DATE, DateFormatClassifier.classify("basic_date||year_month_day"));
    }

    public void testCombinedAlternativesMixedComponentsIsTimestamp() {
        // A date-only alternative mixed with a time-only one is not uniformly date or time.
        assertEquals(SqlTypeName.TIMESTAMP, DateFormatClassifier.classify("yyyy-MM-dd||HH:mm:ss"));
        assertEquals(SqlTypeName.TIMESTAMP, DateFormatClassifier.classify("date||epoch_millis"));
    }

    public void testNullOrBlankFormatIsTimestamp() {
        assertEquals(SqlTypeName.TIMESTAMP, DateFormatClassifier.classify(null));
        assertEquals(SqlTypeName.TIMESTAMP, DateFormatClassifier.classify(""));
        assertEquals(SqlTypeName.TIMESTAMP, DateFormatClassifier.classify("   "));
    }
}
