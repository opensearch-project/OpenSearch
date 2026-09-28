/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.opensearch.analytics.schema.DateFormatClassifier;
import org.opensearch.analytics.schema.DateFormatClassifier.Kind;
import org.opensearch.test.OpenSearchTestCase;

public class DateFormatClassifierTests extends OpenSearchTestCase {

    public void testNullAndEmptyAreTimestamp() {
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify(null));
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify(""));
    }

    public void testNamedDateOnlyFormats() {
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("basic_date"));
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("date"));
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("strict_date"));
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("week_date"));
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("year_month_day"));
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("weekyear_week_day"));
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("ordinal_date"));
    }

    public void testNamedTimeOnlyFormats() {
        assertEquals(Kind.TIME_ONLY, DateFormatClassifier.classify("basic_time"));
        assertEquals(Kind.TIME_ONLY, DateFormatClassifier.classify("time"));
        assertEquals(Kind.TIME_ONLY, DateFormatClassifier.classify("hour_minute_second"));
        assertEquals(Kind.TIME_ONLY, DateFormatClassifier.classify("hour"));
        assertEquals(Kind.TIME_ONLY, DateFormatClassifier.classify("t_time_no_millis"));
    }

    public void testNamedDatetimeFormatsAreTimestamp() {
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("basic_date_time"));
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("date_optional_time"));
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("strict_date_optional_time_nanos"));
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("epoch_millis"));
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("epoch_second"));
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("date_hour_minute_second"));
    }

    public void testCustomDateOnlyPattern() {
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("yyyy-MM-dd"));
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("uuuu-MM-dd"));
    }

    public void testCustomTimeOnlyPattern() {
        assertEquals(Kind.TIME_ONLY, DateFormatClassifier.classify("HH:mm:ss"));
        assertEquals(Kind.TIME_ONLY, DateFormatClassifier.classify("HH"));
    }

    public void testCustomMixedPatternIsTimestamp() {
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("uuuuMMddHHmmss"));
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("yyyy-MM-dd HH:mm:ss"));
    }

    public void testMultipleConsistentFormats() {
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("yyyy-MM-dd||date"));
        assertEquals(Kind.TIME_ONLY, DateFormatClassifier.classify("HH:mm:ss||time"));
    }

    public void testMultipleMixedFormatsAreTimestamp() {
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("yyyy-MM-dd || HH:mm:ss"));
        assertEquals(Kind.TIMESTAMP, DateFormatClassifier.classify("date||time"));
    }

    public void testStrict8Prefix() {
        // Joda-compat 8-prefix is stripped before lookup.
        assertEquals(Kind.DATE_ONLY, DateFormatClassifier.classify("8basic_date"));
    }
}
