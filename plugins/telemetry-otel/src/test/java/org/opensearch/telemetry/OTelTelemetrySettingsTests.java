/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OTelTelemetrySettings.HistogramAggregation;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.telemetry.OTelTelemetrySettings.OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING;
import static org.opensearch.telemetry.OTelTelemetrySettings.OTEL_SERVICE_NAME_SETTING;

public class OTelTelemetrySettingsTests extends OpenSearchTestCase {

    public void testOtelServiceNameDefaultsToOpenSearch() {
        assertEquals("OpenSearch", OTEL_SERVICE_NAME_SETTING.get(Settings.EMPTY));
    }

    public void testOtelServiceNameUsesConfiguredValue() {
        Settings settings = Settings.builder().put(OTEL_SERVICE_NAME_SETTING.getKey(), "jira-search").build();
        assertEquals("jira-search", OTEL_SERVICE_NAME_SETTING.get(settings));
    }

    public void testOtelServiceNameRejectsBlankValue() {
        for (String invalid : new String[] { "", " ", "\t" }) {
            Settings settings = Settings.builder().put(OTEL_SERVICE_NAME_SETTING.getKey(), invalid).build();
            expectThrows(IllegalArgumentException.class, () -> OTEL_SERVICE_NAME_SETTING.get(settings));
        }
    }

    private static Settings histogram(String value) {
        return Settings.builder().put(OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.getKey(), value).build();
    }

    public void testHistogramAggregationDefaultsToBase2Exponential() {
        assertEquals(
            HistogramAggregation.BASE2_EXPONENTIAL_BUCKET_HISTOGRAM,
            OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.get(Settings.EMPTY)
        );
    }

    public void testHistogramAggregationAcceptsBothValues() {
        assertEquals(
            HistogramAggregation.EXPLICIT_BUCKET_HISTOGRAM,
            OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.get(histogram("explicit_bucket_histogram"))
        );
        assertEquals(
            HistogramAggregation.BASE2_EXPONENTIAL_BUCKET_HISTOGRAM,
            OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.get(histogram("base2_exponential_bucket_histogram"))
        );
    }

    public void testHistogramAggregationIsCaseInsensitive() {
        assertEquals(
            HistogramAggregation.EXPLICIT_BUCKET_HISTOGRAM,
            OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.get(histogram("EXPLICIT_BUCKET_HISTOGRAM"))
        );
    }

    public void testHistogramAggregationRejectsInvalidValue() {
        for (String invalid : new String[] { "foo", "" }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.get(histogram(invalid))
            );
            assertTrue(e.getMessage(), e.getMessage().contains(OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.getKey()));
        }
    }
}
