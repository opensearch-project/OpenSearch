/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.test.OpenSearchTestCase;

import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

public class OTelResourceProviderTests extends OpenSearchTestCase {

    private MetricDataType recordAndGetType(Settings settings) {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        SdkMeterProvider provider = OTelResourceProvider.createSdkMetricProvider(settings, Resource.empty(), reader);
        try {
            provider.get("test").histogramBuilder("h").build().record(2.0);
            return reader.collectAllMetrics().iterator().next().getType();
        } finally {
            provider.close();
        }
    }

    public void testDefaultIsExponentialHistogram() {
        assertEquals(MetricDataType.EXPONENTIAL_HISTOGRAM, recordAndGetType(Settings.EMPTY));
    }

    public void testExplicitBucketHistogram() {
        Settings settings = Settings.builder()
            .put(OTelTelemetrySettings.OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.getKey(), "explicit_bucket_histogram")
            .build();
        assertEquals(MetricDataType.HISTOGRAM, recordAndGetType(settings));
    }
}
