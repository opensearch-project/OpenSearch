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

import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.DefaultAggregationSelector;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

public class OTelResourceProviderTests extends OpenSearchTestCase {

    private MetricDataType recordAndGetType(Settings settings) {
        return recordAndGetType(settings, InMemoryMetricReader.create());
    }

    private MetricDataType recordAndGetType(Settings settings, InMemoryMetricReader reader) {
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

    public void testExplicitBucketHistogramOverridesExporterDefault() {
        // InMemoryMetricReader.create() already defaults to explicit buckets; only an exponential-by-default reader shows the View decides.
        InMemoryMetricReader exponentialByDefault = InMemoryMetricReader.create(
            AggregationTemporalitySelector.alwaysCumulative(),
            DefaultAggregationSelector.getDefault().with(InstrumentType.HISTOGRAM, Aggregation.base2ExponentialBucketHistogram())
        );
        Settings settings = Settings.builder()
            .put(OTelTelemetrySettings.OTEL_METRICS_HISTOGRAM_AGGREGATION_DEFAULT_SETTING.getKey(), "explicit_bucket_histogram")
            .build();
        assertEquals(MetricDataType.HISTOGRAM, recordAndGetType(settings, exponentialByDefault));
    }
}
