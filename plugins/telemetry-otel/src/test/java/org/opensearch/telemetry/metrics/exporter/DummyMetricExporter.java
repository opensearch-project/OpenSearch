/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.exporter;

import java.util.Collection;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

public class DummyMetricExporter implements MetricExporter {
    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        return null;
    }

    @Override
    public CompletableResultCode flush() {
        return null;
    }

    @Override
    public CompletableResultCode shutdown() {
        return null;
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return null;
    }
}
