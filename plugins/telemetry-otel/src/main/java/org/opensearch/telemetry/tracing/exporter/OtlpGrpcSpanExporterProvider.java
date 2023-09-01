/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.exporter;

import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OTelTelemetrySettings;

import java.util.Collection;

import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
* OtlpGrpcSpanExporterProvider class creates an instance of OtlpGrpcSpanExporter
*/
public final class OtlpGrpcSpanExporterProvider implements SpanExporter {

    private OtlpGrpcSpanExporter delegate;

    private OtlpGrpcSpanExporterProvider(OtlpGrpcSpanExporter exporter) {
        this.delegate = exporter;
    }

    /**
     * create method is expected by OTelSpanExporterFactory. This creates an instance of
     * OtlpGrpcSpanExporter and sets https endpoint.
     * @param settings settings
     * @return OtlpGrpcSpanExporterProvider instance.
     */
    public static OtlpGrpcSpanExporterProvider create(Settings settings) {
        OtlpGrpcSpanExporter exporter;
        String endpoint = OTelTelemetrySettings.TRACER_SPAN_EXPORTER_ENDPOINT.get(settings);
        // if endpoint is empty, do not set Endpoint
        if (endpoint.isEmpty()) {
            exporter = OtlpGrpcSpanExporter.builder().build();
        } else {
            exporter = OtlpGrpcSpanExporter.builder().setEndpoint(endpoint).build();
        }
        return new OtlpGrpcSpanExporterProvider(exporter);
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        return delegate.export(spans);
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }
}
