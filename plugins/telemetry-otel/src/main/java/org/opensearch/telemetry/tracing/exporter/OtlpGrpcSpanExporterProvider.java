/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.exporter;

import java.util.Collection;

import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
* OtlpGrpcSpanExporterProvider class creates an instance of OtlpGrpcSpanExporter
*/
public class OtlpGrpcSpanExporterProvider implements SpanExporter {

    private OtlpGrpcSpanExporter delegate;

    private OtlpGrpcSpanExporterProvider(OtlpGrpcSpanExporter exporter) {
        this.delegate = exporter;
    }

    /**
     * create() is expected by OTelSpanExporterFactory. This creates an instance of
     * OtlpGrpcSpanExporter and sets https endpoint.
     */
    public static OtlpGrpcSpanExporterProvider create() {
        OtlpGrpcSpanExporter exporter = OtlpGrpcSpanExporter.builder().setEndpoint("https://localhost:4317").build();
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
