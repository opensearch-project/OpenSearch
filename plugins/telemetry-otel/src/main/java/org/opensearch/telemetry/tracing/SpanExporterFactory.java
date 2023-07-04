/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class SpanExporterFactory {

    public SpanExporter create(SpanExporterType exporterType) {
        switch (exporterType) {
            case LOGGING:
                return LoggingSpanExporter.create();
            case OLTP_GRPC:
                return
        }
    }
}
