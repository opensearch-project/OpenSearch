/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.exporter;

import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.opensearch.common.settings.Settings;

/**
 * Provides the instance of {@link io.opentelemetry.exporter.logging.LoggingSpanExporter}
 */
public class LoggingSpanExporterProvider implements SpanExporterProvider {

    /**
     * Base Constructor.
     */
    public LoggingSpanExporterProvider() {}

    @Override
    public SpanExporter create(Settings settings) {
        return LoggingSpanExporter.create();
    }
}
