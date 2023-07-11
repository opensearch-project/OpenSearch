/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.exporter;

import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.opensearch.common.settings.Settings;

/**
 * Helps in creating the SpanExporter.
 */
public interface SpanExporterProvider {
    /**
     * Creates the {@link SpanExporter} instance.
     * @param settings settings.
     * @return SpanExporter
     */
    SpanExporter create(Settings settings);
}
