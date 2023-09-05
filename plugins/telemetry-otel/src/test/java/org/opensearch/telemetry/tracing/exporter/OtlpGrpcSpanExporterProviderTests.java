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
import org.opensearch.test.OpenSearchTestCase;

public class OtlpGrpcSpanExporterProviderTests extends OpenSearchTestCase {
    public void testSpanExporterInstantiatedProperly() {
        Settings settings = Settings.builder()
            .put(
                OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.tracing.exporter.OtlpGrpcSpanExporterProvider"
            )
            .build();

        assertTrue(OTelSpanExporterFactory.create(settings) instanceof OtlpGrpcSpanExporterProvider);
    }
}
