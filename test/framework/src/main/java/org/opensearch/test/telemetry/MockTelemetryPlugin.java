/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry;

import java.util.Optional;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;

/**
 * Mock {@link TelemetryPlugin} implementation for testing.
 */
public class MockTelemetryPlugin extends Plugin implements TelemetryPlugin {
    private static final String MOCK_TRACER_NAME = "mock";

    /**
     * Base constructor.
     */
    public MockTelemetryPlugin() {

    }

    @Override
    public Optional<Telemetry> getTelemetry(TelemetrySettings settings) {
        return Optional.of(new MockTelemetry(settings));
    }

    @Override
    public String getName() {
        return MOCK_TRACER_NAME;
    }
}
