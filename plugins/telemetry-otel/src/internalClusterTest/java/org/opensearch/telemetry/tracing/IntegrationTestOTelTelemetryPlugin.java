/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.OTelTelemetryPlugin;
import org.opensearch.telemetry.Telemetry;

import java.util.Optional;

import io.opentelemetry.api.GlobalOpenTelemetry;

/**
 * Telemetry plugin used for Integration tests.
*/
public class IntegrationTestOTelTelemetryPlugin extends OTelTelemetryPlugin {
    /**
     * Creates IntegrationTestOTelTelemetryPlugin
     */
    public IntegrationTestOTelTelemetryPlugin() {
        super();
    }

    /**
     * This method overrides getTelemetry() method in OTel plugin class, so we create only one instance of global OpenTelemetry
     * resetForTest() will set OpenTelemetry to null again.
     */
    public Optional<Telemetry> getTelemetry() {
        GlobalOpenTelemetry.resetForTest();
        return super.getTelemetry();
    }
}
