/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_INFERRED_SAMPLER_ALLOWLISTED;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;

public class TelemetrySettingsTests extends OpenSearchTestCase {

    public void testSetTracingEnabledOrDisabled() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // Validation for tracingEnabled as true
        telemetrySettings.setTracingEnabled(true);
        assertTrue(telemetrySettings.isTracingEnabled());

        // Validation for tracingEnabled as false
        telemetrySettings.setTracingEnabled(false);
        assertFalse(telemetrySettings.isTracingEnabled());
    }

    public void testSetSamplingProbability() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // Validating default sample rate i.e 1%
        assertEquals(0.01, telemetrySettings.getSamplingProbability(), 0.00d);

        // Validating override for sampling for 100% request
        telemetrySettings.setSamplingProbability(1.00);
        assertEquals(1.00, telemetrySettings.getSamplingProbability(), 0.00d);

        // Validating override for sampling for 50% request
        telemetrySettings.setSamplingProbability(0.50);
        assertEquals(0.50, telemetrySettings.getSamplingProbability(), 0.00d);
    }

    public void testGetSamplingProbability() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // Validating default value of Sampling is 1%
        assertEquals(0.01, telemetrySettings.getSamplingProbability(), 0.00d);

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.sampler.probability", "0.02").build());

        // Validating if default sampling is updated to 2%
        assertEquals(0.02, telemetrySettings.getSamplingProbability(), 0.00d);
    }

    public void testGetInferredSetting() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        assertFalse(telemetrySettings.getInferredSamplingAllowListed());

        clusterSettings.applySettings(Settings.builder().put("telemetry.inferred.sampler.allowlisted", "true").build());

        // Validate inferred allowlist setting
        assertTrue(telemetrySettings.getInferredSamplingAllowListed());
    }

    public void testSetInferredSetting() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        assertFalse(telemetrySettings.getInferredSamplingAllowListed());

        telemetrySettings.setInferredSamplingAllowListed(true);

        // Validate inferred allowlist setting
        assertTrue(telemetrySettings.getInferredSamplingAllowListed());
    }

}
