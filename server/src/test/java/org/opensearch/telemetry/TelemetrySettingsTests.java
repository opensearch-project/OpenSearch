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
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_ACTION_PROBABILITY;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;

public class TelemetrySettingsTests extends OpenSearchTestCase {

    public void testSetTracingEnabledOrDisabled() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // Validation for tracingEnabled as true
        telemetrySettings.setTracingEnabled(true);
        assertTrue(telemetrySettings.isTracingEnabled());

        // Validation for tracingEnabled as false
        telemetrySettings.setTracingEnabled(false);
        assertFalse(telemetrySettings.isTracingEnabled());
    }

    public void testIsActionSamplingOverrideSet() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // There should be no override for action initially
        assertFalse(telemetrySettings.isActionSamplingOverrideSet("dummy_action"));

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.dummy_action.probability", "0.01").build());
        // Validating if value of override for action 'dummy_action' is correct
        assertTrue(telemetrySettings.isActionSamplingOverrideSet("dummy_action"));
    }

    public void testSetSamplingProbability() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
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
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // Validating default value of Sampling is 1%
        assertEquals(0.01, telemetrySettings.getSamplingProbability(), 0.00d);

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.sampler.probability", "0.02").build());

        // Validating if default sampling is updated to 2%
        assertEquals(0.02, telemetrySettings.getSamplingProbability(), 0.00d);
    }

    public void testSetActionSamplingProbability() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.dummy_action.probability", "0.5").build());

        // Validating if value of override for action 'dummy_action' is correct
        assertEquals(0.5, telemetrySettings.getActionSamplingProbability("dummy_action"), 0.00d);

    }

    public void testGetActionSamplingProbability() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.dummy_action.probability", "0.01").build());
        // Validating if value of override for action 'dummy_action' is correct i.e. 1%
        assertEquals(0.01, telemetrySettings.getActionSamplingProbability("dummy_action"), 0.00d);

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.dummy_action.probability", "0.02").build());
        // Validating if value of override for action 'dummy_action' is correct i.e. 2%
        assertEquals(0.02, telemetrySettings.getActionSamplingProbability("dummy_action"), 0.00d);
    }

}
