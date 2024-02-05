/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.sampler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.telemetry.TelemetrySettings;

import java.lang.reflect.Constructor;
import java.util.HashMap;

import io.opentelemetry.sdk.trace.samplers.Sampler;

/**
 * Factory class to create the instance of OTelSampler
 */
public class OTelSamplerFactory {

    /**
     * Logger instance for logging messages related to the OTelSamplerFactory.
     */
    private static final Logger logger = LogManager.getLogger(OTelSamplerFactory.class);

    /**
     * Base constructor.
     */
    private OTelSamplerFactory() {

    }

    /**
     * Creates the {@link Sampler} instances based on the TRACER_SPAN_SAMPLER_CLASSES value.
     * @param telemetrySettings TelemetrySettings.
     * @param setting Settings
     * @return map of samplers.
     */
    public static HashMap<String, Sampler> create(TelemetrySettings telemetrySettings, Settings setting) {
        HashMap<String, Sampler> samplersMap = new HashMap<>();

        for (String samplerName : OTelTelemetrySettings.OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS.get(setting)) {
            try {
                Class<?> samplerClass = Class.forName(samplerName);
                Constructor<?> constructor = samplerClass.getConstructor(TelemetrySettings.class);
                Sampler sampler = (Sampler) constructor.newInstance(telemetrySettings);
                samplersMap.put(samplerName, sampler);
            } catch (Exception e) {
                logger.error("error while creating sampler class object: ", e);
            }
        }
        return samplersMap;
    }
}
