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
import org.opensearch.SpecialPermission;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.telemetry.TelemetrySettings;

import java.lang.reflect.Constructor;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

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
     * @return list of samplers.
     */
    public static List<Sampler> create(TelemetrySettings telemetrySettings, Settings setting) {
        List<Sampler> samplersList = new ArrayList<>();
        for (String samplerName : OTelTelemetrySettings.OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS.get(setting)) {
            Sampler sampler = instantiateSampler(samplerName, telemetrySettings);
            samplersList.add(sampler);
        }
        logger.info("Successfully instantiated the Sampler classes list {}", samplersList);
        return samplersList;
    }

    private static Sampler instantiateSampler(String samplerName, TelemetrySettings telemetrySettings) {
        try {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();

            return AccessController.doPrivileged((PrivilegedExceptionAction<Sampler>) () -> {
                Class<?> samplerClass = Class.forName(samplerName);
                Constructor<?> constructor = samplerClass.getConstructor(TelemetrySettings.class);

                return (Sampler) constructor.newInstance(telemetrySettings);
            });
        } catch (Exception e) {
            throw new IllegalStateException("Sampler instantiation failed for class [" + samplerName + "]", e.getCause());
        }
    }
}
