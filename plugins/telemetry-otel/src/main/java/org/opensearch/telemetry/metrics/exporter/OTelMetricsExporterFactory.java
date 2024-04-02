/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.exporter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.SpecialPermission;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.OTelTelemetrySettings;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import io.opentelemetry.sdk.metrics.export.MetricExporter;

/**
 * Factory class to create the {@link MetricExporter} instance.
 */
public class OTelMetricsExporterFactory {

    private static final Logger logger = LogManager.getLogger(OTelMetricsExporterFactory.class);

    /**
     * Base constructor.
     */
    private OTelMetricsExporterFactory() {

    }

    /**
     * Creates the {@link MetricExporter} instances based on the OTEL_METRIC_EXPORTER_CLASS_SETTING value.
     * As of now, it expects the MetricExporter implementations to have a create factory method to instantiate the
     * MetricExporter.
     * @param settings settings.
     * @return MetricExporter instance.
     */
    public static MetricExporter create(Settings settings) {
        Class<MetricExporter> MetricExporterProviderClass = OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.get(settings);
        MetricExporter metricExporter = instantiateExporter(MetricExporterProviderClass);
        logger.info("Successfully instantiated the Metrics MetricExporter class {}", MetricExporterProviderClass);
        return metricExporter;
    }

    @SuppressWarnings("removal")
    private static MetricExporter instantiateExporter(Class<MetricExporter> exporterProviderClass) {
        try {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();
            return AccessController.doPrivileged((PrivilegedExceptionAction<MetricExporter>) () -> {
                String methodName = "create";
                String getDefaultMethod = "getDefault";
                for (Method m : exporterProviderClass.getMethods()) {
                    if (m.getName().equals(getDefaultMethod)) {
                        methodName = getDefaultMethod;
                        break;
                    }
                }
                try {
                    return (MetricExporter) MethodHandles.publicLookup()
                        .findStatic(exporterProviderClass, methodName, MethodType.methodType(exporterProviderClass))
                        .asType(MethodType.methodType(MetricExporter.class))
                        .invokeExact();
                } catch (Throwable e) {
                    if (e.getCause() instanceof NoSuchMethodException) {
                        throw new IllegalStateException("No create factory method exist in [" + exporterProviderClass.getName() + "]");
                    } else {
                        throw new IllegalStateException(
                            "MetricExporter instantiation failed for class [" + exporterProviderClass.getName() + "]",
                            e.getCause()
                        );
                    }
                }
            });
        } catch (PrivilegedActionException ex) {
            throw new IllegalStateException(
                "MetricExporter instantiation failed for class [" + exporterProviderClass.getName() + "]",
                ex.getCause()
            );
        }
    }
}
