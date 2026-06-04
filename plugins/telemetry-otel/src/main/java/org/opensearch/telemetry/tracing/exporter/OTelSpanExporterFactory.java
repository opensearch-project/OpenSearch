/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.exporter;

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

import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * Factory class to create the {@link SpanExporter} instance.
 */
public class OTelSpanExporterFactory {

    private static final Logger logger = LogManager.getLogger(OTelSpanExporterFactory.class);

    /**
     * Base constructor.
     */
    private OTelSpanExporterFactory() {

    }

    /**
     * Creates the {@link SpanExporter} instances based on the OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING value.
     * As of now, it expects the SpanExporter implementations to have a create factory method to instantiate the
     * SpanExporter.
     * @param settings settings.
     * @return SpanExporter instance.
     */
    public static SpanExporter create(Settings settings) {
        Class<SpanExporter> spanExporterProviderClass = OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.get(settings);
        SpanExporter spanExporter = instantiateSpanExporter(spanExporterProviderClass);
        logger.info("Successfully instantiated the SpanExporter class {}", spanExporterProviderClass);
        return spanExporter;
    }

    @SuppressWarnings("removal")
    private static SpanExporter instantiateSpanExporter(Class<SpanExporter> spanExporterProviderClass) {
        try {
            // Check we ourselves are not being called by unprivileged code.
            SpecialPermission.check();
            return AccessController.doPrivileged((PrivilegedExceptionAction<SpanExporter>) () -> {
                String methodName = "create";
                String getDefaultMethod = "getDefault";
                for (Method m : spanExporterProviderClass.getMethods()) {
                    if (m.getName().equals(getDefaultMethod)) {
                        methodName = getDefaultMethod;
                        break;
                    }
                }
                try {
                    return (SpanExporter) MethodHandles.publicLookup()
                        .findStatic(spanExporterProviderClass, methodName, MethodType.methodType(spanExporterProviderClass))
                        .asType(MethodType.methodType(SpanExporter.class))
                        .invokeExact();
                } catch (Throwable e) {
                    if (e.getCause() instanceof NoSuchMethodException) {
                        throw new IllegalStateException("No create factory method exist in [" + spanExporterProviderClass.getName() + "]");
                    } else {
                        throw new IllegalStateException(
                            "SpanExporter instantiation failed for class [" + spanExporterProviderClass.getName() + "]",
                            e.getCause()
                        );
                    }
                }
            });
        } catch (PrivilegedActionException ex) {
            throw new IllegalStateException(
                "SpanExporter instantiation failed for class [" + spanExporterProviderClass.getName() + "]",
                ex.getCause()
            );
        }
    }
}
