/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.sandbox;

import java.util.Set;

/**
 * Reflection helpers shared by the sandbox wiring ITs ({@link SandboxStackWiringIT} and
 * {@link SandboxStackSingleNodeWiringIT}). These ITs must compile and run WITHOUT any compile dependency on the JDK 25
 * sandbox plugins (they only run when {@code -Dsandbox.enabled=true} puts those plugins on the classpath), so the
 * analytics engine's node-level search service is referenced by FQCN and reached reflectively.
 */
final class SandboxStackTestUtils {

    private SandboxStackTestUtils() {}

    /** FQCN of the analytics engine's node-level search service component (resolved reflectively — no sandbox import). */
    static final String ANALYTICS_SEARCH_SERVICE = "org.opensearch.analytics.exec.AnalyticsSearchService";

    /** The {@link Class} of the analytics engine's node-level search service, so callers can look it up from an injector. */
    static Class<?> analyticsSearchServiceClass() throws Exception {
        return Class.forName(ANALYTICS_SEARCH_SERVICE);
    }

    /**
     * The analytics backend ids registered with AnalyticsPlugin on the node that owns the given
     * {@code AnalyticsSearchService} instance, read through {@code AnalyticsSearchService#getRegisteredBackendNames}
     * reflectively so this stays free of any compile dependency on the sandbox plugins.
     */
    @SuppressWarnings("unchecked")
    static Set<String> getRegisteredBackendNames(Object analyticsSearchService) throws Exception {
        return (Set<String>) analyticsSearchService.getClass().getMethod("getRegisteredBackendNames").invoke(analyticsSearchService);
    }
}
