/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/**
 * Contract test: CAST and SAFE_CAST must be registered as Scalar project capabilities
 * for ARRAY and MAP return types. The planner keys capability lookups on the call's
 * return type, so without these registrations, queries like {@code mvfind(array(...), ...)}
 * that trigger implicit CAST-to-ARRAY fail with "No backend supports scalar function [CAST]".
 */
public class CastProjectCapabilitiesTests extends OpenSearchTestCase {

    private Set<ProjectCapability> getProjectCapabilities() {
        DataFusionAnalyticsBackendPlugin backendPlugin = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin());
        BackendCapabilityProvider provider = backendPlugin.getCapabilityProvider();
        return provider.projectCapabilities();
    }

    private boolean hasScalarCapability(ScalarFunction function, FieldType fieldType) {
        Set<ProjectCapability> capabilities = getProjectCapabilities();
        for (ProjectCapability cap : capabilities) {
            if (cap instanceof ProjectCapability.Scalar scalar) {
                if (scalar.function().equals(function) && scalar.fieldTypes().contains(fieldType)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void testCastSupportsArrayReturnType() {
        assertTrue(
            "CAST must be registered for ARRAY return type to support implicit array casts",
            hasScalarCapability(ScalarFunction.CAST, FieldType.ARRAY)
        );
    }

    public void testCastSupportsMapReturnType() {
        assertTrue(
            "CAST must be registered for MAP return type to support implicit map casts",
            hasScalarCapability(ScalarFunction.CAST, FieldType.MAP)
        );
    }

    public void testSafeCastSupportsArrayReturnType() {
        assertTrue(
            "SAFE_CAST must be registered for ARRAY return type to support nullable array casts",
            hasScalarCapability(ScalarFunction.SAFE_CAST, FieldType.ARRAY)
        );
    }

    public void testSafeCastSupportsMapReturnType() {
        assertTrue(
            "SAFE_CAST must be registered for MAP return type to support nullable map casts",
            hasScalarCapability(ScalarFunction.SAFE_CAST, FieldType.MAP)
        );
    }
}
