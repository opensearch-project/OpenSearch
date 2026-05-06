/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contract test for Group G: every Tier-1 math function and every Tier-2 adapter
 * target is registered as a Scalar project capability on the DataFusion backend.
 * Without this registration {@code OpenSearchProjectRule} drops the function
 * through to a residual project on the coordinator, defeating native pushdown.
 */
public class MathProjectCapabilitiesTests extends OpenSearchTestCase {

    private Set<ScalarFunction> exposedProjectScalars() {
        DataFusionAnalyticsBackendPlugin backendPlugin = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin());
        BackendCapabilityProvider provider = backendPlugin.getCapabilityProvider();
        Set<ScalarFunction> seen = new HashSet<>();
        for (ProjectCapability cap : provider.projectCapabilities()) {
            if (cap instanceof ProjectCapability.Scalar scalar) {
                seen.add(scalar.function());
            }
        }
        return seen;
    }

    public void testTier1MathFunctionsAreProjectCapable() {
        Set<ScalarFunction> projectable = exposedProjectScalars();
        ScalarFunction[] tier1 = new ScalarFunction[] {
            ScalarFunction.ABS,
            ScalarFunction.ACOS,
            ScalarFunction.ASIN,
            ScalarFunction.ATAN,
            ScalarFunction.ATAN2,
            ScalarFunction.CBRT,
            ScalarFunction.CEIL,
            ScalarFunction.COS,
            ScalarFunction.COT,
            ScalarFunction.DEGREES,
            ScalarFunction.EXP,
            ScalarFunction.FLOOR,
            ScalarFunction.LN,
            ScalarFunction.LOG,
            ScalarFunction.LOG10,
            ScalarFunction.LOG2,
            ScalarFunction.PI,
            ScalarFunction.POWER,
            ScalarFunction.RADIANS,
            ScalarFunction.RAND,
            ScalarFunction.ROUND,
            ScalarFunction.SIGN,
            ScalarFunction.SIN,
            ScalarFunction.TAN,
            ScalarFunction.TRUNCATE, };
        for (ScalarFunction f : tier1) {
            assertTrue("Tier-1 function not registered as Scalar project capability: " + f, projectable.contains(f));
        }
    }

    public void testTier2AdapterTargetFunctionsAreProjectCapable() {
        Set<ScalarFunction> projectable = exposedProjectScalars();
        ScalarFunction[] tier2 = new ScalarFunction[] {
            ScalarFunction.COSH,
            ScalarFunction.SINH,
            ScalarFunction.E,
            ScalarFunction.EXPM1,
            ScalarFunction.SCALAR_MAX,
            ScalarFunction.SCALAR_MIN, };
        for (ScalarFunction f : tier2) {
            assertTrue("Tier-2 adapter target not registered as Scalar project capability: " + f, projectable.contains(f));
        }
    }

    public void testTier2AdapterTargetFunctionsHaveAdapters() {
        DataFusionAnalyticsBackendPlugin backendPlugin = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin());
        Map<ScalarFunction, ScalarFunctionAdapter> adapters = backendPlugin.getCapabilityProvider().scalarFunctionAdapters();
        assertNotNull("SINH must have an adapter registered", adapters.get(ScalarFunction.SINH));
        assertNotNull("COSH must have an adapter registered", adapters.get(ScalarFunction.COSH));
        assertNotNull("E must have an adapter registered", adapters.get(ScalarFunction.E));
        assertNotNull("EXPM1 must have an adapter registered", adapters.get(ScalarFunction.EXPM1));
        assertNotNull("SCALAR_MAX must have an adapter registered", adapters.get(ScalarFunction.SCALAR_MAX));
        assertNotNull("SCALAR_MIN must have an adapter registered", adapters.get(ScalarFunction.SCALAR_MIN));
        assertNotNull("SIGN must have an adapter registered", adapters.get(ScalarFunction.SIGN));
    }

    /** MINUS must be project-capable because Expm1Adapter rewrites {@code expm1(x)} to {@code MINUS(EXP(x), 1)}. */
    public void testMinusIsProjectCapableForExpm1AdapterOutput() {
        Set<ScalarFunction> projectable = exposedProjectScalars();
        assertTrue("MINUS must be project-capable because Expm1Adapter emits it", projectable.contains(ScalarFunction.MINUS));
    }
}
