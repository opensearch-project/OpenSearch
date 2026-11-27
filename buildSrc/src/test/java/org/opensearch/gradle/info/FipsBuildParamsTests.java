/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.info;

import org.opensearch.gradle.test.GradleUnitTestCase;

import java.util.function.Function;

public class FipsBuildParamsTests extends GradleUnitTestCase {

    public void testIsFipsModeEnabled() {
        FipsBuildParams.init(cryptoEntryFnWithStringParam);

        FipsBuildParams.fipsModeEnvSupplier = () -> "true";
        assertTrue(FipsBuildParams.isFipsModeEnabled());

        FipsBuildParams.fipsModeEnvSupplier = () -> "TRUE";
        assertTrue(FipsBuildParams.isFipsModeEnabled());

        FipsBuildParams.fipsModeEnvSupplier = () -> "false";
        assertFalse(FipsBuildParams.isFipsModeEnabled());

        FipsBuildParams.fipsModeEnvSupplier = () -> null;
        assertFalse(FipsBuildParams.isFipsModeEnabled());

        // Not a FIPS build: FIPS mode stays off regardless of the env var
        FipsBuildParams.init(param -> null);
        FipsBuildParams.fipsModeEnvSupplier = () -> "true";
        assertFalse(FipsBuildParams.isFipsModeEnabled());

        // Reset
        FipsBuildParams.fipsModeEnvSupplier = () -> System.getenv("OPENSEARCH_FIPS_MODE");
    }

    public void testIsFipsBuild() {
        FipsBuildParams.init(cryptoEntryFnWithStringParam);
        assertTrue(FipsBuildParams.isFipsBuild());

        FipsBuildParams.init(cryptoEntryFnWithBooleanParam);
        assertTrue(FipsBuildParams.isFipsBuild());

        FipsBuildParams.init(param -> "FIPS-140-2");
        assertFalse(FipsBuildParams.isFipsBuild());

        FipsBuildParams.init(param -> null);
        assertFalse(FipsBuildParams.isFipsBuild());
    }

    @SuppressWarnings("deprecation")
    public void testDeprecatedNamesDelegate() {
        FipsBuildParams.init(cryptoEntryFnWithStringParam);
        FipsBuildParams.fipsModeEnvSupplier = () -> "true";
        assertTrue(FipsBuildParams.isInFipsMode());
        assertTrue(FipsBuildParams.isInFipsApprovedOnlyMode());

        FipsBuildParams.fipsModeEnvSupplier = () -> "false";
        assertTrue(FipsBuildParams.isInFipsMode());
        assertFalse(FipsBuildParams.isInFipsApprovedOnlyMode());

        FipsBuildParams.fipsModeEnvSupplier = () -> System.getenv("OPENSEARCH_FIPS_MODE");
    }

    public void testGetFipsMode() {
        FipsBuildParams.init(cryptoEntryFnWithStringParam);
        assertEquals("FIPS-140-3", FipsBuildParams.getFipsMode());

        FipsBuildParams.init(cryptoEntryFnWithBooleanParam);
        assertEquals("FIPS-140-3", FipsBuildParams.getFipsMode());

        FipsBuildParams.init(param -> "FIPS-140-2");
        assertEquals("any-supported", FipsBuildParams.getFipsMode());

        FipsBuildParams.init(param -> null);
        assertEquals("any-supported", FipsBuildParams.getFipsMode());
    }

    final Function<String, Object> cryptoEntryFnWithStringParam = param -> {
        if (param.equals(FipsBuildParams.FIPS_BUILD_PARAM)) {
            return "FIPS-140-3";
        } else if (param.equals(FipsBuildParams.FIPS_BUILD_PARAM_FOR_TESTS)) {
            return null;
        }
        throw new IllegalArgumentException("Unknown parameter: " + param);
    };

    final Function<String, Object> cryptoEntryFnWithBooleanParam = param -> {
        if (param.equals(FipsBuildParams.FIPS_BUILD_PARAM)) {
            return null;
        }
        if (param.equals(FipsBuildParams.FIPS_BUILD_PARAM_FOR_TESTS)) {
            return "true";
        }
        throw new IllegalArgumentException("Unknown parameter: " + param);
    };
}
