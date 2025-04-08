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

    public void testIsInFipsMode() {
        FipsBuildParams.init(cryptoEntryFn);
        assertTrue(FipsBuildParams.isInFipsMode());

        FipsBuildParams.init(param -> null);
        assertFalse(FipsBuildParams.isInFipsMode());
    }

    public void testGetFipsMode() {
        FipsBuildParams.init(cryptoEntryFn);
        assertEquals("FIPS-140-3", FipsBuildParams.getFipsMode());

        FipsBuildParams.init(param -> null);
        assertEquals("any-supported", FipsBuildParams.getFipsMode());
    }

    final Function<String, Object> cryptoEntryFn = param -> {
        if (param.equals(FipsBuildParams.FIPS_BUILD_PARAM)) {
            return "FIPS-140-3";
        }
        throw new IllegalArgumentException("Unknown parameter: " + param);
    };
}
