/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.info;

import java.util.function.Function;

public class FipsBuildParams {

    @Deprecated
    public static final String FIPS_BUILD_PARAM_FOR_TESTS = "tests.fips.enabled";
    public static final String FIPS_BUILD_PARAM = "crypto.standard";
    public static final String DEFAULT_FIPS_MODE = "FIPS-140-3";

    private static String fipsMode;

    public static void init(Function<String, Object> fipsValue) {
        var fipsBuildParamForTests = Boolean.parseBoolean((String) fipsValue.apply(FIPS_BUILD_PARAM_FOR_TESTS));
        var fipsBuildParam = (String) fipsValue.apply(FIPS_BUILD_PARAM);

        if (fipsBuildParamForTests || DEFAULT_FIPS_MODE.equals(fipsBuildParam)) {
            fipsMode = DEFAULT_FIPS_MODE;
        } else {
            fipsMode = "any-supported";
        }
    }

    private FipsBuildParams() {}

    public static boolean isInFipsMode() {
        return DEFAULT_FIPS_MODE.equals(fipsMode);
    }

    public static String getFipsMode() {
        return fipsMode;
    }

}
