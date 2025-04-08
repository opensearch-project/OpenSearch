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

    public static final String FIPS_BUILD_PARAM = "crypto.standard";

    private static String fipsMode;

    public static void init(Function<String, Object> fipsValue) {
        fipsMode = (String) fipsValue.apply(FIPS_BUILD_PARAM);
        fipsMode = fipsMode == null ? "any-supported" : fipsMode;
    }

    private FipsBuildParams() {}

    public static boolean isInFipsMode() {
        return "FIPS-140-3".equals(fipsMode);
    }

    public static String getFipsMode() {
        return fipsMode;
    }

}
