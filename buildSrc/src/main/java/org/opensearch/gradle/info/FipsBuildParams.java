/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.info;

public class FipsBuildParams {

    private static final String FIPS_MODE = System.getenv("OPENSEARCH_CRYPTO_STANDARD");

    private FipsBuildParams() {}

    public static boolean isInFipsMode() {
        return "FIPS-140-3".equals(FIPS_MODE);
    }

    public static String getFipsMode() {
        return FIPS_MODE;
    }

}
