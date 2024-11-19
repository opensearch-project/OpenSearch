/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import java.security.Security;

public class SecurityProviderManager {

    private static final String SUN_JCE = "SunJCE";

    private SecurityProviderManager() {
        // singleton constructor
    }

    /**
     * Removes the SunJCE provider from the list of security providers. This method is intended to be used when running in a FIPS JVM.
     * It serves as a workaround for the lack of a mechanism in Java to control which providers can or cannot be implicitly instantiated.
     */
    public static void excludeSunJCE() {
        Security.removeProvider(SUN_JCE);
    }
}
