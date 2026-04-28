/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import java.security.Security;

/**
 * Provides additional control over declared security providers in 'java.security' file.
 */
final class SecurityProviderManager {

    public static final String SUN_JCE = "SunJCE";

    private SecurityProviderManager() {}

    /**
     * Removes the SunJCE provider from the list of installed security providers. This method is intended to be used when running
     * in a FIPS JVM and when the security file specifies additional configuration, instead of a complete replacement.
     */
    public static void removeNonCompliantFipsProviders() {
        Security.removeProvider(SUN_JCE);
    }

    /**
     * Returns the position at which the provider is found by its name, otherwise returns -1.
     * Provider's position starts by 1 and will not always represent the configured value at 'java.security' file.
     */
    public static int getPosition(String providerName) {
        var provider = java.security.Security.getProvider(providerName);
        if (provider != null) {
            var providers = java.security.Security.getProviders();
            for (int i = 0; i < providers.length; i++) {
                if (providers[i].getName().equals(providerName)) {
                    return i + 1; // provider positions start at 1
                }
            }
        }
        return -1;
    }
}
