/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fips;

/**
 * This configuration determines the keystore type, file extension, JCA (Java Cryptography Architecture)
 * provider, and JSSE (Java Secure Socket Extension) provider based on whether a FIPS-compliant mode is enabled.
 */
record FipsConfig(String keyStoreType, String fileExtension, String jcaProvider, String jsseProvider) {
    static FipsConfig detect() {
        boolean fips = FipsMode.CHECK.isFipsEnabled();
        return new FipsConfig(
            fips ? "BCFKS" : "JKS", //
            fips ? ".bcfks" : ".jks", //
            fips ? "BCFIPS" : "SUN", //
            fips ? "BCJSSE" : "SunJSSE" //
        );
    }
}
