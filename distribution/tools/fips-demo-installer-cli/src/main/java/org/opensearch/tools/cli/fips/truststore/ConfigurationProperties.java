/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.util.Locale;

/**
 * Encapsulates properties related to a TrustStore configuration. Is primarily used to manage and
 * log details of TrustStore configurations within the system.
 *
 * @param trustStorePath path to the trust store file
 * @param trustStoreType type of the trust store (e.g., BCFKS, PKCS11)
 * @param trustStorePassword password for the trust store
 * @param trustStoreProvider security provider name for the trust store
 */
record ConfigurationProperties(String trustStorePath, String trustStoreType, String trustStorePassword, String trustStoreProvider) {

    public static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    public static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    public static final String JAVAX_NET_SSL_TRUST_STORE_PROVIDER = "javax.net.ssl.trustStoreProvider";
    public static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    @Override
    public String toString() {
        return String.format(
            Locale.ROOT,
            """
                %s: %s
                %s: %s
                %s: %s
                %s: %s""",
            JAVAX_NET_SSL_TRUST_STORE,
            trustStorePath,
            JAVAX_NET_SSL_TRUST_STORE_TYPE,
            trustStoreType,
            JAVAX_NET_SSL_TRUST_STORE_PROVIDER,
            trustStoreProvider,
            JAVAX_NET_SSL_TRUST_STORE_PASSWORD,
            trustStorePassword.isEmpty() ? "[NOT SET]" : "[SET]"
        );
    }
}
