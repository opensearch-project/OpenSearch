/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.security.Provider;
import java.security.Security;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.opensearch.tools.cli.fips.truststore.ConfigurationProperties.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;

/**
 * The ConfigureSystemTrustStore class provides methods to interact with and configure the system
 * trust store for PKCS#11 providers. It supports discovering PKCS#11 KeyStore providers and
 * encapsulates the process of setting up a PKCS#11-based trust store for enhanced security
 * configurations.
 */
public class ConfigureSystemTrustStore {

    private static final String PKCS_11 = "PKCS11";
    private static final String TRUST_STORE_PASSWORD = Security.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
    private static final String SYSTEMSTORE_PASSWORD = Objects.requireNonNullElse(TRUST_STORE_PASSWORD, "");

    public static List<Provider.Service> findPKCS11ProviderService() {
        return Arrays.stream(Security.getProviders())
            .filter(it -> it.getName().toUpperCase(Locale.ROOT).contains(PKCS_11))
            .map(it -> it.getService("KeyStore", PKCS_11))
            .filter(Objects::nonNull)
            .toList();
    }

    public static ConfigurationProperties configurePKCS11TrustStore(Provider.Service pkcs11ProviderService) {
        return new ConfigurationProperties("NONE", PKCS_11, SYSTEMSTORE_PASSWORD, pkcs11ProviderService.getProvider().getName());
    }
}
