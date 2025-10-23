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
 * trust store for PKCS#11 providers.
 */
public class ConfigureSystemTrustStore {

    private static final String PKCS_11 = "PKCS11";
    private static final String TRUST_STORE_PASSWORD = Security.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD);
    private static final String SYSTEMSTORE_PASSWORD = Objects.requireNonNullElse(TRUST_STORE_PASSWORD, "");

    /**
     * Finds all available PKCS11 KeyStore provider services in the security environment.
     *
     * @return list of PKCS11 KeyStore provider services
     */
    public static List<Provider.Service> findPKCS11ProviderService() {
        return Arrays.stream(Security.getProviders())
            .filter(it -> it.getName().toUpperCase(Locale.ROOT).contains(PKCS_11))
            .map(it -> it.getService("KeyStore", PKCS_11))
            .filter(Objects::nonNull)
            .toList();
    }

    /**
     * Creates configuration properties for a PKCS11-based trust store.
     *
     * @param pkcs11ProviderService the PKCS11 provider service to configure
     * @return configuration properties for the PKCS11 trust store
     */
    public static ConfigurationProperties configurePKCS11TrustStore(Provider.Service pkcs11ProviderService) {
        return new ConfigurationProperties("NONE", PKCS_11, SYSTEMSTORE_PASSWORD, pkcs11ProviderService.getProvider().getName());
    }
}
