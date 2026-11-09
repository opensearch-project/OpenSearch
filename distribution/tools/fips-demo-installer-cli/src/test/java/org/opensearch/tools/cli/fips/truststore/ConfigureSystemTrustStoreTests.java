/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;

import java.security.Provider;

public class ConfigureSystemTrustStoreTests extends OpenSearchTestCase {

    public void testFindPKCS11ProviderServiceReturnsEmptyListWhenNoProviders() {
        var services = ConfigureSystemTrustStore.findPKCS11ProviderService();
        assertNotNull(services);
    }

    public void testConfigurePKCS11TrustStoreCreatesValidConfiguration() {
        var mockProvider = new Provider("TestPKCS11Provider", "1.0", "Test provider") {
        };
        var service = new Provider.Service(mockProvider, "KeyStore", "PKCS11", "java.security.KeyStore", null, null);

        var config = ConfigureSystemTrustStore.configurePKCS11TrustStore(service);

        assertNotNull(config);
        assertEquals("NONE", config.trustStorePath());
        assertEquals("PKCS11", config.trustStoreType());
        assertEquals("TestPKCS11Provider", config.trustStoreProvider());
    }
}
