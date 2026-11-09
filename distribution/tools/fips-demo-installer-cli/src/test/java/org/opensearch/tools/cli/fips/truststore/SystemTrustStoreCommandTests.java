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
import java.util.concurrent.Callable;

public class SystemTrustStoreCommandTests extends FipsTrustStoreCommandTestCase {

    public void testCommand() throws Exception {
        // given
        var mockProvider = new Provider("PKCS11-Mock", "1.0", "Mock PKCS11 Provider") {
            @Override
            public String toString() {
                return "MockPKCS11Provider[" + getName() + "]";
            }
        };

        try {
            mockProvider.put("KeyStore.PKCS11", "com.example.MockPKCS11KeyStore");
            mockProvider.put("KeyStore.PKCS11 KeySize", "1024|2048");
            Security.addProvider(mockProvider);

            // when
            int exitCode = commandLine.execute("--non-interactive");

            // then
            assertEquals(0, exitCode);
            var output = outputCapture.toString();
            assertTrue(errorCapture.toString().isEmpty());
            assertTrue(output.contains("Trust Store Configuration"));
            assertTrue(output.contains("javax.net.ssl.trustStoreType: PKCS11"));
        } finally {
            Security.removeProvider(mockProvider.getName());
        }
    }

    public void testCommandWithPreselectedProvider() throws Exception {
        // given
        var mockProvider = new Provider("PKCS11-Mock", "1.0", "Mock PKCS11 Provider") {
            @Override
            public String toString() {
                return "MockPKCS11Provider[" + getName() + "]";
            }
        };

        try {
            mockProvider.put("KeyStore.PKCS11", "com.example.MockPKCS11KeyStore");
            mockProvider.put("KeyStore.PKCS11 KeySize", "1024|2048");
            Security.addProvider(mockProvider);

            // when
            int exitCode = commandLine.execute("--non-interactive", "--pkcs11-provider", "PKCS11-Mock");

            // then
            assertEquals(0, exitCode);
            assertTrue(errorCapture.toString().isEmpty());
            var output = outputCapture.toString();
            assertTrue(output.contains("Trust Store Configuration"));
            assertTrue(output.contains("javax.net.ssl.trustStoreType: PKCS11"));
            assertTrue(output.contains("javax.net.ssl.trustStoreProvider: PKCS11-Mock"));
        } finally {
            Security.removeProvider(mockProvider.getName());
        }
    }

    @Override
    Callable<Integer> getCut() {
        return new SystemTrustStoreCommand();
    }
}
