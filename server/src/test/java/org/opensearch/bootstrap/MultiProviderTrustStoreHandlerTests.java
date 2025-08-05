/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.fips.FipsMode;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.nio.file.Path;
import java.security.KeyStore;

public class MultiProviderTrustStoreHandlerTests extends OpenSearchTestCase {

    private static final Path JAVA_HOME = Path.of(System.getProperty("java.home"));

    @BeforeClass
    public static void beforeClass() throws Exception {
        assumeTrue("Test should run in FIPS JVM", FipsMode.CHECK.isFipsEnabled());
    }

    @Before
    @SuppressForbidden(reason = "clears system properties for clean setup")
    public void setup() throws Exception {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
        System.clearProperty("javax.net.ssl.trustStoreType");
        System.clearProperty("javax.net.ssl.trustStoreProvider");
    }

    @SuppressForbidden(reason = "set system properties for TrustStore configuration")
    public void testPredefinedTrustStoreIsRespected() throws Exception {
        // given
        var trust = createTempFile("trust", ".bcfks");
        System.setProperty("javax.net.ssl.trustStore", trust.toString());

        // when
        MultiProviderTrustStoreHandler.configureTrustStore(createTempDir(), JAVA_HOME);

        // then
        assertEquals(System.getProperty("javax.net.ssl.trustStore"), trust.toString());
        assertNull(System.getProperty("javax.net.ssl.trustStorePassword"));
        assertNull(System.getProperty("javax.net.ssl.trustStoreType"));
        assertNull(System.getProperty("javax.net.ssl.trustStoreProvider"));
    }

    public void testPKCS11TrustStoreSetup() throws Exception {
        // given
        var pkcs11ProviderService = MultiProviderTrustStoreHandler.findPKCS11ProviderService();
        assumeTrue("Should only run when PKCS11 provider is installed.", pkcs11ProviderService.isPresent());
        var trustStoreProviderName = pkcs11ProviderService.get().getProvider().getName();

        // when
        MultiProviderTrustStoreHandler.configureTrustStore(createTempDir(), JAVA_HOME);

        // then
        assertEquals("NONE", System.getProperty("javax.net.ssl.trustStore"));
        assertEquals("", System.getProperty("javax.net.ssl.trustStorePassword"));
        assertEquals("PKCS11", System.getProperty("javax.net.ssl.trustStoreType"));
        assertEquals(trustStoreProviderName, System.getProperty("javax.net.ssl.trustStoreProvider"));
        assertTrue(System.getProperty("javax.net.ssl.trustStorePassword").isEmpty());
    }

    public void testBCFKSTrustStoreCreation() throws Exception {
        // given
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null); // Should use included truststore shipped by JVM
        var totalDefaultCerts = ((X509TrustManager) tmf.getTrustManagers()[0]).getAcceptedIssuers();
        assumeTrue("JVMs truststore cannot be empty", totalDefaultCerts.length > 0);
        var pkcs11ProviderService = MultiProviderTrustStoreHandler.findPKCS11ProviderService();
        assumeFalse(
            "Should only run when NO PKCS11 provider is installed, otherwise it would be prioritised.",
            pkcs11ProviderService.isPresent()
        );
        var tmpDir = createTempDir();

        // when
        MultiProviderTrustStoreHandler.configureTrustStore(tmpDir, JAVA_HOME);

        // then
        assertTrue(System.getProperty("javax.net.ssl.trustStore").endsWith(".bcfks"));
        assertEquals("changeit", System.getProperty("javax.net.ssl.trustStorePassword"));
        assertEquals("BCFKS", System.getProperty("javax.net.ssl.trustStoreType"));
        assertEquals("BCFIPS", System.getProperty("javax.net.ssl.trustStoreProvider"));

        tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null); // Should use configured truststore
        var trustManagers = tmf.getTrustManagers();
        assertTrue(trustManagers.length > 0);
        assertTrue(trustManagers[0] instanceof X509TrustManager);

        var totalMigratedCerts = ((X509TrustManager) trustManagers[0]).getAcceptedIssuers();
        assertEquals(
            "number of accepted totalMigratedCerts matches the JVMs default truststore",
            totalDefaultCerts.length,
            totalMigratedCerts.length
        );
    }
}
