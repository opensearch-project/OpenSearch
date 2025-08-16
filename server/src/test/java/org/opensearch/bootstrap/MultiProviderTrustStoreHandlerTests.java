/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

public class MultiProviderTrustStoreHandlerTests extends OpenSearchTestCase {

    private static final Path JAVA_HOME = Path.of(System.getProperty("java.home"));

    @Before
    @SuppressForbidden(reason = "clears system properties for clean setup")
    public void setup() throws Exception {
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
        System.clearProperty("javax.net.ssl.trustStoreType");
        System.clearProperty("javax.net.ssl.trustStoreProvider");
    }

    public void testNoConfiguration() throws Exception {
        Throwable throwable = assertThrows(
            IllegalArgumentException.class,
            () -> MultiProviderTrustStoreHandler.configureTrustStore(Settings.builder().build(), createTempDir(), JAVA_HOME)
        );
        assertEquals(
            "In FIPS JVM it is required for default TrustStore to be configured by 'javax.net.ssl.trustStore' "
                + "JVM parameter or by 'cluster.fips.truststore.source' cluster setting",
            throwable.getMessage()
        );
    }

    @SuppressForbidden(reason = "set system properties for TrustStore configuration")
    public void testPredefinedTrustStoreIsRespected() throws Exception {
        // given
        var trust = createTempFile("trust", ".bcfks");
        System.setProperty("javax.net.ssl.trustStore", trust.toString());

        // when
        MultiProviderTrustStoreHandler.configureTrustStore(Settings.builder().build(), createTempDir(), JAVA_HOME);

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
        var settings = Settings.builder().put("cluster.fips.truststore.source", "SYSTEM_TRUST").build();

        // when
        MultiProviderTrustStoreHandler.configureTrustStore(settings, createTempDir(), JAVA_HOME);

        // then
        assertEquals("NONE", System.getProperty("javax.net.ssl.trustStore"));
        assertEquals("", System.getProperty("javax.net.ssl.trustStorePassword"));
        assertEquals("PKCS11", System.getProperty("javax.net.ssl.trustStoreType"));
        assertEquals(trustStoreProviderName, System.getProperty("javax.net.ssl.trustStoreProvider"));
        assertTrue(System.getProperty("javax.net.ssl.trustStorePassword").isEmpty());
    }

    public void testUnconfiguredPKCS11TrustStoreSetup() throws Exception {
        // given
        var pkcs11ProviderService = MultiProviderTrustStoreHandler.findPKCS11ProviderService();
        assumeFalse("Should only run when PKCS11 provider is NOT installed.", pkcs11ProviderService.isPresent());
        var settings = Settings.builder().put("cluster.fips.truststore.source", "SYSTEM_TRUST").build();

        // when & then
        Throwable throwable = assertThrows(
            IllegalStateException.class,
            () -> MultiProviderTrustStoreHandler.configureTrustStore(settings, createTempDir(), JAVA_HOME)
        );
        assertEquals("No PKCS11 provider found, check java.security configuration for FIPS.", throwable.getMessage());
    }

    public void testBCFKSTrustStoreCreation() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());
        // given
        var tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null); // Should use included truststore shipped by JVM
        var totalDefaultCerts = ((X509TrustManager) tmf.getTrustManagers()[0]).getAcceptedIssuers();
        assumeTrue("JVMs truststore cannot be empty", totalDefaultCerts.length > 0);
        var settings = Settings.builder().put("cluster.fips.truststore.source", "GENERATED").build();

        // when
        MultiProviderTrustStoreHandler.configureTrustStore(settings, createTempDir(), JAVA_HOME);

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

    public void testBCFKSTrustStoreCreation_withEmptyJavaHome() throws Exception {
        var settings = Settings.builder().put("cluster.fips.truststore.source", "GENERATED").build();

        Throwable throwable = assertThrows(
            IllegalStateException.class,
            () -> MultiProviderTrustStoreHandler.configureTrustStore(settings, createTempDir(), createEmptyJavaHomePath())
        );
        assertTrue(throwable.getMessage().startsWith("System cacerts not found at"));
        assertTrue(throwable.getMessage().endsWith("/lib/security/cacerts"));
    }

    public void testBCFKSTrustStoreCreation_withInvalidTrustStore() throws Exception {
        var settings = Settings.builder().put("cluster.fips.truststore.source", "GENERATED").build();

        Throwable throwable = assertThrows(
            IllegalStateException.class,
            () -> MultiProviderTrustStoreHandler.configureTrustStore(settings, createTempDir(), createJavaHomePathWithInvalidTrustStore())
        );
        assertEquals("Could not load system cacerts in any known format[PKCS12, JKS]", throwable.getMessage());
    }

    public void testPrintCurrentConfiguration() throws Exception {
        // Setup an in-memory logger
        var output = new StringWriter();
        var layout = PatternLayout.newBuilder().withPattern("%m%n").build();
        var appender = WriterAppender.newBuilder().setTarget(output).setLayout(layout).setName("TestAppender").build();

        appender.start();

        var logger = (Logger) LogManager.getLogger(MultiProviderTrustStoreHandler.class);
        logger.addAppender(appender);
        logger.setLevel(Level.TRACE);

        try {
            MultiProviderTrustStoreHandler.printCurrentConfiguration(Level.TRACE);
            String logOutput = output.toString();
            assertTrue(logOutput.contains("Available Security Providers:"));
            assertTrue(logOutput.contains("KeyStore"));
        } finally {
            logger.removeAppender(appender);
        }
    }

    private Path createJavaHomePathWithInvalidTrustStore() throws Exception {
        var javaHome = createEmptyJavaHomePath();
        var cacertsFile = javaHome.resolve("lib").resolve("security").resolve("cacerts");
        Files.createFile(cacertsFile);
        Files.writeString(cacertsFile, "dummy cacerts content");
        return javaHome;
    }

    private Path createEmptyJavaHomePath() throws Exception {
        var javaHome = createTempDir();
        var libDir = javaHome.resolve("lib");
        var securityDir = libDir.resolve("security");
        Files.createDirectories(securityDir);
        return javaHome;
    }
}
