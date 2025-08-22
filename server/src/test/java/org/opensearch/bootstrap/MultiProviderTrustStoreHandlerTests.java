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
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.PublicKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

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
        var throwable = assertThrows(
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
        var mockProvider = new Provider("PKCS11-Mock", "1.0", "Mock PKCS11 Provider") {
            @Override
            public String toString() {
                return "MockPKCS11Provider[" + getName() + "]";
            }
        };

        mockProvider.put("KeyStore.PKCS11", "com.example.MockPKCS11KeyStore");
        mockProvider.put("KeyStore.PKCS11 KeySize", "1024|2048");
        Security.addProvider(mockProvider);
        try {
            var pkcs11ProviderService = MultiProviderTrustStoreHandler.findPKCS11ProviderService();
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
        } finally {
            Security.removeProvider(mockProvider.getName());
        }
    }

    public void testUnconfiguredPKCS11TrustStoreSetup() throws Exception {
        assumeFalse(
            "Should only run when PKCS11 provider is NOT installed.",
            MultiProviderTrustStoreHandler.findPKCS11ProviderService().isPresent()
        );
        var settings = Settings.builder().put("cluster.fips.truststore.source", "SYSTEM_TRUST").build();
        var throwable = assertThrows(
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

    public void testReadFileWithPrivilege_withNonExistentFile() {
        var nonExistentFile = createTempDir().resolve("nonExistentFile");

        var throwable = assertThrows(RuntimeException.class, () -> MultiProviderTrustStoreHandler.readFileWithPrivilege(nonExistentFile));

        assertNotNull(throwable.getCause());
        assertTrue(throwable.getCause() instanceof IOException);
        assertTrue(throwable.getCause().getMessage().contains(nonExistentFile.toString()));
    }

    public void testBCFKSTrustStoreCreation_withoutWritePermission() throws Exception {
        assumeTrue("requires POSIX file permissions", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        var noWriteDir = createTempDir().resolve("no-write");
        Files.createDirectory(noWriteDir);
        Files.setPosixFilePermissions(noWriteDir, PosixFilePermissions.fromString("r-xr-xr-x"));

        try {
            var throwable = assertThrows(
                IllegalStateException.class,
                () -> MultiProviderTrustStoreHandler.convertToBCFKS(
                    MultiProviderTrustStoreHandler.loadJvmDefaultTrustStore(JAVA_HOME),
                    noWriteDir
                )
            );
            assertTrue(throwable.getMessage().startsWith("Could not create temporary truststore file in [%s]".formatted(noWriteDir)));
            assertTrue(throwable.getCause() instanceof AccessDeniedException);
        } finally {
            Files.setPosixFilePermissions(noWriteDir, PosixFilePermissions.fromString("rwxrwxrwx"));
        }
    }

    public void testWriteBCFKSKeyStoreToFile_withoutWritePermission() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());
        assumeTrue("requires POSIX file permissions", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        var tempBcfksFile = Files.createTempFile(createTempDir(), "no-write", ".bcfks");
        Files.setPosixFilePermissions(tempBcfksFile, PosixFilePermissions.fromString("r-xr-xr-x"));

        try {
            var keyStore = KeyStore.getInstance("BCFKS", "BCFIPS");
            keyStore.load(null, "changeit".toCharArray());

            var throwable = assertThrows(
                IllegalStateException.class,
                () -> MultiProviderTrustStoreHandler.writeBCFKSKeyStoreToFile(keyStore, tempBcfksFile)
            );
            assertTrue(throwable.getMessage().startsWith("Failed to write BCFKS keystore to [%s]".formatted(tempBcfksFile)));
            assertTrue(throwable.getCause() instanceof AccessDeniedException);
        } finally {
            Files.setPosixFilePermissions(tempBcfksFile, PosixFilePermissions.fromString("rwxrwxrwx"));
        }
    }

    public void testBCFKSTrustStoreCreation_withoutBcfipsProvider() throws Exception {
        // given
        var bcfipsProvider = Security.getProvider("BCFIPS");
        var bcfipsProviderPosition = -1;
        if (bcfipsProvider != null) {
            var providers = Security.getProviders();
            for (int i = 0; i < providers.length; i++) {
                if (providers[i].equals(bcfipsProvider)) {
                    bcfipsProviderPosition = i + 1; // Position is 1-based
                }
            }

            Security.removeProvider("BCFIPS");
        }

        try {
            // when & then
            var throwable = assertThrows(
                SecurityException.class,
                () -> MultiProviderTrustStoreHandler.convertToBCFKS(
                    MultiProviderTrustStoreHandler.loadJvmDefaultTrustStore(JAVA_HOME),
                    createTempDir()
                )
            );

            assertTrue(throwable.getCause() instanceof NoSuchProviderException);
            assertEquals("no such provider: BCFIPS", throwable.getCause().getMessage());
        } finally {
            if (bcfipsProvider != null) {
                Security.insertProviderAt(bcfipsProvider, bcfipsProviderPosition);
            }
        }
    }

    public void testBCFKSTrustSToreCreation_withWeakCert() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());
        // given
        var testKeyStore = KeyStore.getInstance("JKS");
        testKeyStore.load(null, null);

        var rsa1024Cert = loadCertificateFromResource("test-certs/cert1024.pem");
        testKeyStore.setCertificateEntry("rsa1024-weak", rsa1024Cert);

        var md5Cert = loadCertificateFromResource("test-certs/certmd5.pem");
        testKeyStore.setCertificateEntry("md5-weak", md5Cert);

        var weakCurveCert = loadCertificateFromResource("test-certs/certecc.pem");
        testKeyStore.setCertificateEntry("weak-curve", weakCurveCert);

        assertEquals(3, testKeyStore.size());

        // when
        var result = MultiProviderTrustStoreHandler.convertToBCFKS(testKeyStore, createTempDir());

        // then
        var bcfks = KeyStore.getInstance("BCFKS", "BCFIPS");
        try (var input = Files.newInputStream(result)) {
            bcfks.load(input, "changeit".toCharArray());
            assertEquals("Conversion results: %d/%d certificates converted%n", 3, bcfks.size());
        }
    }

    public void testBCFKSTrustStoreCreation_withProblematicCertificates() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());
        // given
        var testKeyStore = KeyStore.getInstance("JKS");
        testKeyStore.load(null, null);
        var encodingFailureCertificate = new Certificate("X.509") {
            @Override
            public byte[] getEncoded() {
                throw new RuntimeException("Encoding failure - certificate data corrupted");
            }

            @Override
            public void verify(PublicKey key) {}

            @Override
            public void verify(PublicKey key, String sigProvider) {}

            @Override
            public PublicKey getPublicKey() {
                throw new RuntimeException("Public key extraction failed");
            }

            @Override
            public String toString() {
                return "CorruptedCertificate[exception while encoding]";
            }
        };

        var nullEncodingCertificate = new Certificate("X.509") {
            @Override
            public byte[] getEncoded() {
                return null;
            }

            @Override
            public void verify(PublicKey key) {}

            public void verify(PublicKey key, String sigProvider) {}

            @Override
            public PublicKey getPublicKey() {
                return null;
            }

            @Override
            public String toString() {
                return "CorruptedCertificate[null encoding]";
            }
        };

        testKeyStore.setCertificateEntry("encoding-fail", encodingFailureCertificate);
        testKeyStore.setCertificateEntry("null-encoding", nullEncodingCertificate);

        assertEquals(2, testKeyStore.size());

        // when & then
        var result = MultiProviderTrustStoreHandler.convertToBCFKS(testKeyStore, createTempDir());

        var bcfks = KeyStore.getInstance("BCFKS", "BCFIPS");
        try (var input = Files.newInputStream(result)) {
            bcfks.load(input, "changeit".toCharArray());
            assertEquals("Some certificates should fail during copy", 0, bcfks.size());
        }
    }

    public void testBCFKSTrustStoreCreation_withEmptyJavaHome() throws Exception {
        var settings = Settings.builder().put("cluster.fips.truststore.source", "GENERATED").build();

        var throwable = assertThrows(
            IllegalStateException.class,
            () -> MultiProviderTrustStoreHandler.configureTrustStore(settings, createTempDir(), createEmptyJavaHomePath())
        );
        assertTrue(throwable.getMessage().startsWith("System cacerts not found at"));
        assertTrue(throwable.getMessage().endsWith("/lib/security/cacerts"));
    }

    public void testBCFKSTrustStoreCreation_withInvalidTrustStore() throws Exception {
        var settings = Settings.builder().put("cluster.fips.truststore.source", "GENERATED").build();

        var throwable = assertThrows(
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

    private Certificate loadCertificateFromResource(String resourcePath) throws Exception {
        var certPath = getDataPath(resourcePath);

        try (var inputStream = Files.newInputStream(certPath)) {
            var cf = CertificateFactory.getInstance("X.509");
            return cf.generateCertificate(inputStream);
        }
    }
}
