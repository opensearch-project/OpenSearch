/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.util;

import org.opensearch.gradle.test.GradleUnitTestCase;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class TestJavaSecurityFileTests extends GradleUnitTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testFipsReplacesJdkProvidersAndKeepsOtherDefaults() {
        final Properties fips = new Properties();
        fips.setProperty("security.provider.1", TestJavaSecurityFile.BC_FIPS_PROVIDER_ENTRY);
        fips.setProperty("ssl.KeyManagerFactory.algorithm", "PKIX");

        final Properties result = TestJavaSecurityFile.forFips(jdk(), fips);

        assertEquals(TestJavaSecurityFile.BC_FIPS_PROVIDER_ENTRY, result.getProperty("security.provider.1"));
        assertNull(result.getProperty("security.provider.2"));
        assertEquals("SSLv3, TLSv1", result.getProperty("jdk.tls.disabledAlgorithms"));
        assertEquals("PKIX", result.getProperty("ssl.KeyManagerFactory.algorithm"));
    }

    /**
     * The build tools carry the distribution's fips_java.security, so plugins built outside this repository get its providers.
     */
    public void testBundledFipsFileStartsWithTheBouncyCastleFipsProviders() throws IOException {
        final Properties bundled = new Properties();
        bundled.load(new StringReader(TestJavaSecurityFile.bundledFipsJavaSecurity()));

        final Properties result = TestJavaSecurityFile.forFips(jdk(), bundled);

        assertEquals(TestJavaSecurityFile.BC_FIPS_PROVIDER_ENTRY, result.getProperty("security.provider.1"));
        assertTrue(result.getProperty("security.provider.2").startsWith("org.bouncycastle.jsse.provider.BouncyCastleJsseProvider"));
        assertEquals("SSLv3, TLSv1", result.getProperty("jdk.tls.disabledAlgorithms"));
    }

    public void testFipsTrustStoreIsTheBundledResource() throws IOException {
        final Path target = temporaryFolder.getRoot().toPath().resolve("java-security").resolve("test-truststore.bcfks");

        TestJavaSecurityFile.writeFipsTrustStore(target);

        final byte[] bundled;
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(TestJavaSecurityFile.BUNDLED_FIPS_TRUST_STORE_RESOURCE)) {
            bundled = in.readAllBytes();
        }
        assertTrue(bundled.length > 0);
        assertArrayEquals(bundled, Files.readAllBytes(target));
    }

    public void testFipsFileWithoutProvidersIsRejected() {
        expectThrows(IllegalStateException.class, () -> TestJavaSecurityFile.forFips(jdk(), new Properties()));
    }

    public void testDefaultAppendsBcFipsAfterJdkProviders() {
        final Properties result = TestJavaSecurityFile.forDefault(jdk());

        assertEquals("SUN", result.getProperty("security.provider.1"));
        assertEquals("SunJCE", result.getProperty("security.provider.2"));
        assertEquals(TestJavaSecurityFile.BC_FIPS_PROVIDER_ENTRY, result.getProperty("security.provider.3"));
        assertEquals("SSLv3, TLSv1", result.getProperty("jdk.tls.disabledAlgorithms"));
    }

    private static Properties jdk() {
        final Properties jdk = new Properties();
        jdk.setProperty("security.provider.1", "SUN");
        jdk.setProperty("security.provider.2", "SunJCE");
        jdk.setProperty("jdk.tls.disabledAlgorithms", "SSLv3, TLSv1");
        jdk.setProperty("ssl.KeyManagerFactory.algorithm", "SunX509");
        return jdk;
    }
}
