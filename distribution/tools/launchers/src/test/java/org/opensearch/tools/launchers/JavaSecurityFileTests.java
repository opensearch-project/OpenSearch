/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.launchers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Properties;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class JavaSecurityFileTests extends LaunchersTestCase {

    private static final String JDK_DISABLED_TLS = "SSLv3, TLSv1, TLSv1.1, RC4, DES, MD5withRSA";

    public void testFipsModeReplacesJdkProvidersWithFipsFileProviders() {
        final Properties fips = properties(
            "security.provider.1",
            JavaSecurityFile.BC_FIPS_PROVIDER_ENTRY,
            "security.provider.2",
            "org.bouncycastle.jsse.provider.BouncyCastleJsseProvider fips:BCFIPS",
            "security.provider.3",
            "SUN"
        );

        final Properties result = JavaSecurityFile.forFipsMode(linuxJdk(), fips);

        assertThat(result.getProperty("security.provider.1"), equalTo(JavaSecurityFile.BC_FIPS_PROVIDER_ENTRY));
        assertThat(result.getProperty("security.provider.3"), equalTo("SUN"));
        assertThat("JDK providers after the FIPS list must be gone", result.getProperty("security.provider.4"), nullValue());
        assertThat(JavaSecurityFile.verifiedProviderCount(result), equalTo(3));
    }

    public void testFipsModeKeepsJdkDefaultsAndAppliesFipsFileOverrides() {
        final Properties fips = properties(
            "security.provider.1",
            JavaSecurityFile.BC_FIPS_PROVIDER_ENTRY,
            "ssl.KeyManagerFactory.algorithm",
            "PKIX"
        );

        final Properties result = JavaSecurityFile.forFipsMode(linuxJdk(), fips);

        assertThat(result.getProperty("jdk.tls.disabledAlgorithms"), equalTo(JDK_DISABLED_TLS));
        assertThat(result.getProperty("ssl.KeyManagerFactory.algorithm"), equalTo("PKIX"));
    }

    public void testFipsModeRequiresAProvider() {
        final Properties fips = properties("ssl.KeyManagerFactory.algorithm", "PKIX");

        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> JavaSecurityFile.forFipsMode(linuxJdk(), fips));

        assertThat(e.getMessage(), containsString("security.provider.1"));
    }

    public void testFipsModeRejectsAProviderAfterAGap() {
        final Properties fips = properties("security.provider.1", JavaSecurityFile.BC_FIPS_PROVIDER_ENTRY, "security.provider.3", "SUN");

        final IllegalStateException e = assertThrows(IllegalStateException.class, () -> JavaSecurityFile.forFipsMode(linuxJdk(), fips));

        assertThat(e.getMessage(), containsString("[security.provider.3] would be ignored"));
    }

    public void testDefaultModeAppendsBcFipsAfterJdkProviders() {
        final Properties result = JavaSecurityFile.forDefaultMode(linuxJdk(), true);

        assertThat(result.getProperty("security.provider.1"), equalTo("SUN"));
        assertThat(result.getProperty("security.provider.12"), equalTo("SunPKCS11"));
        assertThat(result.getProperty("security.provider.13"), equalTo(JavaSecurityFile.BC_FIPS_PROVIDER_ENTRY));
        assertThat(result.getProperty("jdk.tls.disabledAlgorithms"), equalTo(JDK_DISABLED_TLS));
    }

    public void testDefaultModeUsesTheNextFreeSlotOfALongerJdkList() {
        // Windows and macOS JDKs declare one provider more (SunMSCAPI, Apple), which moves SunPKCS11 to slot 13
        final Properties windowsJdk = linuxJdk();
        windowsJdk.setProperty("security.provider.12", "SunMSCAPI");
        windowsJdk.setProperty("security.provider.13", "SunPKCS11");

        final Properties result = JavaSecurityFile.forDefaultMode(windowsJdk, true);

        assertThat(result.getProperty("security.provider.13"), equalTo("SunPKCS11"));
        assertThat(result.getProperty("security.provider.14"), equalTo(JavaSecurityFile.BC_FIPS_PROVIDER_ENTRY));
    }

    public void testDefaultModeDoesNotDeclareBcFipsTwice() {
        final Properties jdk = linuxJdk();
        jdk.setProperty("security.provider.13", JavaSecurityFile.BC_FIPS_PROVIDER_NAME);

        final Properties result = JavaSecurityFile.forDefaultMode(jdk, true);

        assertThat(result.getProperty("security.provider.14"), nullValue());
    }

    public void testDefaultModeWithoutBcFipsOnTheClasspathKeepsTheJdkList() {
        final Properties result = JavaSecurityFile.forDefaultMode(linuxJdk(), false);

        assertThat(result, equalTo(linuxJdk()));
    }

    public void testRenderedFileLoadsBackToTheSameProperties() throws IOException {
        final Properties expected = JavaSecurityFile.forDefaultMode(linuxJdk(), true);
        expected.setProperty("needs.escaping", " leading space, colon: equals= hash# backslash\\ umlautä \n newline");

        final Properties loaded = new Properties();
        loaded.load(new ByteArrayInputStream(JavaSecurityFile.render(expected, "test")));

        assertThat(loaded, equalTo(expected));
    }

    public void testRenderingIsStableAndListsProvidersInSlotOrder() {
        final Properties properties = JavaSecurityFile.forDefaultMode(linuxJdk(), true);

        final byte[] first = JavaSecurityFile.render(properties, "test");
        final byte[] second = JavaSecurityFile.render(properties, "test");
        final String text = new String(first, StandardCharsets.ISO_8859_1);

        assertArrayEquals(first, second);
        assertTrue(text.indexOf("security.provider.2=") < text.indexOf("security.provider.10="));
        assertTrue(text.indexOf("security.provider.13=") < text.indexOf("jdk.tls.disabledAlgorithms="));
    }

    public void testUserConfiguredSecurityPropertiesSkipGeneration() {
        assertTrue(JavaSecurityFile.isConfiguredByUser("-Xmx1g -Djava.security.properties=/etc/custom.security"));
        assertTrue(JavaSecurityFile.isConfiguredByUser("-Djava.security.properties==/etc/custom.security"));
        assertFalse(JavaSecurityFile.isConfiguredByUser("-Xmx1g -Djava.security.debug=all"));
        assertFalse(JavaSecurityFile.isConfiguredByUser(null));
    }

    public void testWriteReusesAFileWithTheSameContent() throws IOException {
        final Path directory = newTempDir();
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");

        final Path first = JavaSecurityFile.writeIfChanged(directory, content);
        final long modified = Files.getLastModifiedTime(first).toMillis();
        final Path second = JavaSecurityFile.writeIfChanged(directory, content);

        assertThat(second, equalTo(first));
        assertThat(Files.getLastModifiedTime(second).toMillis(), equalTo(modified));
        assertArrayEquals(content, Files.readAllBytes(first));
    }

    public void testWriteUsesADifferentFileForDifferentContent() throws IOException {
        final Path directory = newTempDir();

        final Path linux = JavaSecurityFile.writeIfChanged(directory, JavaSecurityFile.render(linuxJdk(), "test"));
        final Path withBcFips = JavaSecurityFile.writeIfChanged(
            directory,
            JavaSecurityFile.render(JavaSecurityFile.forDefaultMode(linuxJdk(), true), "test")
        );

        assertNotEquals(linux, withBcFips);
    }

    public void testWriteReplacesATamperedFile() throws IOException {
        final Path directory = newTempDir();
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");
        final Path file = JavaSecurityFile.writeIfChanged(directory, content);
        Files.write(file, "security.provider.1=Evil".getBytes(StandardCharsets.ISO_8859_1));

        final Path rewritten = JavaSecurityFile.writeIfChanged(directory, content);

        assertThat(rewritten, equalTo(file));
        assertArrayEquals(content, Files.readAllBytes(file));
    }

    public void testPrivateDirectoryIsCreatedForTheOwnerOnly() throws IOException {
        assumePosix();
        final Path base = newTempDir();

        final Path directory = JavaSecurityFile.privateDirectory(base);

        assertThat(directory.getParent(), equalTo(base));
        assertThat(directory.getFileName().toString(), not(containsString("probe")));
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(directory)), equalTo("rwx------"));
        try (Stream<Path> entries = Files.list(base)) {
            assertThat("the probe file must be removed", entries.count(), equalTo(1L));
        }
        assertThat(JavaSecurityFile.privateDirectory(base), equalTo(directory));
    }

    public void testPrivateDirectoryAccessibleByOthersIsRejected() throws IOException {
        assumePosix();
        final Path base = newTempDir();
        final Path directory = JavaSecurityFile.privateDirectory(base);
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxrwxr-x"));

        final IOException e = assertThrows(IOException.class, () -> JavaSecurityFile.privateDirectory(base));

        assertThat(e.getMessage(), containsString("must only be accessible by its owner"));
    }

    public void testPrivateDirectoryThatIsASymbolicLinkIsRejected() throws IOException {
        assumePosix();
        final Path base = newTempDir();
        final Path directory = JavaSecurityFile.privateDirectory(base);
        final Path elsewhere = Files.createDirectory(base.resolve("elsewhere"));
        Files.delete(directory);
        Files.createSymbolicLink(directory, elsewhere);

        final IOException e = assertThrows(IOException.class, () -> JavaSecurityFile.privateDirectory(base));

        assertThat(e.getMessage(), containsString("must be a directory"));
    }

    private static void assumePosix() {
        assumeTrue("needs POSIX file permissions", FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    }

    /**
     * The provider list and one other default of a Linux JDK 21 {@code java.security}.
     */
    private static Properties linuxJdk() {
        return properties(
            "security.provider.1",
            "SUN",
            "security.provider.2",
            "SunRsaSign",
            "security.provider.3",
            "SunEC",
            "security.provider.4",
            "SunJSSE",
            "security.provider.5",
            "SunJCE",
            "security.provider.6",
            "SunJGSS",
            "security.provider.7",
            "SunSASL",
            "security.provider.8",
            "XMLDSig",
            "security.provider.9",
            "SunPCSC",
            "security.provider.10",
            "JdkLDAP",
            "security.provider.11",
            "JdkSASL",
            "security.provider.12",
            "SunPKCS11",
            "jdk.tls.disabledAlgorithms",
            JDK_DISABLED_TLS
        );
    }

    private static Properties properties(final String... keysAndValues) {
        final Properties properties = new Properties();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            properties.setProperty(keysAndValues[i], keysAndValues[i + 1]);
        }
        return properties;
    }
}
