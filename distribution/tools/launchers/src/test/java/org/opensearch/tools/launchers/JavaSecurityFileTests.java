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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
        final Path file = newTempDir().resolve("java.security");
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");

        JavaSecurityFile.writeIfChanged(file, content);
        final long modified = Files.getLastModifiedTime(file).toMillis();
        final Path second = JavaSecurityFile.writeIfChanged(file, content);

        assertThat(second, equalTo(file));
        assertThat(Files.getLastModifiedTime(file).toMillis(), equalTo(modified));
        assertArrayEquals(content, Files.readAllBytes(file));
    }

    public void testHashedFileNamesDifferOnlyForDifferentContent() {
        final String linux = JavaSecurityFile.hashedFileName(JavaSecurityFile.render(linuxJdk(), "test"));
        final String withBcFips = JavaSecurityFile.hashedFileName(
            JavaSecurityFile.render(JavaSecurityFile.forDefaultMode(linuxJdk(), true), "test")
        );

        assertNotEquals(linux, withBcFips);
        assertThat(JavaSecurityFile.hashedFileName(JavaSecurityFile.render(linuxJdk(), "test")), equalTo(linux));
    }

    public void testWriteReplacesATamperedFile() throws IOException {
        final Path file = newTempDir().resolve("java.security");
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");
        JavaSecurityFile.writeIfChanged(file, content);
        Files.writeString(file, "security.provider.1=Evil", StandardCharsets.ISO_8859_1);

        final Path rewritten = JavaSecurityFile.writeIfChanged(file, content);

        assertThat(rewritten, equalTo(file));
        assertArrayEquals(content, Files.readAllBytes(file));
    }

    public void testWriteReplacesAnUnreadableFile() throws IOException {
        assumePosix();
        final Path file = newTempDir().resolve("java.security");
        Files.writeString(file, "security.provider.1=Other", StandardCharsets.ISO_8859_1);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("---------"));
        assumeTrue("needs a user that file permissions apply to", Files.isReadable(file) == false);
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");

        JavaSecurityFile.writeIfChanged(file, content);

        assertArrayEquals(content, Files.readAllBytes(file));
    }

    public void testConcurrentWritesOfTheSameContentShareOneFile() throws Exception {
        final Path config = newTempDir();
        final Path base = newTempDir();
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");
        final int writers = 8;
        final CyclicBarrier start = new CyclicBarrier(writers);
        final ExecutorService executor = Executors.newFixedThreadPool(writers);
        final List<Future<Path>> results = new ArrayList<>();
        try {
            for (int i = 0; i < writers; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return JavaSecurityFile.write(config, base, content);
                }));
            }
            for (final Future<Path> result : results) {
                assertThat(result.get(30, TimeUnit.SECONDS), equalTo(config.resolve(JavaSecurityFile.CONFIG_FILE_NAME)));
            }
            assertArrayEquals(content, Files.readAllBytes(config.resolve(JavaSecurityFile.CONFIG_FILE_NAME)));
            try (Stream<Path> entries = Files.list(config)) {
                assertThat("temporary files must be removed", entries.count(), equalTo(1L));
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    public void testWriteFailsWhenTheFileCannotBeReplaced() throws IOException {
        final Path directory = newTempDir();
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");
        final Path file = directory.resolve("java.security");
        Files.createDirectory(file);
        Files.createFile(file.resolve("blocker"));

        assertThrows(IOException.class, () -> JavaSecurityFile.writeIfChanged(file, content));

        try (Stream<Path> entries = Files.list(directory)) {
            assertThat("the temporary file must be removed", entries.count(), equalTo(1L));
        }
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

    public void testConfigDirectoryMustBeSet() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> JavaSecurityFile.configDirectory(" "));

        assertThat(e.getMessage(), containsString("must be set"));
    }

    public void testConfigDirectoryMustBeAbsolute() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> JavaSecurityFile.configDirectory("config"));

        assertThat(e.getMessage(), containsString("must be an absolute path to an existing directory"));
    }

    public void testConfigDirectoryMustExist() throws IOException {
        final String missing = newTempDir().resolve("missing").toString();

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> JavaSecurityFile.configDirectory(missing));

        assertThat(e.getMessage(), containsString("must be an absolute path to an existing directory"));
    }

    public void testConfigDirectoryAcceptsAnExistingAbsoluteDirectory() throws IOException {
        final Path config = newTempDir().toAbsolutePath();

        assertThat(JavaSecurityFile.configDirectory(config.toString()), equalTo(config));
    }

    public void testTemporaryDirectoryPrefersOpenSearchTmpdir() {
        assertThat(JavaSecurityFile.temporaryDirectory("/os-tmp", "/java-tmp"), equalTo(Path.of("/os-tmp")));
        assertThat(JavaSecurityFile.temporaryDirectory(null, "/java-tmp"), equalTo(Path.of("/java-tmp")));
        assertThat(JavaSecurityFile.temporaryDirectory(" ", "/java-tmp"), equalTo(Path.of("/java-tmp")));
    }

    public void testWritePrefersTheConfigDirectory() throws IOException {
        final Path config = newTempDir();
        final Path base = newTempDir();
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");

        final Path file = JavaSecurityFile.write(config, base, content);

        assertThat(file, equalTo(config.resolve(JavaSecurityFile.CONFIG_FILE_NAME)));
        assertArrayEquals(content, Files.readAllBytes(file));
        try (Stream<Path> entries = Files.list(base)) {
            assertThat("nothing must be written to the temporary directory", entries.count(), equalTo(0L));
        }
    }

    public void testWriteReplacesTheConfigFileWhenTheContentChanges() throws IOException {
        final Path config = newTempDir();
        final Path base = newTempDir();
        JavaSecurityFile.write(config, base, JavaSecurityFile.render(linuxJdk(), "test"));
        final byte[] withBcFips = JavaSecurityFile.render(JavaSecurityFile.forDefaultMode(linuxJdk(), true), "test");

        final Path file = JavaSecurityFile.write(config, base, withBcFips);

        assertThat(file, equalTo(config.resolve(JavaSecurityFile.CONFIG_FILE_NAME)));
        assertArrayEquals(withBcFips, Files.readAllBytes(file));
    }

    public void testWriteFallsBackToThePrivateDirectoryWhenTheConfigFileCannotBeWritten() throws IOException {
        final Path config = newTempDir();
        final Path base = newTempDir();
        blockConfigFile(config);
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");

        final Path file = JavaSecurityFile.write(config, base, content);

        assertThat(file.getParent().getParent(), equalTo(base));
        assertThat(file.getFileName().toString(), equalTo(JavaSecurityFile.hashedFileName(content)));
        assertArrayEquals(content, Files.readAllBytes(file));
    }

    public void testWriteFallsBackToThePrivateDirectoryWhenTheConfigDirectoryIsReadOnly() throws IOException {
        assumePosix();
        final Path config = newTempDir();
        final Path base = newTempDir();
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");
        Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("r-xr-xr-x"));
        try {
            assumeTrue("needs a user that file permissions apply to", Files.isWritable(config) == false);

            final Path file = JavaSecurityFile.write(config, base, content);

            assertThat(file.getParent().getParent(), equalTo(base));
            assertArrayEquals(content, Files.readAllBytes(file));
        } finally {
            // lets the test framework delete the directory
            Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    public void testWriteFailureNamesBothLocationsAndTheFix() throws IOException {
        final Path config = newTempDir();
        blockConfigFile(config);
        final Path missing = newTempDir().resolve("missing");
        final byte[] content = JavaSecurityFile.render(linuxJdk(), "test");

        final IOException e = assertThrows(IOException.class, () -> JavaSecurityFile.write(config, missing, content));

        assertThat(e.getMessage(), containsString("[" + config.resolve(JavaSecurityFile.CONFIG_FILE_NAME) + "]"));
        assertThat(e.getMessage(), containsString("[" + missing + "]"));
        assertThat(
            e.getMessage(),
            containsString(
                "Make the config directory writable, point OPENSEARCH_TMPDIR to a writable directory, "
                    + "or pass your own file with -Djava.security.properties==<file> in OPENSEARCH_JAVA_OPTS."
            )
        );
        assertThat("the config directory failure must be kept", e.getSuppressed().length, equalTo(1));
    }

    /**
     * Puts a non-empty directory where the generated file belongs, so that writing the file fails on every platform.
     */
    private static void blockConfigFile(final Path config) throws IOException {
        final Path blocker = Files.createDirectory(config.resolve(JavaSecurityFile.CONFIG_FILE_NAME));
        Files.createFile(blocker.resolve("blocker"));
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
