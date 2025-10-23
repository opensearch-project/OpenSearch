/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import picocli.CommandLine;

public class CreateFipsTrustStoreTests extends OpenSearchTestCase {

    private static final Path JAVA_HOME = Path.of(System.getProperty("java.home"));
    private static Path sharedTempDir;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private CommandLine.Model.CommandSpec spec;

    @BeforeClass
    public static void setUpClass() throws IOException {
        sharedTempDir = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "fips-test-");
        Path confDir = sharedTempDir.resolve("config");
        Files.createDirectories(confDir);
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        if (sharedTempDir != null && Files.exists(sharedTempDir)) {
            try (Stream<Path> walk = Files.walk(sharedTempDir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        // Ignore
                    }
                });
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        @CommandLine.Command
        class DummyCommand {}

        CommandLine commandLine = new CommandLine(new DummyCommand());
        commandLine.setOut(new PrintWriter(new LoggerWriter(logger::info), true));
        commandLine.setErr(new PrintWriter(new LoggerWriter(logger::error), true));
        spec = commandLine.getCommandSpec();

        // Clean up any existing truststore file from previous tests
        Path confDir = sharedTempDir.resolve("config");
        Path trustStorePath = confDir.resolve("opensearch-fips-truststore.bcfks");
        if (Files.exists(trustStorePath)) {
            Files.delete(trustStorePath);
        }
    }

    /** Writer that delegates to a logger */
    private static class LoggerWriter extends Writer {
        private final StringBuilder buffer = new StringBuilder();
        private final Consumer<String> log;

        LoggerWriter(Consumer<String> log) {
            this.log = log;
        }

        @Override
        public void write(char[] cbuf, int off, int len) {
            buffer.append(cbuf, off, len);
        }

        @Override
        public void flush() {
            if (!buffer.isEmpty()) {
                String message = buffer.toString().stripTrailing();
                if (!message.isEmpty()) log.accept(message);
                buffer.setLength(0);
            }
        }

        @Override
        public void close() {
            flush();
        }
    }

    public void testLoadJvmDefaultTrustStore() throws Exception {
        // when
        var keyStore = CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, JAVA_HOME);

        // then
        assertTrue("JVMs truststore is not empty", keyStore.size() > 0);
        assertNotNull(keyStore);
    }

    public void testLoadJvmDefaultTrustStoreWithInvalidPath() {
        // given
        Path invalidPath = Path.of("/non/existent/path");

        // when/then
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, invalidPath)
        );
        assertTrue(exception.getMessage().contains("System cacerts not found at"));
    }

    public void testConfigureBCFKSTrustStore() {
        // given
        Path bcfksPath = Path.of("/tmp/test-truststore.bcfks");
        String password = "testPassword123";

        // when
        ConfigurationProperties config = CreateFipsTrustStore.configureBCFKSTrustStore(bcfksPath, password);

        // then
        assertNotNull(config);
        assertEquals(bcfksPath.toAbsolutePath().toString(), config.trustStorePath());
        assertEquals("BCFKS", config.trustStoreType());
        assertEquals(password, config.trustStorePassword());
        assertEquals("BCFIPS", config.trustStoreProvider());
    }

    public void testConvertToBCFKS() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        // given
        KeyStore sourceKeyStore = CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, JAVA_HOME);
        assertTrue("Source keystore should have certificates", sourceKeyStore.size() > 0);

        CommonOptions options = new CommonOptions();
        options.force = false;
        String password = "testPassword123";
        Path confPath = sharedTempDir.resolve("config");

        // when
        Path result = CreateFipsTrustStore.convertToBCFKS(spec, sourceKeyStore, options, password, confPath);

        // then
        assertNotNull(result);
        assertTrue(Files.exists(result));
        assertTrue(result.toString().endsWith("opensearch-fips-truststore.bcfks"));

        // Verify the converted keystore has the same certificates
        KeyStore bcfksStore = KeyStore.getInstance("BCFKS", "BCFIPS");
        try (var is = Files.newInputStream(result)) {
            bcfksStore.load(is, password.toCharArray());
        }
        assertEquals("Converted keystore should have same number of certificates", sourceKeyStore.size(), bcfksStore.size());
    }

    public void testConvertToBCFKSFileExistsWithoutForce() throws Exception {
        // Skip if BCFIPS not available since the method needs it to check file handling
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        // given
        KeyStore sourceKeyStore = CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, JAVA_HOME);
        assertTrue("Source keystore should have certificates", sourceKeyStore.size() > 0);

        CommonOptions options = new CommonOptions();
        options.force = false;
        String password = "testPassword123";

        // Create file first to simulate existing truststore
        Path confPath = sharedTempDir.resolve("config");
        Path trustStorePath = confPath.resolve("opensearch-fips-truststore.bcfks");
        Files.createFile(trustStorePath);

        assertTrue("Test setup: file should exist", Files.exists(trustStorePath));

        // when/then
        RuntimeException exception = expectThrows(
            RuntimeException.class,
            () -> CreateFipsTrustStore.convertToBCFKS(spec, sourceKeyStore, options, password, confPath)
        );
        assertEquals("Operation cancelled. Trust store file already exists.", exception.getMessage());
    }

    public void testConvertToBCFKSFileExistsWithForce() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        // given
        KeyStore sourceKeyStore = CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, JAVA_HOME);
        assertTrue("Source keystore should have certificates", sourceKeyStore.size() > 0);

        CommonOptions options = new CommonOptions();
        options.force = true;
        String password = "testPassword123";

        // Create file first
        Path confPath = sharedTempDir.resolve("config");
        Path trustStorePath = confPath.resolve("opensearch-fips-truststore.bcfks");
        Files.createFile(trustStorePath);

        assertTrue(Files.exists(trustStorePath));

        // when
        Path result = CreateFipsTrustStore.convertToBCFKS(spec, sourceKeyStore, options, password, confPath);

        // then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        // Verify the converted keystore has actual certificates
        KeyStore bcfksStore = KeyStore.getInstance("BCFKS", "BCFIPS");
        try (var is = Files.newInputStream(result)) {
            bcfksStore.load(is, password.toCharArray());
        }
        assertTrue("Converted keystore should have certificates", bcfksStore.size() > 0);
        assertEquals("Converted keystore should have same number of certificates", sourceKeyStore.size(), bcfksStore.size());
    }

}
