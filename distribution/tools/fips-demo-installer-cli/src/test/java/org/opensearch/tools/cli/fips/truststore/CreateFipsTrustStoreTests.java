/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.cli.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import picocli.CommandLine;

public class CreateFipsTrustStoreTests extends OpenSearchTestCase {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    protected static final Path JAVA_HOME = Path.of(System.getProperty("java.home"));
    protected static Path confDir;

    protected CommandLine.Model.CommandSpec spec;

    @BeforeClass
    @SuppressForbidden(reason = "the java.io.File is exposed by TemporaryFolder")
    public static void setUpClass() throws IOException {
        confDir = tempFolder.newFolder().toPath().resolve("config");
        Files.createDirectories(confDir);
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

}
