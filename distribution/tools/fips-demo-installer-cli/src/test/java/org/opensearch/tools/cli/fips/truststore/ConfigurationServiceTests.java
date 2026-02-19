/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import picocli.CommandLine;

public class ConfigurationServiceTests extends OpenSearchTestCase {

    private static Path sharedTempDir;

    private Path jvmOptionsFile;
    private CommandLine.Model.CommandSpec spec;
    private StringWriter outputCapture;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    @SuppressForbidden(reason = "TemporaryFolder does not support Path-based APIs")
    public static void setUpClass() throws Exception {
        sharedTempDir = tempFolder.newFolder("config-test").toPath();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        jvmOptionsFile = sharedTempDir.resolve("jvm.options");
        if (Files.exists(jvmOptionsFile)) {
            Files.delete(jvmOptionsFile);
        }

        @CommandLine.Command
        class DummyCommand {}

        outputCapture = new StringWriter();
        CommandLine commandLine = new CommandLine(new DummyCommand());
        commandLine.setOut(new PrintWriter(outputCapture, true));
        spec = commandLine.getCommandSpec();
    }

    public void testVerifyJvmOptionsFileWithForceMode() throws Exception {
        // given
        var options = new CommonOptions();
        options.force = true;

        // when
        ConfigurationService.verifyJvmOptionsFile(spec, options, sharedTempDir);

        // then
        var output = outputCapture.toString();
        assertTrue(output.contains("WARNING: Force mode enabled"));
    }

    public void testVerifyJvmOptionsFileDoesNotExist() {
        var exception = expectThrows(
            IllegalStateException.class,
            () -> ConfigurationService.verifyJvmOptionsFile(spec, new CommonOptions(), sharedTempDir)
        );
        assertTrue(exception.getMessage().contains("jvm.options file does not exist"));
    }

    public void testVerifyJvmOptionsFileIsEmpty() throws Exception {
        Files.createFile(jvmOptionsFile);

        var exception = expectThrows(
            IllegalStateException.class,
            () -> ConfigurationService.verifyJvmOptionsFile(spec, new CommonOptions(), sharedTempDir)
        );
        assertTrue(exception.getMessage().contains("jvm.options file is empty"));
    }

    public void testVerifyJvmOptionsFileContainsFipsConfiguration() throws Exception {
        Files.writeString(jvmOptionsFile, """
            -Xms1g
            -Djavax.net.ssl.trustStore=/path/to/store
            """, StandardCharsets.UTF_8);

        var exception = expectThrows(
            IllegalStateException.class,
            () -> ConfigurationService.verifyJvmOptionsFile(spec, new CommonOptions(), sharedTempDir)
        );
        assertTrue(exception.getMessage().contains("FIPS demo configuration already exists"));
        assertTrue(exception.getMessage().contains("trustStor"));
    }

    public void testWriteSecurityConfigToJvmOptionsFile() throws Exception {
        // given
        Files.writeString(jvmOptionsFile, """
            -Xms1g
            -Xmx1g
            """, StandardCharsets.UTF_8);
        var properties = new ConfigurationProperties("/path/to/truststore.bcfks", "BCFKS", "changeit", "BCFIPS");
        var service = new ConfigurationService();

        // when
        service.writeSecurityConfigToJvmOptionsFile(properties, sharedTempDir);

        // then
        var content = Files.readString(jvmOptionsFile, StandardCharsets.UTF_8);
        assertTrue(content.contains("-Xms1g"));
        assertTrue(content.contains("Start OpenSearch FIPS Demo Configuration"));
        assertTrue(content.contains("WARNING: revise all the lines below before you go into production"));
        assertTrue(content.contains("-Djavax.net.ssl.trustStore=/path/to/truststore.bcfks"));
        assertTrue(content.contains("-Djavax.net.ssl.trustStoreType=BCFKS"));
        assertTrue(content.contains("-Djavax.net.ssl.trustStorePassword=changeit"));
        assertTrue(content.contains("-Djavax.net.ssl.trustStoreProvider=BCFIPS"));
    }

    public void testWriteSecurityConfigCreatesFileIfNotExists() throws Exception {
        // given
        var properties = new ConfigurationProperties("/new/truststore.bcfks", "BCFKS", "password", "BCFIPS");
        var service = new ConfigurationService();

        // when
        service.writeSecurityConfigToJvmOptionsFile(properties, sharedTempDir);

        // then
        assertTrue(Files.exists(jvmOptionsFile));
        String content = Files.readString(jvmOptionsFile, StandardCharsets.UTF_8);
        assertTrue(content.contains("-Djavax.net.ssl.trustStore=/new/truststore.bcfks"));
    }

    public void testWriteSecurityConfigFormatting() throws Exception {
        // given
        Files.createFile(jvmOptionsFile);
        var properties = new ConfigurationProperties("/path/to/store", "BCFKS", "pass", "BCFIPS");
        var service = new ConfigurationService();

        // when
        service.writeSecurityConfigToJvmOptionsFile(properties, sharedTempDir);

        // then
        var content = Files.readString(jvmOptionsFile, StandardCharsets.UTF_8);

        assertTrue(content.contains("################################################################"));
        assertTrue(content.contains("## Start OpenSearch FIPS Demo Configuration"));

        assertEquals(4, content.lines().filter(line -> line.startsWith("-Djavax.net.ssl.trustStore")).count());
    }

    public void testVerifyJvmOptionsFileThrowsOnReadError() throws Exception {
        assumeTrue("requires POSIX file permissions", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        Files.createFile(jvmOptionsFile);
        Files.writeString(jvmOptionsFile, "-Xms1g\n", StandardCharsets.UTF_8);

        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions(jvmOptionsFile);
        Set<PosixFilePermission> noReadPermissions = PosixFilePermissions.fromString("---------");
        Files.setPosixFilePermissions(jvmOptionsFile, noReadPermissions);

        try {
            var exception = expectThrows(
                IllegalStateException.class,
                () -> ConfigurationService.verifyJvmOptionsFile(spec, new CommonOptions(), sharedTempDir)
            );
            assertTrue(
                exception.getMessage().contains("jvm.options file is not readable")
                    || exception.getMessage().contains("Failed to read jvm.options file")
            );
        } finally {
            Files.setPosixFilePermissions(jvmOptionsFile, originalPermissions);
        }
    }

    public void testWriteSecurityConfigThrowsOnWriteError() throws Exception {
        assumeTrue("requires POSIX file permissions", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        Files.createFile(jvmOptionsFile);

        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions(jvmOptionsFile);
        Set<PosixFilePermission> readOnlyPermissions = PosixFilePermissions.fromString("r--r--r--");
        Files.setPosixFilePermissions(jvmOptionsFile, readOnlyPermissions);

        var properties = new ConfigurationProperties("/path/to/store", "BCFKS", "pass", "BCFIPS");
        var service = new ConfigurationService();

        try {
            var exception = expectThrows(
                RuntimeException.class,
                () -> service.writeSecurityConfigToJvmOptionsFile(properties, sharedTempDir)
            );
            assertTrue(exception.getMessage().contains("Exception writing security configuration"));
        } finally {
            Files.setPosixFilePermissions(jvmOptionsFile, originalPermissions);
        }
    }

    public void testValidateJvmOptionsContentWithValidFile() throws Exception {
        Files.writeString(jvmOptionsFile, """
            -Xms1g
            -Xmx1g
            -XX:+UseG1GC
            """, StandardCharsets.UTF_8);

        ConfigurationService.validateJvmOptionsContent(jvmOptionsFile);
    }

    public void testValidateJvmOptionsContentThrowsOnEmptyFile() throws Exception {
        Files.createFile(jvmOptionsFile);

        var exception = expectThrows(IllegalStateException.class, () -> ConfigurationService.validateJvmOptionsContent(jvmOptionsFile));
        assertTrue(exception.getMessage().contains("jvm.options file is empty"));
    }

    public void testValidateJvmOptionsContentThrowsOnExistingFipsProperty() throws Exception {
        Files.writeString(jvmOptionsFile, """
            -Xms1g
            -Djavax.net.ssl.trustStore=/path/to/store
            """, StandardCharsets.UTF_8);

        var exception = expectThrows(IllegalStateException.class, () -> ConfigurationService.validateJvmOptionsContent(jvmOptionsFile));
        assertTrue(exception.getMessage().contains("FIPS demo configuration already exists"));
        assertTrue(exception.getMessage().contains("trustStor"));
    }

    public void testValidateJvmOptionsContentThrowsOnIOException() throws Exception {
        var nonExistentFile = sharedTempDir.resolve("does-not-exist.txt");

        var exception = expectThrows(IllegalStateException.class, () -> ConfigurationService.validateJvmOptionsContent(nonExistentFile));
        assertTrue(exception.getMessage().contains("Failed to read jvm.options file"));
    }
}
