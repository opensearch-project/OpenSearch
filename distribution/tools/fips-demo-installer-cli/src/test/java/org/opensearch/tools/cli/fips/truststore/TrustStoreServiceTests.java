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
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import java.util.Scanner;

import picocli.CommandLine;

import static org.opensearch.tools.cli.fips.truststore.ConfigureSystemTrustStore.findPKCS11ProviderService;

public class TrustStoreServiceTests extends OpenSearchTestCase {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private CommandLine.Model.CommandSpec spec;
    private StringWriter outputCapture;
    private Path confPath;

    @Override
    @SuppressForbidden(reason = "the java.io.File is exposed by TemporaryFolder")
    public void setUp() throws Exception {
        super.setUp();
        outputCapture = new StringWriter();

        @CommandLine.Command
        class TestCommand {}

        var commandLine = new CommandLine(new TestCommand());
        commandLine.setOut(new PrintWriter(outputCapture, true));
        spec = commandLine.getCommandSpec();

        confPath = Files.createTempDirectory(tempFolder.newFolder().toPath(), "conf-");
    }

    public void testUseSystemTrustStoreUserCancels() {
        // given
        var userInteraction = createUserInteractionService("no\n");
        var service = new TrustStoreService(userInteraction);

        // when
        var result = service.useSystemTrustStore(spec, new CommonOptions(), null, confPath);

        // then
        assertEquals(Integer.valueOf(0), result);
        assertTrue(outputCapture.toString().contains("Operation cancelled."));
    }

    public void testUseSystemTrustStoreNoPKCS11ProvidersFound() {
        assumeTrue("Should only run when PKCS11 provider is NOT installed.", findPKCS11ProviderService().isEmpty());
        // given
        var options = new CommonOptions();
        options.nonInteractive = true;
        var userInteraction = createUserInteractionService("yes\n");
        var service = new TrustStoreService(userInteraction);

        // when
        var ex = assertThrows(IllegalStateException.class, () -> service.useSystemTrustStore(spec, options, null, confPath));

        // then
        assertTrue(ex.getMessage().contains("No PKCS11 provider found"));
    }

    public void testUseSystemTrustStoreWithPKCS11Provider() {
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
            var options = new CommonOptions();
            var userInteraction = createUserInteractionService("yes\nno\n"); // Select installation then cancel
            var service = new TrustStoreService(userInteraction);

            // when
            var result = service.useSystemTrustStore(spec, options, null, confPath);

            // then
            assertEquals(Integer.valueOf(0), result);
            assertTrue(outputCapture.toString().contains("Operation cancelled."));
        } finally {
            Security.removeProvider("PKCS11-Mock");
        }
    }

    public void testExecuteInteractiveSelectionNonInteractiveMode() {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        // given
        var options = new CommonOptions();
        options.nonInteractive = true;
        var userInteraction = createUserInteractionService("password123\npassword123\n");
        var service = new TrustStoreService(userInteraction);

        // when
        var result = service.executeInteractiveSelection(spec, options, confPath);

        // then
        assertTrue(outputCapture.toString().contains("Non-interactive mode: Using generated trust store (default)"));
        assertNotNull(result);
    }

    public void testExecuteInteractiveSelectionUserSelectsGenerate() {
        // given
        var options = new CommonOptions();
        var userInteraction = createUserInteractionService("1\nno\n"); // Select option 1, then cancel
        var service = new TrustStoreService(userInteraction);

        // when
        var result = service.executeInteractiveSelection(spec, options, confPath);

        // then
        var output = outputCapture.toString();
        assertTrue(output.contains("OpenSearch FIPS Demo Configuration Installer"));
        assertTrue(output.contains("Please select trust store configuration:"));
        assertTrue(output.contains("1. Generate new BCFKS trust store from system defaults"));
        assertTrue(output.contains("2. Use existing system PKCS11 trust store"));
        assertEquals(Integer.valueOf(0), result); // Cancelled
    }

    public void testExecuteInteractiveSelectionUserSelectsSystem() {
        // given
        var options = new CommonOptions();
        var userInteraction = createUserInteractionService("2\nno\n"); // Select option 2, then cancel
        var service = new TrustStoreService(userInteraction);

        // when
        var result = service.executeInteractiveSelection(spec, options, confPath);

        // then
        var output = outputCapture.toString();
        assertTrue(output.contains("OpenSearch FIPS Demo Configuration Installer"));
        assertEquals(Integer.valueOf(0), result); // Cancelled
    }

    /**
     * Creates a test UserInteractionService with simulated user input.
     * Uses the same pattern as UserInteractionServiceTests.
     */
    private UserInteractionService createUserInteractionService(String input) {
        // Cache scanner outside anonymous class to maintain stream position across multiple getScanner() calls
        var scanner = new Scanner(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        return new UserInteractionService() {
            @Override
            protected Scanner getScanner() {
                return scanner;
            }
        };
    }
}
