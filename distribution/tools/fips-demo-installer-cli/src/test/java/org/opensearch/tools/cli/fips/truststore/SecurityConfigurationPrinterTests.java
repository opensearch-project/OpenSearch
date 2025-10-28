/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Security;
import java.util.Arrays;

import picocli.CommandLine;

public class SecurityConfigurationPrinterTests extends OpenSearchTestCase {

    private CommandLine.Model.CommandSpec spec;
    private StringWriter outputCapture;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        @CommandLine.Command
        class DummyCommand {}

        outputCapture = new StringWriter();
        CommandLine commandLine = new CommandLine(new DummyCommand());
        commandLine.setOut(new PrintWriter(outputCapture, true));
        spec = commandLine.getCommandSpec();
    }

    public void testPrintCurrentConfigurationBasicStructure() {
        // given
        var providers = Security.getProviders();

        // when
        SecurityConfigurationPrinter.printCurrentConfiguration(spec);

        // then
        String output = outputCapture.toString();

        assertFalse(output.isEmpty());
        assertTrue(output.contains("Available Security Providers:"));
        assertTrue(output.contains("  1."));
        assertTrue(output.contains("(version"));

        long numberedItems = output.lines().filter(line -> line.trim().matches("^\\d+\\..*")).count();
        assertEquals(providers.length, numberedItems);

        var firstProvider = providers[0];
        assertTrue(output.contains(firstProvider.getName()));
        String expectedVersionFormat = firstProvider.getName() + " (version " + firstProvider.getVersionStr() + ")";
        assertTrue(output.contains(expectedVersionFormat));
    }

    public void testPrintCurrentConfigurationKeyStoreServices() {
        // given
        var keyStoreAlgorithms = Arrays.stream(Security.getProviders())
            .flatMap(provider -> provider.getServices().stream())
            .filter(service -> "KeyStore".equals(service.getType()))
            .map(java.security.Provider.Service::getAlgorithm)
            .toList();

        // when
        SecurityConfigurationPrinter.printCurrentConfiguration(spec);

        // then
        String output = outputCapture.toString();

        if (!keyStoreAlgorithms.isEmpty()) {
            assertTrue(output.contains("└─ KeyStore."));

            for (String algorithm : keyStoreAlgorithms) {
                assertTrue(output.contains("KeyStore." + algorithm));
            }
        }
    }
}
