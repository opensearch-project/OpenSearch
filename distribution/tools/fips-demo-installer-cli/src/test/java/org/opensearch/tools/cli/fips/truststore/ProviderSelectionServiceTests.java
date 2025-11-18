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
import java.security.Provider;
import java.util.List;

import picocli.CommandLine;

public class ProviderSelectionServiceTests extends OpenSearchTestCase {

    private ProviderSelectionService cut;
    private StringWriter outputCapture;
    private StringWriter errorCapture;
    private CommandLine.Model.CommandSpec spec;
    private CommonOptions options;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        cut = new ProviderSelectionService(UserInteractionService.getInstance());
        outputCapture = new StringWriter();
        errorCapture = new StringWriter();

        var testCommand = new TestCommand();
        CommandLine commandLine = new CommandLine(testCommand);
        commandLine.setOut(new PrintWriter(outputCapture, true));
        commandLine.setErr(new PrintWriter(errorCapture, true));
        spec = commandLine.getCommandSpec();
        options = new CommonOptions();
    }

    public void testSelectProviderWithPreselectedProviderFound() {
        // given
        var service1 = createMockService("Provider1");
        var service2 = createMockService("Provider2");
        List<Provider.Service> serviceList = List.of(service1, service2);

        // when
        var result = cut.selectProvider(spec, options, serviceList, "Provider2");

        // then
        assertEquals("Provider2", result.getProvider().getName());
        assertEquals(service2, result);
    }

    public void testSelectProviderWithPreselectedProviderNotFound() {
        // given
        var service1 = createMockService("Provider1");
        List<Provider.Service> serviceList = List.of(service1);

        // when/then
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> cut.selectProvider(spec, options, serviceList, "NonExistentProvider")
        );

        assertEquals("Provider not found: NonExistentProvider", exception.getMessage());
        assertTrue(errorCapture.toString().contains("ERROR: Specified PKCS11 provider 'NonExistentProvider' not found"));
        assertTrue(errorCapture.toString().contains("Available providers:"));
        assertTrue(errorCapture.toString().contains("- Provider1"));
    }

    public void testSelectProviderSingleProviderNonInteractive() {
        // given
        options.nonInteractive = true;
        var service1 = createMockService("OnlyProvider");
        List<Provider.Service> serviceList = List.of(service1);

        // when
        var result = cut.selectProvider(spec, options, serviceList, null);

        // then
        assertEquals("OnlyProvider", result.getProvider().getName());
        assertTrue(outputCapture.toString().contains("Using PKCS11 provider: OnlyProvider"));
    }

    public void testSelectProviderMultipleProvidersNonInteractive() {
        // given
        options.nonInteractive = true;
        var service1 = createMockService("FirstProvider");
        var service2 = createMockService("SecondProvider");
        List<Provider.Service> serviceList = List.of(service1, service2);

        // when
        var result = cut.selectProvider(spec, options, serviceList, null);

        // then
        assertEquals("FirstProvider", result.getProvider().getName());
        assertTrue(outputCapture.toString().contains("Using PKCS11 provider: FirstProvider"));
    }

    public void testSelectProviderWithEmptyList() {
        // given
        List<Provider.Service> serviceProviderList = List.of();
        options.nonInteractive = true;

        // when/then
        var exception = expectThrows(IllegalStateException.class, () -> cut.selectProvider(spec, options, serviceProviderList, null));

        assertEquals("No PKCS11 providers available. Please ensure a PKCS11 provider is installed and configured.", exception.getMessage());
    }

    public void testSelectProviderInteractivelyWithThreeProvidersShowsAllOptions() {
        // given
        var service1 = createMockService("ProviderAlpha");
        var service2 = createMockService("ProviderBeta");
        var service3 = createMockService("ProviderGamma");
        List<Provider.Service> serviceList = List.of(service1, service2, service3);

        // when/then - no input available, throws IllegalStateException
        expectThrows(IllegalStateException.class, () -> cut.selectProviderInteractively(spec, serviceList));

        // Verify output displays all three providers with correct numbering
        var output = outputCapture.toString();
        assertTrue(output.contains("Multiple PKCS11 providers found:"));
        assertTrue(output.contains("1. ProviderAlpha (Algorithm: KeyStore)"));
        assertTrue(output.contains("2. ProviderBeta (Algorithm: KeyStore)"));
        assertTrue(output.contains("3. ProviderGamma (Algorithm: KeyStore)"));
        assertTrue(output.contains("Select PKCS11 provider"));
    }

    // Test: Multiple providers in interactive mode call selectProviderInteractively
    public void testSelectProviderMultipleProvidersInteractiveModeCallsSelectProviderInteractively() {
        // given
        options.nonInteractive = false;
        var service1 = createMockService("InteractiveProv1");
        var service2 = createMockService("InteractiveProv2");
        List<Provider.Service> serviceList = List.of(service1, service2);

        // when/then - no input available, throws IllegalStateException
        expectThrows(IllegalStateException.class, () -> cut.selectProvider(spec, options, serviceList, null));

        var output = outputCapture.toString();
        assertTrue(output.contains("Multiple PKCS11 providers found:"));
        assertTrue(output.contains("1. InteractiveProv1"));
        assertTrue(output.contains("2. InteractiveProv2"));
    }

    private Provider.Service createMockService(String name) {
        var provider = new Provider(name, "1.0", "Mock Provider for " + name) {
        };
        return new Provider.Service(provider, "PKCS11", "KeyStore", "MockImplementation", null, null);
    }

    // Simple test command for getting CommandSpec
    @CommandLine.Command(name = "test")
    static class TestCommand {}
}
