/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.security.Provider;
import java.util.List;
import java.util.Optional;

import picocli.CommandLine;

/**
 * Service for selecting appropriate security providers.
 * Handles interactive and non-interactive provider selection for PKCS11 configurations.
 */
public class ProviderSelectionService {

    private final UserInteractionService userInteraction;

    public ProviderSelectionService(UserInteractionService userInteraction) {
        this.userInteraction = userInteraction;
    }

    /**
     * Selects a PKCS11 provider service based on configuration and user interaction.
     *
     * @param spec the command specification for output
     * @param options common command-line options
     * @param serviceProviderList list of available PKCS11 provider services
     * @param preselectedPKCS11Provider optional pre-selected provider name
     * @return the selected provider service
     * @throws IllegalStateException if no providers are available
     * @throws IllegalArgumentException if pre-selected provider is not found
     * @throws RuntimeException if user cancels the operation
     */
    public Provider.Service selectProvider(
        CommandLine.Model.CommandSpec spec,
        CommonOptions options,
        List<Provider.Service> serviceProviderList,
        String preselectedPKCS11Provider
    ) {
        if (serviceProviderList.isEmpty()) {
            throw new IllegalStateException("No PKCS11 providers available. Please ensure a PKCS11 provider is installed and configured.");
        }

        if (preselectedPKCS11Provider != null) {
            Optional<Provider.Service> found = serviceProviderList.stream()
                .filter(service -> service.getProvider().getName().equals(preselectedPKCS11Provider))
                .findFirst();

            if (found.isPresent()) {
                return found.get();
            } else {
                var err = spec.commandLine().getErr();
                err.println(
                    spec.commandLine()
                        .getColorScheme()
                        .errorText("ERROR: Specified PKCS11 provider '" + preselectedPKCS11Provider + "' not found.")
                );
                err.println(spec.commandLine().getColorScheme().errorText("Available providers:"));
                serviceProviderList.forEach(
                    service -> err.println(spec.commandLine().getColorScheme().errorText("  - " + service.getProvider().getName()))
                );
                throw new IllegalArgumentException("Provider not found: " + preselectedPKCS11Provider);
            }
        }

        // Non-interactive mode: always use first available provider
        if (options.nonInteractive) {
            var service = serviceProviderList.get(0);
            spec.commandLine().getOut().println("Using PKCS11 provider: " + service.getProvider().getName());
            return service;
        }

        // Interactive mode: single provider requires confirmation
        if (serviceProviderList.size() == 1) {
            var service = serviceProviderList.get(0);
            var providerName = service.getProvider().getName();
            if (userInteraction.confirmAction(spec, options, "Use PKCS11 provider '" + providerName + "'?")) {
                return service;
            } else {
                return null;
            }
        }

        // Interactive mode: multiple providers - let user choose
        return selectProviderInteractively(spec, serviceProviderList);
    }

    /**
     * Prompts the user to select a provider from multiple available options.
     *
     * @param spec the command specification for output
     * @param serviceProviderList list of available PKCS11 provider services
     * @return the user-selected provider service, or null if operation is cancelled
     */
    protected Provider.Service selectProviderInteractively(CommandLine.Model.CommandSpec spec, List<Provider.Service> serviceProviderList) {
        var out = spec.commandLine().getOut();
        out.println("Multiple PKCS11 providers found:");
        for (int i = 0; i < serviceProviderList.size(); i++) {
            var service = serviceProviderList.get(i);
            out.println("  " + (i + 1) + ". " + service.getProvider().getName() + " (Algorithm: " + service.getAlgorithm() + ")");
        }
        out.println("Select PKCS11 provider");

        var choice = userInteraction.promptForChoice(spec, serviceProviderList.size(), 1);

        return serviceProviderList.get(choice - 1);
    }

}
