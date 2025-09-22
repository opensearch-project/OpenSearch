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

import static org.opensearch.tools.cli.fips.truststore.UserInteractionService.CONSOLE_SCANNER;

/**
 * Service for selecting appropriate security providers.
 * Handles interactive and non-interactive provider selection for PKCS11 configurations.
 */
public class ProviderSelectionService {

    public Provider.Service selectProvider(CommonOptions options, List<Provider.Service> serviceProviderList, String pkcs11Provider) {
        if (pkcs11Provider != null) {
            Optional<Provider.Service> found = serviceProviderList.stream()
                .filter(service -> service.getProvider().getName().equals(pkcs11Provider))
                .findFirst();

            if (found.isPresent()) {
                return found.get();
            } else {
                System.err.println("ERROR: Specified PKCS11 provider '" + pkcs11Provider + "' not found.");
                System.err.println("Available providers:");
                serviceProviderList.forEach(service -> System.err.println("  - " + service.getProvider().getName()));
                throw new IllegalArgumentException("Provider not found: " + pkcs11Provider);
            }
        }

        if (serviceProviderList.size() == 1) {
            Provider.Service service = serviceProviderList.get(0);
            String providerName = service.getProvider().getName();
            if (options.nonInteractive) {
                System.out.println("Using PKCS11 provider: " + providerName);
                return service;
            } else if (UserInteractionService.confirmAction(options, "Use PKCS11 provider '" + providerName + "'?")) {
                return service;
            } else {
                throw new RuntimeException("Operation cancelled by user.");
            }
        }

        if (options.nonInteractive) {
            Provider.Service service = serviceProviderList.get(0);
            System.out.println("Non-interactive mode: Using first available PKCS11 provider: " + service.getProvider().getName());
            return service;
        }
        return selectProviderInteractively(serviceProviderList);
    }

    private Provider.Service selectProviderInteractively(List<Provider.Service> serviceProviderList) {
        System.out.println("Multiple PKCS11 providers found:");
        for (int i = 0; i < serviceProviderList.size(); i++) {
            Provider.Service service = serviceProviderList.get(i);
            System.out.println("  " + (i + 1) + ". " + service.getProvider().getName() + " (Algorithm: " + service.getAlgorithm() + ")");
        }

        while (true) {
            System.out.print("Select PKCS11 provider (1-" + serviceProviderList.size() + "): ");
            System.out.flush();

            if (!CONSOLE_SCANNER.hasNextLine()) {
                throw new RuntimeException("No input available. Specify provider with --pkcs11-provider option.");
            }

            var input = CONSOLE_SCANNER.nextLine().trim();

            try {
                int choice = Integer.parseInt(input);
                if (choice >= 1 && choice <= serviceProviderList.size()) {
                    return serviceProviderList.get(choice - 1);
                }
            } catch (NumberFormatException ignored) {}

            System.out.println("Invalid choice. Please enter a number between 1 and " + serviceProviderList.size());
        }
    }

}
