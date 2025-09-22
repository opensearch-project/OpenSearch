/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.nio.file.Path;
import java.security.Provider;
import java.util.function.Function;

import static org.opensearch.tools.cli.fips.truststore.UserInteractionService.CONSOLE_SCANNER;

/**
 * Service for managing trust store operations.
 * Handles generation and configuration of FIPS-compliant trust stores.
 */
public class TrustStoreService {

    public Integer generateTrustStore(CommonOptions options) {
        if (!UserInteractionService.confirmAction(options, "Generate new BCFKS trust store from system defaults?")) {
            System.out.println("Operation cancelled.");
            return 0;
        }

        System.out.println("Generating BCFKS trust store...");

        try {
            ConfigurationProperties properties = Function.<Path>identity()
                .andThen(CreateFipsTrustStore::loadJvmDefaultTrustStore)
                .andThen(trustStore -> CreateFipsTrustStore.convertToBCFKS(trustStore, options))
                .andThen(CreateFipsTrustStore::configureBCFKSTrustStore)
                .apply(Path.of(System.getProperty("java.home")));

            new ConfigurationService().writeSecurityConfigToJvmOptionsFile(properties);
            finishInstallation(properties);

            return 0;
        } catch (Exception e) {
            System.err.println("Error generating trust store: " + e.getMessage());
            return 1;
        }
    }

    public Integer useSystemTrustStore(CommonOptions options, String pkcs11Provider) {
        if (!UserInteractionService.confirmAction(options, "Use system PKCS11 trust store?")) {
            System.out.println("Operation cancelled.");
            return 0;
        }

        System.out.println("Configuring system PKCS11 trust store...");

        try {
            var serviceProviderList = ConfigureSystemTrustStore.findPKCS11ProviderService();
            if (serviceProviderList.isEmpty()) {
                System.err.println("ERROR: No PKCS11 provider found. Check java.security configuration for FIPS.");
                return 1;
            }

            Provider.Service selectedService = new ProviderSelectionService().selectProvider(options, serviceProviderList, pkcs11Provider);
            System.out.println("Using PKCS11 provider: " + selectedService.getProvider().getName());

            var properties = ConfigureSystemTrustStore.configurePKCS11TrustStore(selectedService);

            new ConfigurationService().writeSecurityConfigToJvmOptionsFile(properties);
            finishInstallation(properties);

            return 0;
        } catch (Exception e) {
            System.err.println("Error configuring system trust store: " + e.getMessage());
            return 1;
        }
    }

    public Integer executeInteractiveSelection(CommonOptions options) {
        if (options.nonInteractive) {
            System.out.println("Non-interactive mode: Using generated trust store (default)");
            return generateTrustStore(options);
        }

        System.out.println("OpenSearch FIPS Demo Configuration Installer");
        System.out.println("Please select trust store configuration:");
        System.out.println("  1. Generate new BCFKS trust store from system defaults");
        System.out.println("  2. Use existing system PKCS11 trust store");

        while (true) {
            System.out.print("Enter choice (1-2) [1]: ");
            System.out.flush();

            if (!CONSOLE_SCANNER.hasNextLine()) {
                System.err.println("\nERROR: No input available. Use --non-interactive for automated execution.");
                return 1;
            }

            var input = CONSOLE_SCANNER.nextLine().trim();

            if (input.isEmpty() || input.equals("1")) {
                return generateTrustStore(options);
            } else if (input.equals("2")) {
                return useSystemTrustStore(options, null);
            } else {
                System.out.println("Invalid choice. Please enter 1 or 2.");
            }
        }
    }

    private static void finishInstallation(ConfigurationProperties properties) {
        System.out.println();
        System.out.println("### Success!");
        System.out.println("### Execute this script on all your nodes and then start all nodes");
        System.out.println("### Trust Store Configuration:");
        System.out.print(properties.logout());
        System.out.println();
    }
}
