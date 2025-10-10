/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.nio.file.Path;
import java.util.function.Function;

import picocli.CommandLine.Model.CommandSpec;

import static org.opensearch.tools.cli.fips.truststore.UserInteractionService.CONSOLE_SCANNER;

/**
 * Service for managing trust store operations.
 * Handles generation and configuration of FIPS-compliant trust stores.
 */
public class TrustStoreService {

    /**
     * Generates a new BCFKS trust store from the JVM default trust store.
     *
     * @param spec the command specification for output
     * @param options common command-line options
     * @param confPath path to the OpenSearch configuration directory
     * @return exit code (0 for success, 1 for failure)
     */
    public Integer generateTrustStore(CommandSpec spec, CommonOptions options, Path confPath) {
        if (!UserInteractionService.confirmAction(spec, options, "Generate new BCFKS trust store from system defaults?")) {
            spec.commandLine().getOut().println("Operation cancelled.");
            return 0;
        }

        String password = UserInteractionService.promptForPasswordWithConfirmation(spec, options, "Enter trust store password");

        spec.commandLine().getOut().println("Generating BCFKS trust store...");

        try {
            ConfigurationProperties properties = Function.<Path>identity()
                .andThen(path -> CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, path))
                .andThen(trustStore -> CreateFipsTrustStore.convertToBCFKS(spec, trustStore, options, password, confPath))
                .andThen(bcfksPath -> CreateFipsTrustStore.configureBCFKSTrustStore(bcfksPath, password))
                .apply(Path.of(System.getProperty("java.home")));

            new ConfigurationService().writeSecurityConfigToJvmOptionsFile(properties, confPath);
            finishInstallation(spec, properties);

            return 0;
        } catch (Exception e) {
            spec.commandLine()
                .getErr()
                .println(spec.commandLine().getColorScheme().errorText("Error generating trust store: " + e.getMessage()));
            return 1;
        }
    }

    /**
     * Configures OpenSearch to use the system PKCS11 trust store.
     *
     * @param spec the command specification for output
     * @param options common command-line options
     * @param preselectedPKCS11Provider optional pre-selected PKCS11 provider name
     * @param confPath path to the OpenSearch configuration directory
     * @return exit code (0 for success, 1 for failure)
     */
    public Integer useSystemTrustStore(CommandSpec spec, CommonOptions options, String preselectedPKCS11Provider, Path confPath) {
        if (!UserInteractionService.confirmAction(spec, options, "Use system PKCS11 trust store?")) {
            spec.commandLine().getOut().println("Operation cancelled.");
            return 0;
        }

        spec.commandLine().getOut().println("Configuring system PKCS11 trust store...");

        try {
            var serviceProviderList = ConfigureSystemTrustStore.findPKCS11ProviderService();
            if (serviceProviderList.isEmpty()) {
                spec.commandLine()
                    .getErr()
                    .println(
                        spec.commandLine()
                            .getColorScheme()
                            .errorText("ERROR: No PKCS11 provider found. Check java.security configuration for FIPS.")
                    );
                return 1;
            }

            var selectedService = new ProviderSelectionService().selectProvider(
                spec,
                options,
                serviceProviderList,
                preselectedPKCS11Provider
            );
            spec.commandLine().getOut().println("Using PKCS11 provider: " + selectedService.getProvider().getName());

            var properties = ConfigureSystemTrustStore.configurePKCS11TrustStore(selectedService);

            new ConfigurationService().writeSecurityConfigToJvmOptionsFile(properties, confPath);
            finishInstallation(spec, properties);

            return 0;
        } catch (Exception e) {
            spec.commandLine()
                .getErr()
                .println(spec.commandLine().getColorScheme().errorText("Error configuring system trust store: " + e.getMessage()));
            return 1;
        }
    }

    /**
     * Presents an interactive menu for trust store configuration selection.
     *
     * @param spec the command specification for output
     * @param options common command-line options
     * @param confPath path to the OpenSearch configuration directory
     * @return exit code (0 for success, 1 for failure)
     */
    public Integer executeInteractiveSelection(CommandSpec spec, CommonOptions options, Path confPath) {
        if (options.nonInteractive) {
            spec.commandLine().getOut().println("Non-interactive mode: Using generated trust store (default)");
            return generateTrustStore(spec, options, confPath);
        }

        spec.commandLine().getOut().println("OpenSearch FIPS Demo Configuration Installer");
        spec.commandLine().getOut().println("Please select trust store configuration:");
        spec.commandLine().getOut().println("  1. Generate new BCFKS trust store from system defaults");
        spec.commandLine().getOut().println("  2. Use existing system PKCS11 trust store");

        while (true) {
            spec.commandLine().getOut().print("Enter choice (1-2) [1]: ");
            spec.commandLine().getOut().flush();

            if (!CONSOLE_SCANNER.hasNextLine()) {
                spec.commandLine()
                    .getErr()
                    .println(
                        spec.commandLine()
                            .getColorScheme()
                            .errorText("\nERROR: No input available. Use --non-interactive for automated execution.")
                    );
                return 1;
            }

            var input = CONSOLE_SCANNER.nextLine().trim();

            if (input.isEmpty() || input.equals("1")) {
                return generateTrustStore(spec, options, confPath);
            } else if (input.equals("2")) {
                return useSystemTrustStore(spec, options, null, confPath);
            } else {
                spec.commandLine().getOut().println("Invalid choice. Please enter 1 or 2.");
            }
        }
    }

    private static void finishInstallation(CommandSpec spec, ConfigurationProperties properties) {
        spec.commandLine().getOut().println();
        spec.commandLine().getOut().println("### Success!");
        spec.commandLine().getOut().println("### Execute this script on all your nodes and then start all nodes");
        spec.commandLine().getOut().println("### Trust Store Configuration:");
        spec.commandLine().getOut().print(properties.logout());
        spec.commandLine().getOut().println();
    }
}
