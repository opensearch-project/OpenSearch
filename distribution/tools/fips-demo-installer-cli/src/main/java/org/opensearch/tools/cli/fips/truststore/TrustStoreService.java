/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;

import picocli.CommandLine.Model.CommandSpec;

/**
 * Service for managing trust store operations.
 * Handles generation and configuration of FIPS-compliant trust stores.
 */
public class TrustStoreService {

    private final UserInteractionService userInteraction;

    public TrustStoreService(UserInteractionService userInteraction) {
        this.userInteraction = userInteraction;
    }

    /**
     * Generates a new BCFKS trust store from the JVM default trust store.
     *
     * @param spec the command specification for output
     * @param options common command-line options
     * @param confPath path to the OpenSearch configuration directory
     * @return exit code (0 for success, 1 for failure)
     */
    public Integer generateTrustStore(CommandSpec spec, CommonOptions options, Path confPath) {
        if (!userInteraction.confirmAction(spec, options, "Generate new BCFKS trust store from system defaults?")) {
            spec.commandLine().getOut().println("Operation cancelled.");
            return 0;
        }

        var password = Optional.ofNullable(options.password).orElseGet(() -> {
            if (options.nonInteractive) {
                spec.commandLine().getOut().println("Generated secure password for trust store (non-interactive mode)");
                return userInteraction.generateSecurePassword();
            }
            return userInteraction.promptForPasswordWithConfirmation(spec, options, "Enter trust store password");
        });
        if (password.isBlank()) {
            spec.commandLine()
                .getOut()
                .println(spec.commandLine().getColorScheme().ansi().string("@|yellow WARNING: Using empty password|@"));
        }

        spec.commandLine().getOut().println("Generating BCFKS trust store...");
        var properties = Function.<Path>identity()
            .andThen(path -> CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, path))
            .andThen(trustStore -> CreateFipsTrustStore.convertToBCFKS(spec, trustStore, options, password, confPath))
            .andThen(bcfksPath -> CreateFipsTrustStore.configureBCFKSTrustStore(bcfksPath, password))
            .apply(Path.of(System.getProperty("java.home")));

        new ConfigurationService().writeSecurityConfigToJvmOptionsFile(properties, confPath);
        finishInstallation(spec, properties);

        return 0;
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
        if (!userInteraction.confirmAction(spec, options, "Use system PKCS11 trust store?")) {
            spec.commandLine().getOut().println("Operation cancelled.");
            return 0;
        }

        spec.commandLine().getOut().println("Configuring system PKCS11 trust store...");

        var serviceProviderList = ConfigureSystemTrustStore.findPKCS11ProviderService();
        if (serviceProviderList.isEmpty()) {
            throw new IllegalStateException(
                "No PKCS11 provider found. Please check 'java.security' configuration file for installed providers."
            );
        }

        var selectedService = new ProviderSelectionService(userInteraction).selectProvider(
            spec,
            options,
            serviceProviderList,
            preselectedPKCS11Provider
        );
        if (selectedService == null) {
            spec.commandLine().getOut().println("Operation cancelled.");
            return 0;
        }
        spec.commandLine().getOut().println("Using PKCS11 provider: " + selectedService.getProvider().getName());
        var properties = ConfigureSystemTrustStore.configurePKCS11TrustStore(selectedService);
        new ConfigurationService().writeSecurityConfigToJvmOptionsFile(properties, confPath);
        finishInstallation(spec, properties);
        return 0;
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

        var out = spec.commandLine().getOut();
        out.println("OpenSearch FIPS Demo Configuration Installer");
        out.println("Please select trust store configuration:");
        out.println("  1. Generate new BCFKS trust store from system defaults");
        out.println("  2. Use existing system PKCS11 trust store");

        var choice = userInteraction.promptForChoice(spec, 2, 1);

        return choice == 1 ? generateTrustStore(spec, options, confPath) : useSystemTrustStore(spec, options, null, confPath);
    }

    private static void finishInstallation(CommandSpec spec, ConfigurationProperties properties) {
        spec.commandLine().getOut().println();
        spec.commandLine().getOut().println("### Success!");
        spec.commandLine().getOut().println("### Execute this script on all your nodes and then start all nodes");
        spec.commandLine().getOut().println("### Trust Store Configuration:");
        spec.commandLine().getOut().print(properties.toString());
        spec.commandLine().getOut().println();
    }
}
