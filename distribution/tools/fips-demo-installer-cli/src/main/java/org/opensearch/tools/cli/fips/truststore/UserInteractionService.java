/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.io.Console;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Scanner;

import picocli.CommandLine;

/**
 * Service for handling user interaction in console applications.
 * Provides utilities for confirmations and password input.
 */
public class UserInteractionService {

    public static final Scanner CONSOLE_SCANNER = new Scanner(System.in, StandardCharsets.UTF_8);
    private static final String ALPHA_NUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*";

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(CONSOLE_SCANNER::close));
    }

    public static boolean confirmAction(CommandLine.Model.CommandSpec spec, CommonOptions options, String message) {
        PrintWriter out = spec.commandLine().getOut();
        PrintWriter err = spec.commandLine().getErr();

        if (options.nonInteractive) {
            out.println(message + " - Auto-confirmed (non-interactive mode)");
            return true;
        }

        out.print(message + " [y/N] ");
        out.flush();

        if (!CONSOLE_SCANNER.hasNextLine()) {
            err.println(
                spec.commandLine().getColorScheme().errorText("\nERROR: No input available. Use --non-interactive to skip confirmations.")
            );
            return false;
        }
        String response = CONSOLE_SCANNER.nextLine().trim();
        return response.equalsIgnoreCase("yes") || response.equalsIgnoreCase("y");
    }

    public static String promptForPasswordWithConfirmation(CommandLine.Model.CommandSpec spec, CommonOptions options, String message) {
        if (options.nonInteractive) {
            // Generate a secure random password for non-interactive mode
            String password = generateSecurePassword();
            spec.commandLine().getOut().println("Generated secure password for trust store (non-interactive mode)");
            return password;
        }

        var password = promptForPassword(spec, options, message);
        var confirmPassword = promptForPassword(spec, options, "Confirm " + message.toLowerCase(java.util.Locale.ROOT));

        if (!password.equals(confirmPassword)) {
            throw new RuntimeException("Passwords do not match. Operation cancelled.");
        }

        return password;
    }

    private static String generateSecurePassword() {
        SecureRandom random = new SecureRandom();
        StringBuilder password = new StringBuilder();

        for (int i = 0; i < 24; i++) { // 24 characters for a strong password
            password.append(ALPHA_NUMERIC.charAt(random.nextInt(ALPHA_NUMERIC.length())));
        }

        return password.toString();
    }

    private static String promptForPassword(CommandLine.Model.CommandSpec spec, CommonOptions options, String message) {
        var out = spec.commandLine().getOut();
        var ansi = spec.commandLine().getColorScheme().ansi();

        if (options.nonInteractive) {
            // Generate a secure random password for non-interactive mode
            return generateSecurePassword();
        }

        Console console = System.console();
        if (console != null) {
            // Use Console for hidden password input
            char[] passwordChars = console.readPassword(message + ": ");
            if (passwordChars == null) {
                throw new RuntimeException("Password input cancelled.");
            }

            String password = new String(passwordChars);
            Arrays.fill(passwordChars, ' '); // Clear password from memory

            if (password.trim().isEmpty()) {
                throw new RuntimeException("Password cannot be empty.");
            }

            return password;
        } else {
            // Fallback to Scanner if no console (IDE debugging, etc.)
            out.print(ansi.string("@|yellow " + message + " (WARNING: will be visible): |@"));
            out.flush();

            if (!CONSOLE_SCANNER.hasNextLine()) {
                throw new RuntimeException("No input available for password.");
            }

            String password = CONSOLE_SCANNER.nextLine().trim();
            if (password.isEmpty()) {
                throw new RuntimeException("Password cannot be empty.");
            }

            return password;
        }
    }

}
