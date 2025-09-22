/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.io.Console;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Scanner;

/**
 * Service for handling user interaction in console applications.
 * Provides utilities for confirmations and password input.
 */
public class UserInteractionService {

    public static final Scanner CONSOLE_SCANNER = new Scanner(System.in, StandardCharsets.UTF_8);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(CONSOLE_SCANNER::close));
    }

    public static boolean confirmAction(CommonOptions options, String message) {
        if (options.nonInteractive) {
            System.out.println(message + " - Auto-confirmed (non-interactive mode)");
            return true;
        }

        System.out.print(message + " [y/N] ");
        System.out.flush();

        if (!CONSOLE_SCANNER.hasNextLine()) {
            System.err.println("\nERROR: No input available. Use --non-interactive to skip confirmations.");
            return false;
        }
        String response = CONSOLE_SCANNER.nextLine().trim();
        return response.equalsIgnoreCase("yes") || response.equalsIgnoreCase("y");
    }

    public static String promptForPassword(CommonOptions options, String message) {
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
            System.out.print(message + " (WARNING: will be visible): ");
            System.out.flush();

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

    private static String generateSecurePassword() {
        // Generate a cryptographically secure random password
        SecureRandom random = new SecureRandom();
        StringBuilder password = new StringBuilder();
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*";

        for (int i = 0; i < 24; i++) { // 24 characters for a strong password
            password.append(chars.charAt(random.nextInt(chars.length())));
        }

        return password.toString();
    }

}
