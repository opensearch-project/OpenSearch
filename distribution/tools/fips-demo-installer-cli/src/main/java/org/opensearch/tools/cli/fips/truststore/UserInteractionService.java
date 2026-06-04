/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.common.Randomness;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Scanner;

import picocli.CommandLine;

/**
 * Service for handling user interaction in console applications.
 * Provides utilities for confirmations and password input.
 */
public abstract class UserInteractionService {

    /** Shared scanner for reading console input. */
    private static final Scanner CONSOLE_SCANNER = new Scanner(System.in, StandardCharsets.UTF_8);
    private static final String ALPHA_NUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*";

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(CONSOLE_SCANNER::close));
    }

    protected abstract Scanner getScanner();

    public static UserInteractionService getInstance() {
        return new UserInteractionService() {
            @Override
            protected Scanner getScanner() {
                return CONSOLE_SCANNER;
            }
        };
    }

    /**
     * Prompts the user to confirm an action.
     *
     * @param spec the command specification for output
     * @param options common command-line options
     * @param message confirmation message to display
     * @return true if user confirms, false otherwise
     */
    public boolean confirmAction(CommandLine.Model.CommandSpec spec, CommonOptions options, String message) {
        var out = spec.commandLine().getOut();

        if (options.nonInteractive) {
            out.println(message + " - Auto-confirmed (non-interactive mode)");
            return true;
        }

        out.print(message + " [y/N] ");
        out.flush();

        if (!getScanner().hasNextLine()) {
            throwNoInputException();
        }
        var response = getScanner().nextLine().trim();
        return response.equalsIgnoreCase("yes") || response.equalsIgnoreCase("y");
    }

    /**
     * Prompts the user for a password with confirmation.
     *
     * @param spec the command specification for output
     * @param options common command-line options
     * @param message password prompt message
     * @return the entered password
     * @throws RuntimeException if passwords don't match or input is canceled
     */
    public String promptForPasswordWithConfirmation(CommandLine.Model.CommandSpec spec, CommonOptions options, String message) {
        var password = promptForPassword(spec, message);
        var confirmPassword = promptForPassword(spec, "Confirm " + message.toLowerCase(java.util.Locale.ROOT));

        if (!password.equals(confirmPassword)) {
            throw new RuntimeException("Passwords do not match. Operation cancelled.");
        }

        return password;
    }

    protected String promptForPassword(CommandLine.Model.CommandSpec spec, String message) {
        var out = spec.commandLine().getOut();
        var ansi = spec.commandLine().getColorScheme().ansi();

        out.print(ansi.string("@|yellow " + message + " (WARNING: will be visible): |@"));
        out.flush();

        if (!getScanner().hasNextLine()) {
            throwNoInputException();
        }

        var password = getScanner().nextLine().trim();
        if (password.isEmpty()) {
            throw new RuntimeException("Password cannot be empty.");
        }

        return password;
    }

    /**
     * Prompts the user to select from a numbered list of choices.
     *
     * @param spec          the command specification for output
     * @param choiceCount   the number of valid choices (1-based)
     * @param defaultChoice the default choice if user enters empty string (1-based, must be within valid range)
     * @return the user's choice (1-based index)
     */
    public int promptForChoice(CommandLine.Model.CommandSpec spec, int choiceCount, int defaultChoice) {
        assert choiceCount > 0 : "Choice count must be greater than 0.";
        assert defaultChoice >= 1 && defaultChoice <= choiceCount : "Default choice must be between 1 and " + choiceCount;

        var out = spec.commandLine().getOut();
        var scanner = getScanner();

        while (true) {
            out.printf(Locale.ROOT, "Enter choice (1-%s) [%s]:  ", choiceCount, defaultChoice);
            out.flush();

            if (!scanner.hasNextLine()) {
                throwNoInputException();
            }

            var input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                return defaultChoice;
            }

            try {
                var choice = Integer.parseInt(input);
                if (choice >= 1 && choice <= choiceCount) {
                    return choice;
                }
            } catch (NumberFormatException e) {
                // fall through to the last statement
            }
            out.println("Invalid choice.");
        }
    }

    public String generateSecurePassword() {
        var password = new StringBuilder();

        for (int i = 0; i < 24; i++) { // 24 characters for a strong password
            password.append(ALPHA_NUMERIC.charAt(Randomness.createSecure().nextInt(ALPHA_NUMERIC.length())));
        }

        return password.toString();
    }

    private static void throwNoInputException() {
        throw new IllegalStateException("\nNo input available. Use the '--non-interactive option' to skip confirmations.");
    }

}
