/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import picocli.CommandLine;

public class UserInteractionServiceTests extends OpenSearchTestCase {

    protected CommandLine.Model.CommandSpec spec;
    protected StringWriter outputCapture;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        @CommandLine.Command
        class DummyCommand {}

        outputCapture = new StringWriter();
        var commandLine = new CommandLine(new DummyCommand());
        commandLine.setOut(new PrintWriter(outputCapture, true));
        spec = commandLine.getCommandSpec();
    }

    public void testConfirmActionYes() {
        {
            var service = createService("yes\n");
            assertTrue(service.confirmAction(spec, new CommonOptions(), "Proceed?"));
            assertTrue(outputCapture.toString().contains("Proceed? [y/N]"));
        }
        {
            var service = createService("YES\n");
            assertTrue(service.confirmAction(spec, new CommonOptions(), "Proceed?"));
            assertTrue(outputCapture.toString().contains("Proceed? [y/N]"));
        }
        {
            var service = createService("y\n");
            assertTrue(service.confirmAction(spec, new CommonOptions(), "Proceed?"));
            assertTrue(outputCapture.toString().contains("Proceed? [y/N]"));
        }
        {
            var service = createService("Y\n");
            assertTrue(service.confirmAction(spec, new CommonOptions(), "Proceed?"));
            assertTrue(outputCapture.toString().contains("Proceed? [y/N]"));
        }
    }

    public void testConfirmActionNo() {
        var service = createService("no\n");
        assertFalse(service.confirmAction(spec, new CommonOptions(), "Proceed?"));
    }

    public void testConfirmActionNonInteractive() {
        var options = new CommonOptions();
        options.nonInteractive = true;
        var service = createService("");
        assertTrue(service.confirmAction(spec, options, "Proceed?"));
        assertTrue(outputCapture.toString().contains("Auto-confirmed"));
    }

    public void testConfirmActionNoInput() {
        var service = createService("");
        var ex = assertThrows(IllegalStateException.class, () -> service.confirmAction(spec, new CommonOptions(), "Proceed?"));
        assertTrue(ex.getMessage().contains("No input available"));
    }

    public void testPromptForPasswordValid() {
        var service = createService("secret123\n");
        assertEquals("secret123", service.promptForPassword(spec, "Enter password:"));
    }

    public void testPromptForPasswordEmpty() {
        var service = createService("\n");
        var ex = assertThrows(RuntimeException.class, () -> service.promptForPassword(spec, "Enter password:"));
        assertEquals("Password cannot be empty.", ex.getMessage());
    }

    public void testPromptForPasswordNoInput() {
        var service = createService("");
        var ex = assertThrows(IllegalStateException.class, () -> service.promptForPassword(spec, "Enter password:"));
        assertTrue(ex.getMessage().contains("No input available"));
    }

    public void testPromptForPasswordWithConfirmationMatching() {
        var service = createService("pass123\npass123\n");
        assertEquals("pass123", service.promptForPasswordWithConfirmation(spec, new CommonOptions(), "Password:"));
    }

    public void testPromptForPasswordWithConfirmationNonMatching() {
        var service = createService("pass123\nwrong\n");
        var ex = assertThrows(
            RuntimeException.class,
            () -> service.promptForPasswordWithConfirmation(spec, new CommonOptions(), "Password:")
        );
        assertTrue(ex.getMessage().contains("Passwords do not match"));
    }

    public void testPromptForChoiceValidSelection() {
        var service = createService("2\n");
        assertEquals(2, service.promptForChoice(spec, 3, 1));
        assertTrue(outputCapture.toString().contains("Enter choice (1-3) [1]:"));
    }

    public void testPromptForChoiceDefaultSelection() {
        var service = createService("\n");
        assertEquals(1, service.promptForChoice(spec, 3, 1));
        assertTrue(outputCapture.toString().contains("Enter choice (1-3) [1]:"));
    }

    public void testPromptForChoiceNoInput() {
        var service = createService("");
        var ex = assertThrows(IllegalStateException.class, () -> service.promptForChoice(spec, 3, 1));
        assertTrue(ex.getMessage().contains("No input available"));
    }

    public void testPromptForChoiceMultipleInvalidAttempts() {
        var service = createService("invalid\n0\n10\n-1\n3\n");
        assertEquals(3, service.promptForChoice(spec, 5, 2));
        var output = outputCapture.toString();
        // Should show "Invalid choice" 4 times before accepting valid input
        assertEquals(4, output.split("Invalid choice\\.").length - 1);
    }

    public void testPromptForChoiceMinBoundaryValue() {
        var service = createService("1\n");
        assertEquals(1, service.promptForChoice(spec, 5, 3));
    }

    public void testPromptForChoiceMaxBoundaryValue() {
        var service = createService("5\n");
        assertEquals(5, service.promptForChoice(spec, 5, 3));
    }

    public void testPromptForChoiceInvalidChoiceCountZero() {
        var service = createService("1\n");
        var ex = expectThrows(AssertionError.class, () -> service.promptForChoice(spec, 0, 1));
        assertTrue(ex.getMessage().contains("Choice count must be greater than 0"));
    }

    public void testPromptForChoiceInvalidChoiceCountNegative() {
        var service = createService("1\n");
        var ex = expectThrows(AssertionError.class, () -> service.promptForChoice(spec, -1, 1));
        assertTrue(ex.getMessage().contains("Choice count must be greater than 0"));
    }

    public void testPromptForChoiceInvalidDefaultTooLow() {
        var service = createService("1\n");
        var ex = expectThrows(AssertionError.class, () -> service.promptForChoice(spec, 5, 0));
        assertTrue(ex.getMessage().contains("Default choice must be between 1 and 5"));
    }

    public void testPromptForChoiceInvalidDefaultTooHigh() {
        var service = createService("1\n");
        var ex = expectThrows(AssertionError.class, () -> service.promptForChoice(spec, 5, 6));
        assertTrue(ex.getMessage().contains("Default choice must be between 1 and 5"));
    }

    protected UserInteractionService createService(String input) {
        // Cache scanner outside anonymous class to maintain stream position across multiple getScanner() calls
        var scanner = new Scanner(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        return new UserInteractionService() {
            @Override
            protected Scanner getScanner() {
                return scanner;
            }
        };
    }

}
