/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.util.Optional;

import picocli.CommandLine;

/**
 * Catches exceptions during command execution and prints only the error
 * message in a formatted way, making the CLI output cleaner and more user-friendly.
 */
public class FipsTrustStoreExceptionHandler implements CommandLine.IExecutionExceptionHandler {

    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine, CommandLine.ParseResult parseResult) {
        var errorMessage = Optional.ofNullable(ex.getMessage()).orElse("[null]");
        commandLine.getErr().println(commandLine.getColorScheme().errorText("Error: " + errorMessage));

        // Return the configured error exit code
        return commandLine.getCommandSpec().exitCodeOnExecutionException();
    }
}
