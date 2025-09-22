/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.util.List;
import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Unmatched;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi.Style;
import picocli.CommandLine.Help.ColorScheme;

/**
 * Main command-line interface for the OpenSearch FIPS Demo Configuration Installer.
 * Provides interactive and programmatic trust store configuration options.
 */
@Command(name = "opensearch-fips-demo-installer", description = "OpenSearch FIPS Demo Configuration Installer", subcommands = {
    GeneratedTrustStoreCommand.class,
    SystemTrustStoreCommand.class,
    ShowProvidersCommand.class }, mixinStandardHelpOptions = true, usageHelpAutoWidth = true, headerHeading = "Usage:%n", synopsisHeading = "%n", descriptionHeading = "%nDescription:%n", parameterListHeading = "%nParameters:%n", optionListHeading = "%nOptions:%n", commandListHeading = "%nCommands:%n")
public class FipsTrustStoreCommand implements Callable<Integer> {

    private final static ColorScheme COLOR_SCHEME = new ColorScheme.Builder().commands(Style.bold, Style.underline)
        .options(Style.fg_yellow)
        .parameters(Style.fg_yellow)
        .optionParams(Style.italic)
        .errors(Style.fg_red, Style.bold)
        .stackTraces(Style.italic)
        .applySystemProperties() // optional: allow end users to customize
        .build();

    @Mixin
    CommonOptions common;

    @Unmatched
    private List<String> unmatchedArgs;

    @Override
    public Integer call() {
        // workaround for opensearch-env logic that scans all environment variables,
        // converts them to e.g. `-Ediscovery.type=single-node` and passes through as additional JVM params.
        if (unmatchedArgs != null && !unmatchedArgs.isEmpty()) {
            System.out.println("Warning: Ignoring unrecognized arguments: " + unmatchedArgs);
        }

        try {
            ConfigurationService.verifyJvmOptionsFile(common);
            return new TrustStoreService().executeInteractiveSelection(common);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new FipsTrustStoreCommand()).setColorScheme(COLOR_SCHEME).execute(args);
        System.exit(exitCode);
    }
}
