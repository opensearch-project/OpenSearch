/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.cli.SuppressForbidden;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi.Style;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.Unmatched;

/**
 * Main command-line interface for the OpenSearch FIPS Demo Configuration Installer.
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
        .applySystemProperties()
        .build();

    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec spec;

    @CommandLine.Mixin
    protected CommonOptions common;

    @Unmatched
    @SuppressWarnings("unused")
    private List<String> unmatchedArgs;

    /**
     * Executes the main command logic, presenting an interactive trust store configuration menu.
     *
     * @return exit code (0 for success, 1 for failure)
     */
    @Override
    public final Integer call() {
        // workaround for opensearch-env logic that scans all environment variables,
        // converts them to e.g. `-Ediscovery.type=single-node` and passes through as additional JVM params.
        if (unmatchedArgs != null && !unmatchedArgs.isEmpty()) {
            spec.commandLine()
                .getOut()
                .println(
                    spec.commandLine()
                        .getColorScheme()
                        .ansi()
                        .string("@|yellow Warning: Ignoring unrecognized arguments: " + unmatchedArgs + "|@")
                );
        }

        var confPath = Path.of(System.getProperty("opensearch.path.conf"));
        ConfigurationService.verifyJvmOptionsFile(spec, common, confPath);
        return new TrustStoreService(UserInteractionService.getInstance()).executeInteractiveSelection(spec, common, confPath);
    }

    /**
     * Main entry point for the FIPS demo installer CLI.
     *
     * @param args command-line arguments
     */
    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    public static void main(String[] args) {
        int exitCode = new CommandLine(new FipsTrustStoreCommand()).setColorScheme(COLOR_SCHEME)
            .setExecutionExceptionHandler(new FipsTrustStoreExceptionHandler())
            .execute(args);
        System.exit(exitCode);
    }
}
