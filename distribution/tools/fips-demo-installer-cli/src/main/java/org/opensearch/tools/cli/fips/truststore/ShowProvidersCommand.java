/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Command for displaying available security providers.
 * Shows current security configuration and exits.
 */
@Command(name = "show-providers", description = "Show available security providers and exit", mixinStandardHelpOptions = true)
public class ShowProvidersCommand implements Callable<Integer> {

    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec spec;

    @Override
    public final Integer call() {
        SecurityConfigurationPrinter.printCurrentConfiguration(spec);
        return 0;
    }
}
