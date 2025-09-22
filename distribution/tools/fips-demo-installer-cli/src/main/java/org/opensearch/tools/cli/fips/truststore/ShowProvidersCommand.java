/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/**
 * Command for displaying available security providers.
 * Shows current security configuration and exits.
 */
@Command(name = "show-providers", description = "Show available security providers and exit", mixinStandardHelpOptions = true)
public class ShowProvidersCommand implements Callable<Integer> {

    @Mixin
    CommonOptions common;

    @Override
    public Integer call() {
        try {
            SecurityProviderService.printCurrentConfiguration();
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
