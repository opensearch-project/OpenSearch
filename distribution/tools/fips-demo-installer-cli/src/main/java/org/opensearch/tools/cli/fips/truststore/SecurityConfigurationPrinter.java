/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.security.Security;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import picocli.CommandLine;

/**
 * Service for displaying information about available security providers.
 * Provides utilities to inspect and report on the security environment.
 */
public class SecurityConfigurationPrinter {

    /**
     * Prints current security provider configuration to the console.
     *
     * @param spec the command specification for output
     */
    public static void printCurrentConfiguration(CommandLine.Model.CommandSpec spec) {
        var out = spec.commandLine().getOut();
        var counter = new AtomicInteger();
        out.println("Available Security Providers:");

        Arrays.stream(Security.getProviders())
            .peek(
                provider -> out.printf(
                    Locale.ROOT,
                    "  %d. %s (version %s)\n",
                    counter.incrementAndGet(),
                    provider.getName(),
                    provider.getVersionStr()
                )
            )
            .flatMap(provider -> provider.getServices().stream().filter(service -> "KeyStore".equals(service.getType())))
            .forEach(service -> out.printf(Locale.ROOT, "     └─ KeyStore.%s\n", service.getAlgorithm()));
    }

}
