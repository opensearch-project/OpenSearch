/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Security;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import picocli.CommandLine;

/**
 * Service for displaying information about available security providers.
 * Provides utilities to inspect and report on the security environment.
 */
public class SecurityProviderService {

    public static void printCurrentConfiguration(CommandLine.Model.CommandSpec spec) {
        var detailLog = new StringWriter();
        var writer = new PrintWriter(detailLog);
        var counter = new AtomicInteger();

        writer.println("Available Security Providers:");
        Arrays.stream(Security.getProviders())
            .peek(
                provider -> writer.printf(
                    Locale.ROOT,
                    "  %d. %s (version %s)\n",
                    counter.incrementAndGet(),
                    provider.getName(),
                    provider.getVersionStr()
                )
            )
            .flatMap(provider -> provider.getServices().stream().filter(service -> "KeyStore".equals(service.getType())))
            .forEach(service -> writer.printf(Locale.ROOT, "     └─ KeyStore.%s\n", service.getAlgorithm()));

        writer.flush();
        spec.commandLine().getOut().println(detailLog);
    }

}
