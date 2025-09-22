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
 * Command for generating a new BCFKS trust store from the JVM default trust store.
 * Provides a CLI interface for trust store generation operations.
 */
@Command(name = "generated", description = "Generate a new BCFKS trust store from JVM default trust store", mixinStandardHelpOptions = true)
public class GeneratedTrustStoreCommand implements Callable<Integer> {

    @Mixin
    CommonOptions common;

    @Override
    public Integer call() {
        try {
            ConfigurationService.verifyJvmOptionsFile(common);
            return new TrustStoreService().generateTrustStore(common);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

}
