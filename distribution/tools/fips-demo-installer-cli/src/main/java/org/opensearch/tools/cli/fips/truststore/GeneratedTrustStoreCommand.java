/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Command for generating a new BCFKS trust store from the JVM default trust store.
 * Provides a CLI interface for trust store generation operations.
 */
@Command(name = "generated", description = "Generate a new BCFKS trust store from JVM default trust store", mixinStandardHelpOptions = true)
public class GeneratedTrustStoreCommand implements Callable<Integer> {

    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec spec;

    @CommandLine.Mixin
    protected CommonOptions common;

    @Override
    public final Integer call() throws Exception {
        var confPath = Path.of(System.getProperty("opensearch.path.conf"));
        ConfigurationService.verifyJvmOptionsFile(spec, common, confPath);
        return new TrustStoreService(UserInteractionService.getInstance()).generateTrustStore(spec, common, confPath);
    }

}
