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
import picocli.CommandLine.Option;

/**
 * Command for configuring system PKCS11 trust store.
 * Enables the use of existing system trust store infrastructure.
 */
@Command(name = "system", description = "Use existing system PKCS11 trust store", mixinStandardHelpOptions = true)
public class SystemTrustStoreCommand implements Callable<Integer> {

    @CommandLine.Spec
    protected CommandLine.Model.CommandSpec spec;

    @CommandLine.Mixin
    protected CommonOptions common;

    @Option(names = { "--pkcs11-provider" }, description = "Specify PKCS11 provider name directly")
    private String preselectedPKCS11Provider;

    @Override
    public final Integer call() {
        var confPath = Path.of(System.getProperty("opensearch.path.conf"));
        ConfigurationService.verifyJvmOptionsFile(spec, common, confPath);
        return new TrustStoreService(UserInteractionService.getInstance()).useSystemTrustStore(
            spec,
            common,
            preselectedPKCS11Provider,
            confPath
        );
    }

}
