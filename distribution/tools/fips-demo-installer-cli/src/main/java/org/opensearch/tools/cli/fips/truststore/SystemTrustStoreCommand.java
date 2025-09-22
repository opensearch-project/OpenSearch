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
import picocli.CommandLine.Option;

/**
 * Command for configuring system PKCS11 trust store.
 * Enables the use of existing system trust store infrastructure.
 */
@Command(name = "system", description = "Use existing system PKCS11 trust store", mixinStandardHelpOptions = true)
public class SystemTrustStoreCommand implements Callable<Integer> {

    @Mixin
    CommonOptions common;

    @Option(names = { "--pkcs11-provider" }, description = "Specify PKCS11 provider name directly")
    private String pkcs11Provider;

    @Override
    public Integer call() {
        try {
            ConfigurationService.verifyJvmOptionsFile(common);
            return new TrustStoreService().useSystemTrustStore(common, pkcs11Provider);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

}
