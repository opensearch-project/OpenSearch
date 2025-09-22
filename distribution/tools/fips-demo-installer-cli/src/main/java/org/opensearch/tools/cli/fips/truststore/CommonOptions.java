/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import picocli.CommandLine.Option;

/**
 * Common command-line options shared across FIPS trust store commands.
 */
public class CommonOptions {

    @Option(names = { "-n", "--non-interactive" }, description = "Run in non-interactive mode (use defaults, no prompts)")
    boolean nonInteractive;

    @Option(names = { "-f", "--force" }, description = "Force installation even if FIPS demo configuration already exists")
    boolean force;
}
