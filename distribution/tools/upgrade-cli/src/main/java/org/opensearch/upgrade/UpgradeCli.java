/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import org.opensearch.cli.LoggingAwareMultiCommand;
import org.opensearch.cli.Terminal;

/**
 * CLI tool for upgrading from a supported Elasticsearch version to
 * an OpenSearch version.
 *
 * - It will autodetect the presence of a running ES server
 * - If the ES Server is not running, it will need the path to the ES installation.
 * - Copy the configurations across
 * - Shutdown the ES server (if running)
 * - Start the OpenSearch server
 *
 */
public class UpgradeCli extends LoggingAwareMultiCommand {
    public UpgradeCli() {
        super("A tool for migrating from a supported Elasticsearch version to an OpenSearch version");
        subcommands.put("upgrade", new UpgradeToOpenSearchCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new UpgradeCli().main(args, Terminal.DEFAULT));
    }
}
