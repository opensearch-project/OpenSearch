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
import org.opensearch.common.settings.ImportConfigCommand;

/**
 * CLI tool for upgrading from a supported Elasticsearch version to
 * an OpenSearch version.
 *
 */
public class UpgradeCli extends LoggingAwareMultiCommand {
    public UpgradeCli() {
        super("A tool for migrating from a supported Elasticsearch version to an OpenSearch version");
        subcommands.put("import-config", new ImportConfigCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new UpgradeCli().main(args, Terminal.DEFAULT));
    }
}
