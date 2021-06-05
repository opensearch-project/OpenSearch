/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import joptsimple.OptionSet;
import org.opensearch.cli.EnvironmentAwareCommand;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.env.Environment;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * CLI tool for upgrading to OpenSearch 1.0.0 from a supported Elasticsearch version.
 */
public class UpgradeCli extends EnvironmentAwareCommand {
    public UpgradeCli() {
        super("A CLI tool for upgrading to OpenSearch 1.0.0 from a supported Elasticsearch version.");
    }

    public static void main(String[] args) throws Exception {
        exit(new UpgradeCli().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        importConfig(terminal, env);
    }

    /**
     * Imports config from an existing elasticsearch installation.
     * <p>
     * The command requires the locations of both the source and destination config directories.
     * The default values of the config directories are read using the environment variables:
     * <ul>
     *  <li>ES_PATH_CONF</li>
     *  <li>OPENSEARCH_PATH_CONF</li>
     * </ul>
     */
    private void importConfig(Terminal terminal, Environment env) throws Exception {
        final Path esConfig = getEsConfigPath(terminal);
        final Path openSearchConfig = env.configFile();

        final ImportConfigOptions icOptions = new ImportConfigOptions(openSearchConfig, esConfig);
        final List<ConfigImporter> importers = ConfigImporter.getImporters(icOptions);

        for (ConfigImporter importer : importers) {
            importer.doImport(terminal);
        }

        terminal.println("Done!");
    }

    @SuppressForbidden(reason = "We need to read external es config files")
    private Path getEsConfigPath(Terminal terminal) throws UserException {
        String esConfigStr = System.getenv("ES_PATH_CONF");
        if (esConfigStr == null) {
            esConfigStr = terminal.readText("Missing ES_PATH_CONF env variable, provide the path to elasticsearch config directory: ");
            if (esConfigStr == null) {
                throw new UserException(ExitCodes.DATA_ERROR, "Invalid input for path to elasticsearch config directory.");
            }
        }
        Path esConfig = new File(esConfigStr).toPath();
        if (!Files.isDirectory(esConfig)) {
            throw new UserException(ExitCodes.DATA_ERROR, "The provided elasticsearch config is not a directory.");
        }
        return esConfig;
    }
}
