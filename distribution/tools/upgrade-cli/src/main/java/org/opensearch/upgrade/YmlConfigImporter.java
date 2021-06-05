/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.upgrade.ImportConfigOptions.OPENSEARCH_CONFIG_FILENAME;

/**
 * Imports settings from an existing elasticsearch installation.
 */
class YmlConfigImporter implements ConfigImporter {
    private final ImportConfigOptions options;

    YmlConfigImporter(ImportConfigOptions options) {
        this.options = options;
    }

    @Override
    public void doImport(final Terminal terminal) throws UserException {
        terminal.println("Importing settings from elasticsearch.yml...");
        final String header = "#\n# ------------------------- Imported from Old Installation --------------------------\n#\n";
        try {
            final Path openSearchYmlPath = options.getOpenSearchYmlConfig();
            Files.copy(openSearchYmlPath, options.getOpenSearchConfig().resolve(OPENSEARCH_CONFIG_FILENAME + ".bkp"));
            final Path esYamlPath = options.getESYmlConfig();

            List<String> settings = Files.readAllLines(esYamlPath).stream().filter(s -> !s.startsWith("#")).collect(Collectors.toList());

            Files.write(openSearchYmlPath, header.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            Files.write(openSearchYmlPath, settings, StandardOpenOption.APPEND);
        } catch (IOException ex) {
            throw new UserException(ExitCodes.DATA_ERROR, ex.getMessage());
        }
    }
}
