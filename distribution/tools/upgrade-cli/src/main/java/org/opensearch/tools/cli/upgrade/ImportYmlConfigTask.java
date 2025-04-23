/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import org.opensearch.cli.Terminal;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.xcontent.yaml.YamlXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Imports settings from an existing elasticsearch installation.
 */
class ImportYmlConfigTask implements UpgradeTask {
    private static final String ES_CONFIG_FILENAME = "elasticsearch.yml";
    private static final String OPENSEARCH_CONFIG_FILENAME = "opensearch.yml";
    static final String HEADER = "# ======================== OpenSearch Configuration =========================\n"
        + "# NOTE: The settings in this file are imported from an existing Elasticsearch\n"
        + "#       installation using the opensearch-upgrade tool. The original file is\n"
        + "#       backed up in this directory as opensearch.yml.bkp for reference.\n\n"
        + "# Please consult the documentation for further information:\n"
        + "# https://www.opensearch.org\n"
        + "#\n";

    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();
        try {
            terminal.println("Importing settings from elasticsearch.yml ...");
            final Path openSearchYmlPath = taskInput.getOpenSearchConfig().resolve(OPENSEARCH_CONFIG_FILENAME);
            final Path esYamlPath = taskInput.getEsConfig().resolve(ES_CONFIG_FILENAME);
            final Settings esSettings = Settings.builder().loadFromPath(esYamlPath).build();
            final Settings settings = Settings.builder().loadFromPath(openSearchYmlPath).build();
            if (esSettings.size() > 0) {
                if (settings.size() > 0
                    && terminal.promptYesNo("Existing settings in opensearch.yml will be overwritten, proceed?", false) == false) {
                    terminal.println("Import settings cancelled by user");
                }
                final Path backupYmlPath = taskInput.getOpenSearchConfig().resolve(OPENSEARCH_CONFIG_FILENAME + ".bkp");
                if (!Files.exists(backupYmlPath)
                    || terminal.promptYesNo("A backup file for opensearch.yml already exists, overwrite?", false)) {
                    Files.copy(openSearchYmlPath, backupYmlPath, StandardCopyOption.REPLACE_EXISTING);
                }
                Files.write(openSearchYmlPath, Collections.singleton(HEADER), StandardOpenOption.TRUNCATE_EXISTING);
                final Settings mergeSettings = mergeSettings(settings, esSettings);
                writeSettings(openSearchYmlPath, mergeSettings);
            }
            terminal.println("Success!" + System.lineSeparator());
        } catch (IOException ex) {
            throw new RuntimeException("Error importing settings from elasticsearch.yml, " + ex);
        }
    }

    // package private for unit testing
    Settings mergeSettings(final Settings first, final Settings second) {
        Settings.Builder builder = Settings.builder();
        for (String key : first.keySet()) {
            builder.copy(key, key, first);
        }
        for (String key : second.keySet()) {
            builder.copy(key, key, second);
        }
        return builder.build();
    }

    /**
     * Write settings to the config file on the file system. It uses the {@link XContentBuilder}
     * to build the YAML content and write it to the output stream.
     *
     * @param configYml path to a yml file where config will be written to.
     * @param settings  the settings to write
     * @throws IOException exception during writing to the output stream.
     */
    private void writeSettings(final Path configYml, final Settings settings) throws IOException {
        try (
            OutputStream os = Files.newOutputStream(configYml, StandardOpenOption.APPEND);
            XContentBuilder builder = new XContentBuilder(YamlXContent.yamlXContent, os)
        ) {
            builder.startObject();
            final Map<String, String> params = new HashMap<>();
            params.put("flat_settings", "true");
            settings.toXContent(builder, new ToXContent.MapParams(params));
            builder.endObject();
            builder.flush();
        } catch (Exception e) {
            throw new SettingsException("Failed to write settings to " + configYml.toString(), e);
        }
    }
}
