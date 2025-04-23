/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.cli.Terminal;
import org.opensearch.common.collect.Tuple;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Validates the input {@link TaskInput} for the upgrade.
 */
class ValidateInputTask implements UpgradeTask {

    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();

        terminal.println("Verifying the details ...");
        // check if the elasticsearch version is supported
        if (taskInput.getVersion().isPresent()) {
            final Version version = taskInput.getVersion().get();
            if (version.equals(LegacyESVersion.fromId(7100299)) == false) {
                throw new RuntimeException(
                    String.format(Locale.getDefault(), "The installed version %s of elasticsearch is not supported.", version)
                );
            }
        } else {
            terminal.println("Unable to detect installed elasticsearch version.");
            confirmToProceed(terminal);
        }
        // check if the OpenSearch config is set to an external location
        if (taskInput.getOpenSearchConfig().getParent().equals(taskInput.getOpenSearchBin().getParent())) {
            terminal.println(
                "OpenSearch config directory is set inside the installation directory. "
                    + "It is recommended to use an external config directory and set the environment variable "
                    + "OPENSEARCH_PATH_CONF to it."
            );
            confirmToProceed(terminal);
        }

        // print summary and confirm with user if everything looks correct.
        final Map<String, String> fieldsMap = getSummaryFieldsMap(taskInput);
        final String format = " %-25s | %s";
        terminal.println("+----------------------- SUMMARY -----------------------+");
        for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
            terminal.println(String.format(Locale.getDefault(), format, entry.getKey(), entry.getValue()));
        }
        terminal.println("+-------------------------------------------------------+");
        terminal.println("Please verify if everything above looks good.");
        confirmToProceed(terminal);
    }

    private void confirmToProceed(final Terminal terminal) {
        terminal.println(System.lineSeparator());
        if (terminal.promptYesNo("Do you want to proceed?", false) == false) {
            throw new RuntimeException("Upgrade cancelled by user.");
        }
    }

    // package private for unit testing
    Map<String, String> getSummaryFieldsMap(final TaskInput taskInput) {
        final String version = taskInput.getVersion().isPresent() ? taskInput.getVersion().get().toString() : "unknown";

        final Map<String, String> fields = new LinkedHashMap<>();
        fields.put("Cluster", taskInput.getCluster());
        fields.put("Node", taskInput.getNode());
        fields.put("Endpoint", taskInput.getBaseUrl());
        fields.put("Elasticsearch Version", version);
        fields.put("Elasticsearch Config", taskInput.getEsConfig().toString());
        fields.put("Elasticsearch Plugins", taskInput.getPlugins() == null ? "[]" : taskInput.getPlugins().toString());
        fields.put("OpenSearch Config", taskInput.getOpenSearchConfig().toString());

        return fields;
    }
}
