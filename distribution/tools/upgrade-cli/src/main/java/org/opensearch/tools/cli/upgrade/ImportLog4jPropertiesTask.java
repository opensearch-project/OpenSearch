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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Properties;

/**
 * Imports Log4j properties from an existing elasticsearch installation.
 */
class ImportLog4jPropertiesTask implements UpgradeTask {
    static final String LOG4J_PROPERTIES = "log4j2.properties";

    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();
        try {
            terminal.println("Importing log4j.properties ...");
            final Path log4jPropPath = taskInput.getOpenSearchConfig().resolve(LOG4J_PROPERTIES);
            if (Files.exists(log4jPropPath)) {
                Files.copy(
                    log4jPropPath,
                    taskInput.getOpenSearchConfig().resolve(LOG4J_PROPERTIES + ".bkp"),
                    StandardCopyOption.REPLACE_EXISTING
                );
            }
            final Path esLog4jPropPath = taskInput.getEsConfig().resolve(LOG4J_PROPERTIES);
            try (
                InputStream esLog4jIs = Files.newInputStream(esLog4jPropPath);
                OutputStream log4jOs = Files.newOutputStream(log4jPropPath, StandardOpenOption.TRUNCATE_EXISTING)
            ) {
                final Properties esLog4JProps = new Properties();
                esLog4JProps.load(esLog4jIs);
                final Properties log4jProps = renameValues(esLog4JProps);

                log4jProps.store(log4jOs, "This is an auto-generated file imported from an existing elasticsearch installation.");
            }
            terminal.println("Success!" + System.lineSeparator());
        } catch (IOException e) {
            throw new RuntimeException("Error copying log4j properties. " + e);
        }
    }

    /**
     * Rename the values for OpenSearch log4j properties to reflect the changed names
     * for java packages, class names and system variables.
     *
     * @param esLog4JProps existing elasticsearch log4j properties.
     * @return updated properties for OpenSearch.
     */
    private Properties renameValues(Properties esLog4JProps) {
        final Properties props = new Properties();
        for (Map.Entry<Object, Object> entry : esLog4JProps.entrySet()) {
            final String key = (String) entry.getKey();
            final String value = (String) entry.getValue();
            final String newKey = key.replaceAll("esmessagefields", "opensearchmessagefields");
            final String newValue = value.replaceAll("ESJsonLayout", "OpenSearchJsonLayout")
                .replaceAll("sys:es.logs", "sys:opensearch.logs")
                .replaceAll("org.elasticsearch", "org.opensearch");
            props.setProperty(newKey, newValue);
        }
        return props;
    }
}
