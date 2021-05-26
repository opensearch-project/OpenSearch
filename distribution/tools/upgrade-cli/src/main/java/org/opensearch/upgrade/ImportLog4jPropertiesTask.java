/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import org.opensearch.cli.Terminal;
import org.opensearch.common.collect.Tuple;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Imports Log4j properties from an existing elasticsearch installation.
 */
class ImportLog4jPropertiesTask implements UpgradeTask {
    private static final String LOG4J_PROPERTIES = "log4j.properties";

    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();
        try {
            terminal.println("Importing log4j.properties ...");
            final Path log4jProp = taskInput.getOpenSearchConfig().resolve(LOG4J_PROPERTIES);
            if (Files.exists(log4jProp)) {
                Files.copy(
                    log4jProp,
                    taskInput.getOpenSearchConfig().resolve(LOG4J_PROPERTIES + ".bkp"),
                    StandardCopyOption.REPLACE_EXISTING
                );
            }
            final Path esLog4jProp = taskInput.getEsConfig().resolve(LOG4J_PROPERTIES);
            if (Files.exists(esLog4jProp) && !Files.isDirectory(esLog4jProp)) {
                Files.copy(esLog4jProp, log4jProp, StandardCopyOption.REPLACE_EXISTING);
            }
            terminal.println("Success!" + System.lineSeparator());
        } catch (IOException e) {
            throw new RuntimeException("Error copying log4j properties. " + e);
        }
    }
}
