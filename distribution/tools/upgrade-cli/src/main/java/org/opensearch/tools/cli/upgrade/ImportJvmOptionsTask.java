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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Imports JVM options from an existing elasticsearch installation.
 */
class ImportJvmOptionsTask implements UpgradeTask {
    private static final String JVM_OPTIONS_D = "jvm.options.d";

    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();
        try {
            terminal.println("Importing JVM options ...");
            final Path jvmOptionsDir = taskInput.getOpenSearchConfig().resolve(JVM_OPTIONS_D);
            if (!Files.exists(jvmOptionsDir)) {
                Files.createDirectory(jvmOptionsDir);
            }

            final Path esJvmOptionsDir = taskInput.getEsConfig().resolve(JVM_OPTIONS_D);
            if (Files.exists(esJvmOptionsDir) && Files.isDirectory(esJvmOptionsDir)) {
                final List<Path> esJvmOptionsFiles = Files.list(esJvmOptionsDir).collect(Collectors.toList());
                for (Path esJvmOptFile : esJvmOptionsFiles) {
                    final Path jvmOptFile = jvmOptionsDir.resolve(esJvmOptFile.getFileName().toString());
                    Files.copy(esJvmOptFile, jvmOptFile, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            terminal.println("Success!" + System.lineSeparator());
        } catch (Exception e) {
            throw new RuntimeException("Error importing JVM options. " + e);
        }
    }
}
