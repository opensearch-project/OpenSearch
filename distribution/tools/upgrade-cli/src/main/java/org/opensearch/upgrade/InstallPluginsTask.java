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
import java.util.Locale;

/**
 * Installs the list of plugins using the opensearch-plugin command.
 */
class InstallPluginsTask implements UpgradeTask {
    private static final String ERROR_MSG = "Error installing plugin %s. Please install it manually.";

    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();
        if (taskInput.getPlugins() == null || taskInput.getPlugins().isEmpty()) {
            return;
        }
        terminal.println("Installing core plugins ...");
        final ProcessBuilder processBuilder = new ProcessBuilder();
        for (String plugin : taskInput.getPlugins()) {
            // TODO - validate if plugin is an official plugin for OpenSearch.
            final String command = taskInput.getOpenSearchBin().resolve("opensearch-plugin") + " install " + plugin;
            if (OS.WINDOWS == OS.current()) {
                processBuilder.command("cmd.exe", "/c", command);
            } else {
                processBuilder.command("sh", "-c", command);
            }
            try {
                final Process process = processBuilder.inheritIO().start();
                if (process.waitFor() != 0) {
                    terminal.errorPrint(Terminal.Verbosity.NORMAL, String.format(Locale.getDefault(), ERROR_MSG, plugin));
                }
            } catch (IOException | InterruptedException e) {
                terminal.errorPrint(Terminal.Verbosity.NORMAL, String.format(Locale.getDefault(), ERROR_MSG, plugin) + e.getMessage());
            }
        }
        terminal.println("Success!" + System.lineSeparator());
    }

    private enum OS {
        WINDOWS,
        MAC,
        LINUX;

        public static OS current() {
            final String os = System.getProperty("os.name", "");
            if (os.startsWith("Windows")) {
                return OS.WINDOWS;
            }
            if (os.startsWith("Linux") || os.startsWith("LINUX")) {
                return OS.LINUX;
            }
            if (os.startsWith("Mac")) {
                return OS.MAC;
            }
            throw new IllegalStateException("Can't determine OS from: " + os);
        }
    }
}
