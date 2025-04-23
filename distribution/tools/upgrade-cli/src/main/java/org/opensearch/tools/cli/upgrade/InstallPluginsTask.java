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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Installs the list of plugins using the opensearch-plugin command.
*/
class InstallPluginsTask implements UpgradeTask {
    private static final String ERROR_MSG = "Error installing plugin %s. Please install it manually.";

    /** The list of official plugins that can be installed by the upgrade tool. */
    static final Set<String> OFFICIAL_PLUGINS;
    static {
        try (
            InputStream stream = InstallPluginsTask.class.getResourceAsStream("/plugins.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
        ) {
            Set<String> plugins = new HashSet<>();
            String line = reader.readLine();
            while (line != null) {
                plugins.add(line.trim());
                line = reader.readLine();
            }
            OFFICIAL_PLUGINS = Collections.unmodifiableSet(plugins);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();
        if (taskInput.getPlugins() == null || taskInput.getPlugins().isEmpty()) {
            return;
        }
        terminal.println("Installing core plugins ...");
        List<String> manualPlugins = new ArrayList<>();

        for (String plugin : taskInput.getPlugins()) {
            if (OFFICIAL_PLUGINS.contains(plugin)) {
                executeInstallPluginCommand(plugin, taskInput, terminal);
            } else {
                manualPlugins.add(plugin);
            }
        }
        if (!manualPlugins.isEmpty()) {
            terminal.println("Please install the following custom plugins manually: " + manualPlugins);
        }
        terminal.println("Success!" + System.lineSeparator());
    }

    // package private for unit testing
    void executeInstallPluginCommand(String plugin, TaskInput taskInput, Terminal terminal) {
        ProcessBuilder processBuilder = getProcessBuilderBasedOnOS(plugin, taskInput);
        try {
            final Process process = processBuilder.inheritIO().start();
            if (process.waitFor() != 0) {
                terminal.errorPrint(Terminal.Verbosity.NORMAL, String.format(Locale.getDefault(), ERROR_MSG, plugin));
            }
        } catch (IOException | InterruptedException e) {
            terminal.errorPrint(Terminal.Verbosity.NORMAL, String.format(Locale.getDefault(), ERROR_MSG, plugin) + e.getMessage());
        }
    }

    // package private for unit testing
    ProcessBuilder getProcessBuilderBasedOnOS(String plugin, TaskInput taskInput) {
        final String command = taskInput.getOpenSearchBin().resolve("opensearch-plugin") + " install " + plugin;
        final ProcessBuilder processBuilder = new ProcessBuilder();
        if (OS.WINDOWS == OS.current()) {
            processBuilder.command("cmd.exe", "/c", command);
        } else {
            processBuilder.command("sh", "-c", command);
        }
        return processBuilder;
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
