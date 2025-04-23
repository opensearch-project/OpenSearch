/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.Version;
import org.opensearch.cli.Terminal;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

/**
 * Looks for an existing elasticsearch installation. First it tries to identify automatically,
 * and if unsuccessful, asks the user to input the missing details.
 * <p>
 * If an elasticsearch installation can not be found, throws a runtime error which fails the
 * upgrade task.
 */
class DetectEsInstallationTask implements UpgradeTask {
    private static final int ES_DEFAULT_PORT = 9200;
    private static final String ES_CONFIG_ENV = "ES_PATH_CONF";
    private static final String ES_CONFIG_YML = "elasticsearch.yml";
    private static final String ES_HOME = "ES_HOME";

    @SuppressForbidden(reason = "We need to read external es config files")
    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();
        try {
            terminal.println("Looking for an elasticsearch installation ...");
            String esHomeEnv = System.getenv(ES_HOME);
            if (esHomeEnv == null) {
                esHomeEnv = terminal.readText("Missing ES_HOME env variable, enter the path to elasticsearch home: ");
                if (esHomeEnv == null || esHomeEnv.isEmpty()) {
                    throw new RuntimeException("Invalid input for path to elasticsearch home directory.");
                }
            }
            taskInput.setEsHome(new File(esHomeEnv).toPath());

            String esConfEnv = System.getenv(ES_CONFIG_ENV);
            if (esConfEnv == null) {
                esConfEnv = terminal.readText("Missing ES_PATH_CONF env variable, enter the path to elasticsearch config directory: ");
                if (esConfEnv == null || esHomeEnv.isEmpty()) {
                    throw new RuntimeException("Invalid input for path to elasticsearch config directory.");
                }
            }
            taskInput.setEsConfig(new File(esConfEnv).toPath());

            final Settings esSettings = Settings.builder().loadFromPath(taskInput.getEsConfig().resolve(ES_CONFIG_YML)).build();
            final String url = retrieveUrl(esSettings);
            taskInput.setBaseUrl(url);
            final boolean running = isRunning(url);
            taskInput.setRunning(running);
            if (running) {
                terminal.println("Found a running instance of elasticsearch at " + url);
                taskInput.setRunning(true);
                try {
                    updateTaskInput(taskInput, fetchInfoFromUrl(taskInput.getBaseUrl()));
                } catch (RuntimeException e) {
                    updateTaskInput(taskInput, fetchInfoFromEsSettings(esSettings));
                }
                try {
                    taskInput.setPlugins(fetchPluginsFromUrl(taskInput.getBaseUrl()));
                } catch (RuntimeException e) {
                    taskInput.setPlugins(detectPluginsFromEsHome(taskInput.getEsHome()));
                }
            } else {
                terminal.println("Did not find a running instance of elasticsearch at " + url);
                updateTaskInput(taskInput, fetchInfoFromEsSettings(esSettings));
                taskInput.setPlugins(detectPluginsFromEsHome(taskInput.getEsHome()));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error detecting existing elasticsearch installation. " + e);
        }
    }

    @SuppressWarnings("unchecked")
    private void updateTaskInput(TaskInput taskInput, Map<?, ?> response) {
        final Map<String, String> versionMap = (Map<String, String>) response.get("version");
        if (versionMap != null) {
            final String vStr = versionMap.get("number");
            if (vStr != null) {
                taskInput.setVersion(Version.fromString(vStr));
            }
        }
        taskInput.setNode((String) response.get("name"));
        taskInput.setCluster((String) response.get("cluster_name"));
    }

    // package private for unit testing
    String retrieveUrl(final Settings esSettings) {
        final int port = Optional.ofNullable(esSettings.get("http.port")).map(this::extractPort).orElse(ES_DEFAULT_PORT);
        return "http://localhost:" + port;
    }

    private Integer extractPort(final String port) {
        try {
            return Integer.parseInt(port.trim());
        } catch (Exception ex) {
            return ES_DEFAULT_PORT;
        }
    }

    @SuppressForbidden(reason = "Need to connect to http endpoint for elasticsearch.")
    private boolean isRunning(final String url) {
        try {
            final URL esUrl = new URL(url);
            final HttpURLConnection conn = (HttpURLConnection) esUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(1000);
            conn.connect();
            return conn.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    @SuppressForbidden(reason = "Retrieve information on the installation.")
    private Map<?, ?> fetchInfoFromUrl(final String url) {
        try {
            final URL esUrl = new URL(url);
            final HttpURLConnection conn = (HttpURLConnection) esUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(1000);
            conn.connect();

            final StringBuilder json = new StringBuilder();
            final Scanner scanner = new Scanner(esUrl.openStream());
            while (scanner.hasNext()) {
                json.append(scanner.nextLine());
            }
            scanner.close();
            final ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json.toString(), Map.class);
        } catch (IOException e) {
            throw new RuntimeException("Error retrieving elasticsearch cluster info, " + e);
        }
    }

    private Map<?, ?> fetchInfoFromEsSettings(final Settings esSettings) throws IOException {
        final Map<String, String> info = new HashMap<>();
        final String node = esSettings.get("node.name") != null ? esSettings.get("node.name") : "unknown";
        final String cluster = esSettings.get("cluster.name") != null ? esSettings.get("cluster.name") : "unknown";
        info.put("name", node);
        info.put("cluster_name", cluster);
        return info;
    }

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "Retrieve information on installed plugins.")
    private List<String> fetchPluginsFromUrl(final String url) {
        final List<String> plugins = new ArrayList<>();
        try {
            final URL esUrl = new URL(url + "/_cat/plugins?format=json&local=true");
            final HttpURLConnection conn = (HttpURLConnection) esUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(1000);
            conn.connect();
            if (conn.getResponseCode() == 200) {
                final StringBuilder json = new StringBuilder();
                final Scanner scanner = new Scanner(esUrl.openStream());
                while (scanner.hasNext()) {
                    json.append(scanner.nextLine());
                }
                scanner.close();
                final ObjectMapper mapper = new ObjectMapper();
                final Map<String, String>[] response = mapper.readValue(json.toString(), Map[].class);
                for (Map<String, String> plugin : response) {
                    plugins.add(plugin.get("component"));
                }
            }
            return plugins;
        } catch (IOException e) {
            throw new RuntimeException("Error retrieving elasticsearch plugin details, " + e);
        }
    }

    private List<String> detectPluginsFromEsHome(final Path esHome) {
        // list out the contents of the plugins directory under esHome
        return Collections.emptyList();
    }
}
