/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import org.opensearch.Version;
import org.opensearch.env.Environment;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * A plain old java object, that contains the information used by tasks
 * in the upgrade process.
 */
class TaskInput {
    private final Environment openSearchEnv;
    private String node;
    private String cluster;
    private String baseUrl;
    private boolean running;
    private Version version;
    private List<String> plugins;
    private Path esHome;
    private Path esConfig;

    TaskInput(Environment openSearchEnv) {
        this.openSearchEnv = openSearchEnv;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public Optional<Version> getVersion() {
        return Optional.ofNullable(version);
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public List<String> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<String> plugins) {
        this.plugins = plugins;
    }

    public Path getEsConfig() {
        return esConfig;
    }

    public void setEsConfig(Path esConfig) {
        this.esConfig = esConfig;
    }

    public Path getEsHome() {
        return esHome;
    }

    public void setEsHome(Path esHome) {
        this.esHome = esHome;
    }

    public Path getOpenSearchConfig() {
        return openSearchEnv.configDir();
    }

    public Path getOpenSearchBin() {
        return openSearchEnv.binDir();
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }
}
