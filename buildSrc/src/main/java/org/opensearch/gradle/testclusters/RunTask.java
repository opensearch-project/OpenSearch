/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gradle.testclusters;

import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of the "run" Gradle task used in run.gradle
 */
public class RunTask extends DefaultTestClustersTask {

    private static final Logger logger = Logging.getLogger(RunTask.class);
    public static final String CUSTOM_SETTINGS_PREFIX = "tests.opensearch.";
    private static final int DEFAULT_HTTP_PORT = 9200;
    private static final int DEFAULT_TRANSPORT_PORT = 9300;
    private static final int DEFAULT_STREAM_PORT = 9880;
    private static final int DEFAULT_DEBUG_PORT = 5005;
    public static final String LOCALHOST_ADDRESS_PREFIX = "127.0.0.1:";

    private Boolean debug = false;

    private Boolean debugServer = false;

    private Boolean preserveData = false;

    private Path dataDir = null;

    private String keystorePassword = "";

    @Option(option = "debug-jvm", description = "Run OpenSearch as a debug client, where it will try to connect to a debugging server at startup.")
    public void setDebug(boolean enabled) {
        if (debugServer != null && debugServer == true) {
            throw new IllegalStateException("Either --debug-jvm or --debug-server-jvm option should be specified (but not both)");
        }
        this.debug = enabled;
    }

    @Option(option = "debug-server-jvm", description = "Run OpenSearch as a debug server that will accept connections from a debugging client.")
    public void setDebugServer(boolean enabled) {
        if (debug != null && debug == true) {
            throw new IllegalStateException("Either --debug-jvm or --debug-server-jvm option should be specified (but not both)");
        }
        this.debugServer = enabled;
    }

    @Input
    public Boolean getDebug() {
        return debug;
    }

    @Input
    public Boolean getDebugServer() {
        return debugServer;
    }

    @Option(option = "data-dir", description = "Override the base data directory used by the testcluster")
    public void setDataDir(String dataDirStr) {
        dataDir = Paths.get(dataDirStr).toAbsolutePath();
    }

    @Input
    public Boolean getPreserveData() {
        return preserveData;
    }

    @Option(option = "preserve-data", description = "Preserves data directory contents (path provided to --data-dir is always preserved)")
    public void setPreserveData(Boolean preserveData) {
        this.preserveData = preserveData;
    }

    @Option(option = "keystore-password", description = "Set the opensearch keystore password")
    public void setKeystorePassword(String password) {
        keystorePassword = password;
    }

    @Input
    @Optional
    public String getKeystorePassword() {
        return keystorePassword;
    }

    @Input
    @Optional
    public String getDataDir() {
        if (dataDir == null) {
            return null;
        }
        return dataDir.toString();
    }

    @Override
    public void beforeStart() {
        int debugPort = DEFAULT_DEBUG_PORT;
        int httpPort = DEFAULT_HTTP_PORT;
        int transportPort = DEFAULT_TRANSPORT_PORT;
        int streamPort = DEFAULT_STREAM_PORT;

        Map<String, String> additionalSettings = System.getProperties()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().toString().startsWith(CUSTOM_SETTINGS_PREFIX))
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().toString().substring(CUSTOM_SETTINGS_PREFIX.length()),
                    entry -> entry.getValue().toString()
                )
            );
        boolean singleNode = getClusters().stream().flatMap(c -> c.getNodes().stream()).count() == 1;
        final Function<OpenSearchNode, Path> getDataPath;
        if (singleNode) {
            getDataPath = n -> dataDir;
        } else {
            getDataPath = n -> dataDir.resolve(n.getName());
        }

        for (OpenSearchCluster cluster : getClusters()) {
            // Configure the first node with the default ports first
            OpenSearchNode firstNode = cluster.getFirstNode();
            firstNode.setHttpPort(String.valueOf(httpPort));
            httpPort++;
            firstNode.setTransportPort(String.valueOf(transportPort));
            firstNode.setStreamPort(String.valueOf(streamPort));
            transportPort++;
            streamPort++;
            firstNode.setting("discovery.seed_hosts", LOCALHOST_ADDRESS_PREFIX + DEFAULT_TRANSPORT_PORT);
            cluster.setPreserveDataDir(preserveData);
            for (OpenSearchNode node : cluster.getNodes()) {
                if (node != firstNode) {
                    node.setHttpPort(String.valueOf(httpPort));
                    httpPort++;
                    node.setTransportPort(String.valueOf(transportPort));
                    node.setStreamPort(String.valueOf(streamPort));
                    transportPort++;
                    streamPort++;
                    node.setting("discovery.seed_hosts", LOCALHOST_ADDRESS_PREFIX + DEFAULT_TRANSPORT_PORT);
                }
                additionalSettings.forEach(node::setting);
                if (dataDir != null) {
                    node.setDataPath(getDataPath.apply(node));
                }
                if (debug) {
                    logger.lifecycle(
                        "Running opensearch in debug mode (client), {} expecting running debug server on port {}",
                        node,
                        debugPort
                    );
                    node.jvmArgs("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + debugPort);
                    debugPort += 1;
                } else if (debugServer) {
                    logger.lifecycle("Running opensearch in debug mode (server), {} running server with debug port {}", node, debugPort);
                    node.jvmArgs("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=" + debugPort);
                    debugPort += 1;
                }
                if (keystorePassword.length() > 0) {
                    node.keystorePassword(keystorePassword);
                }
            }
        }
    }

    @TaskAction
    public void runAndWait() throws IOException {
        List<BufferedReader> toRead = new ArrayList<>();
        List<BooleanSupplier> aliveChecks = new ArrayList<>();
        try {
            for (OpenSearchCluster cluster : getClusters()) {
                for (OpenSearchNode node : cluster.getNodes()) {
                    BufferedReader reader = Files.newBufferedReader(node.getOpensearchStdoutFile());
                    toRead.add(reader);
                    aliveChecks.add(node::isProcessAlive);
                }
            }

            while (Thread.currentThread().isInterrupted() == false) {
                boolean readData = false;
                for (BufferedReader bufferedReader : toRead) {
                    if (bufferedReader.ready()) {
                        readData = true;
                        logger.lifecycle(bufferedReader.readLine());
                    }
                }

                if (aliveChecks.stream().allMatch(BooleanSupplier::getAsBoolean) == false) {
                    throw new GradleException("OpenSearch cluster died");
                }

                if (readData == false) {
                    // no data was ready to be consumed and rather than continuously spinning, pause
                    // for some time to avoid excessive CPU usage. Ideally we would use the JDK
                    // WatchService to receive change notifications but the WatchService does not have
                    // a native MacOS implementation and instead relies upon polling with possible
                    // delays up to 10s before a notification is received. See JDK-7133447.
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        } finally {
            Exception thrown = null;
            for (Closeable closeable : toRead) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    if (thrown == null) {
                        thrown = e;
                    } else {
                        thrown.addSuppressed(e);
                    }
                }
            }

            if (thrown != null) {
                logger.debug("exception occurred during close of stdout file readers", thrown);
            }
        }
    }
}
