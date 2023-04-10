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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test;

import org.opensearch.action.admin.cluster.remote.RemoteInfoAction;
import org.opensearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.opensearch.client.Client;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteConnectionInfo;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransportPlugin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.opensearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public abstract class AbstractMultiClustersTestCase extends OpenSearchTestCase {
    public static final String LOCAL_CLUSTER = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    private static volatile ClusterGroup clusterGroup;

    protected Collection<String> remoteClusterAlias() {
        return randomSubsetOf(Arrays.asList("cluster-a", "cluster-b"));
    }

    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return Collections.emptyList();
    }

    protected final Client client() {
        return client(LOCAL_CLUSTER);
    }

    protected final Client client(String clusterAlias) {
        return cluster(clusterAlias).client();
    }

    protected final InternalTestCluster cluster(String clusterAlias) {
        return clusterGroup.getCluster(clusterAlias);
    }

    protected final Map<String, InternalTestCluster> clusters() {
        return Collections.unmodifiableMap(clusterGroup.clusters);
    }

    protected boolean reuseClusters() {
        return true;
    }

    @Before
    public final void startClusters() throws Exception {
        if (clusterGroup != null && reuseClusters()) {
            return;
        }
        stopClusters();
        final Map<String, InternalTestCluster> clusters = new HashMap<>();
        final List<String> clusterAliases = new ArrayList<>(remoteClusterAlias());
        clusterAliases.add(LOCAL_CLUSTER);
        for (String clusterAlias : clusterAliases) {
            final String clusterName = clusterAlias.equals(LOCAL_CLUSTER) ? "main-cluster" : clusterAlias;
            final int numberOfNodes = randomIntBetween(1, 3);
            final List<Class<? extends Plugin>> mockPlugins = Arrays.asList(
                MockHttpTransport.TestPlugin.class,
                MockTransportService.TestPlugin.class,
                MockNioTransportPlugin.class
            );
            final Collection<Class<? extends Plugin>> nodePlugins = nodePlugins(clusterAlias);
            final Settings nodeSettings = Settings.EMPTY;
            final NodeConfigurationSource nodeConfigurationSource = nodeConfigurationSource(nodeSettings, nodePlugins);
            final InternalTestCluster cluster = new InternalTestCluster(
                randomLong(),
                createTempDir(),
                true,
                true,
                numberOfNodes,
                numberOfNodes,
                clusterName,
                nodeConfigurationSource,
                0,
                clusterName + "-",
                mockPlugins,
                Function.identity()
            );
            cluster.beforeTest(random());
            clusters.put(clusterAlias, cluster);
        }
        clusterGroup = new ClusterGroup(clusters);
        configureAndConnectsToRemoteClusters();
    }

    @After
    public void assertAfterTest() throws Exception {
        for (InternalTestCluster cluster : clusters().values()) {
            cluster.wipe(Collections.emptySet());
            cluster.assertAfterTest();
        }
    }

    @AfterClass
    public static void stopClusters() throws IOException {
        IOUtils.close(clusterGroup);
        clusterGroup = null;
    }

    protected void disconnectFromRemoteClusters() throws Exception {
        Settings.Builder settings = Settings.builder();
        final Set<String> clusterAliases = clusterGroup.clusterAliases();
        for (String clusterAlias : clusterAliases) {
            if (clusterAlias.equals(LOCAL_CLUSTER) == false) {
                settings.putNull("cluster.remote." + clusterAlias + ".seeds");
            }
        }
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        assertBusy(() -> {
            for (TransportService transportService : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
                assertThat(transportService.getRemoteClusterService().getRegisteredRemoteClusterNames(), empty());
            }
        });
    }

    protected void configureAndConnectsToRemoteClusters() throws Exception {
        Map<String, List<String>> seedNodes = new HashMap<>();
        for (String clusterAlias : clusterGroup.clusterAliases()) {
            if (clusterAlias.equals(LOCAL_CLUSTER) == false) {
                final InternalTestCluster cluster = clusterGroup.getCluster(clusterAlias);
                final String[] allNodes = cluster.getNodeNames();
                final List<String> selectedNodes = randomSubsetOf(randomIntBetween(1, Math.min(3, allNodes.length)), allNodes);
                seedNodes.put(clusterAlias, selectedNodes);
            }
        }
        if (seedNodes.isEmpty()) {
            return;
        }
        Settings.Builder settings = Settings.builder();
        for (Map.Entry<String, List<String>> entry : seedNodes.entrySet()) {
            final String clusterAlias = entry.getKey();
            final String seeds = entry.getValue()
                .stream()
                .map(node -> cluster(clusterAlias).getInstance(TransportService.class, node).boundAddress().publishAddress().toString())
                .collect(Collectors.joining(","));
            settings.put("cluster.remote." + clusterAlias + ".seeds", seeds);
        }
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        assertBusy(() -> {
            List<RemoteConnectionInfo> remoteConnectionInfos = client().execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest())
                .actionGet()
                .getInfos()
                .stream()
                .filter(RemoteConnectionInfo::isConnected)
                .collect(Collectors.toList());
            final long totalConnections = seedNodes.values().stream().map(List::size).count();
            assertThat(remoteConnectionInfos, hasSize(Math.toIntExact(totalConnections)));
        });
    }

    static class ClusterGroup implements Closeable {
        private final Map<String, InternalTestCluster> clusters;

        ClusterGroup(Map<String, InternalTestCluster> clusters) {
            this.clusters = Collections.unmodifiableMap(clusters);
        }

        InternalTestCluster getCluster(String clusterAlias) {
            assertThat(clusters, hasKey(clusterAlias));
            return clusters.get(clusterAlias);
        }

        Set<String> clusterAliases() {
            return clusters.keySet();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(clusters.values());
        }
    }

    static NodeConfigurationSource nodeConfigurationSource(Settings nodeSettings, Collection<Class<? extends Plugin>> nodePlugins) {
        final Settings.Builder builder = Settings.builder();
        builder.putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()); // empty list disables a port scan for other nodes
        builder.putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file");
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        builder.put(nodeSettings);

        return new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return builder.build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return nodePlugins;
            }
        };
    }
}
