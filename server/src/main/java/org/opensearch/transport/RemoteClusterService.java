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

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.opensearch.common.settings.Setting.boolSetting;
import static org.opensearch.common.settings.Setting.timeSetting;

/**
 * Basic service for accessing remote clusters via gateway nodes
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class RemoteClusterService extends RemoteClusterAware implements Closeable {

    private final Logger logger = LogManager.getLogger(RemoteClusterService.class);

    /**
     * The initial connect timeout for remote cluster connections
     */
    public static final Setting<TimeValue> REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "cluster.remote.initial_connect_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    /**
     * The name of a node attribute to select nodes that should be connected to in the remote cluster.
     * For instance a node can be configured with {@code node.attr.gateway: true} in order to be eligible as a gateway node between
     * clusters. In that case {@code cluster.remote.node.attr: gateway} can be used to filter out other nodes in the remote cluster.
     * The value of the setting is expected to be a boolean, {@code true} for nodes that can become gateways, {@code false} otherwise.
     */
    public static final Setting<String> REMOTE_NODE_ATTRIBUTE = Setting.simpleString(
        "cluster.remote.node.attr",
        Setting.Property.NodeScope
    );

    /**
     * If <code>true</code> connecting to remote clusters is supported on this node. If <code>false</code> this node will not establish
     * connections to any remote clusters configured. Search requests executed against this node (where this node is the coordinating node)
     * will fail if remote cluster syntax is used as an index pattern. The default is <code>true</code>
     */
    public static final Setting<Boolean> ENABLE_REMOTE_CLUSTERS = Setting.boolSetting(
        "cluster.remote.connect",
        true,
        Setting.Property.Deprecated,
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<Boolean> REMOTE_CLUSTER_SKIP_UNAVAILABLE = Setting.affixKeySetting(
        "cluster.remote.",
        "skip_unavailable",
        (ns, key) -> boolSetting(key, false, new RemoteConnectionEnabled<>(ns, key), Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> REMOTE_CLUSTER_PING_SCHEDULE = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.ping_schedule",
        (ns, key) -> timeSetting(
            key,
            TransportSettings.PING_SCHEDULE,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    public static final Setting.AffixSetting<Boolean> REMOTE_CLUSTER_COMPRESS = Setting.affixKeySetting(
        "cluster.remote.",
        "transport.compress",
        (ns, key) -> boolSetting(
            key,
            TransportSettings.TRANSPORT_COMPRESS,
            new RemoteConnectionEnabled<>(ns, key),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    private final boolean enabled;

    public boolean isEnabled() {
        return enabled;
    }

    private final TransportService transportService;
    private final Map<String, RemoteClusterConnection> remoteClusters = ConcurrentCollections.newConcurrentMap();

    RemoteClusterService(Settings settings, TransportService transportService) {
        super(settings);
        this.enabled = DiscoveryNode.isRemoteClusterClient(settings);
        this.transportService = transportService;
    }

    /**
     * Returns <code>true</code> if at least one remote cluster is configured
     */
    public boolean isCrossClusterSearchEnabled() {
        return remoteClusters.isEmpty() == false;
    }

    boolean isRemoteNodeConnected(final String remoteCluster, final DiscoveryNode node) {
        return remoteClusters.get(remoteCluster).isNodeConnected(node);
    }

    public Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indices, Predicate<String> indexExists) {
        Map<String, OriginalIndices> originalIndicesMap = new HashMap<>();
        if (isCrossClusterSearchEnabled()) {
            final Map<String, List<String>> groupedIndices = groupClusterIndices(getRemoteClusterNames(), indices, indexExists);
            if (groupedIndices.isEmpty()) {
                // search on _all in the local cluster if neither local indices nor remote indices were specified
                originalIndicesMap.put(LOCAL_CLUSTER_GROUP_KEY, new OriginalIndices(Strings.EMPTY_ARRAY, indicesOptions));
            } else {
                for (Map.Entry<String, List<String>> entry : groupedIndices.entrySet()) {
                    String clusterAlias = entry.getKey();
                    List<String> originalIndices = entry.getValue();
                    originalIndicesMap.put(clusterAlias, new OriginalIndices(originalIndices.toArray(new String[0]), indicesOptions));
                }
            }
        } else {
            originalIndicesMap.put(LOCAL_CLUSTER_GROUP_KEY, new OriginalIndices(indices, indicesOptions));
        }
        return originalIndicesMap;
    }

    /**
     * Returns <code>true</code> iff the given cluster is configured as a remote cluster. Otherwise <code>false</code>
     */
    boolean isRemoteClusterRegistered(String clusterName) {
        return remoteClusters.containsKey(clusterName);
    }

    /**
     * Returns the registered remote cluster names.
     */
    public Set<String> getRegisteredRemoteClusterNames() {
        // remoteClusters is unmodifiable so its key set will be unmodifiable too
        return remoteClusters.keySet();
    }

    /**
     * Returns a connection to the given node on the given remote cluster
     *
     * @throws IllegalArgumentException if the remote cluster is unknown
     */
    public Transport.Connection getConnection(DiscoveryNode node, String cluster) {
        return getRemoteClusterConnection(cluster).getConnection(node);
    }

    /**
     * Ensures that the given cluster alias is connected. If the cluster is connected this operation
     * will invoke the listener immediately.
     */
    void ensureConnected(String clusterAlias, ActionListener<Void> listener) {
        getRemoteClusterConnection(clusterAlias).ensureConnected(listener);
    }

    /**
     * Returns whether the cluster identified by the provided alias is configured to be skipped when unavailable
     */
    public boolean isSkipUnavailable(String clusterAlias) {
        return getRemoteClusterConnection(clusterAlias).isSkipUnavailable();
    }

    public Transport.Connection getConnection(String cluster) {
        return getRemoteClusterConnection(cluster).getConnection();
    }

    RemoteClusterConnection getRemoteClusterConnection(String cluster) {
        if (enabled == false) {
            throw new IllegalArgumentException(
                "this node does not have the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role"
            );
        }
        RemoteClusterConnection connection = remoteClusters.get(cluster);
        if (connection == null) {
            throw new NoSuchRemoteClusterException(cluster);
        }
        return connection;
    }

    Set<String> getRemoteClusterNames() {
        return this.remoteClusters.keySet();
    }

    @Override
    public void listenForUpdates(ClusterSettings clusterSettings) {
        super.listenForUpdates(clusterSettings);
        clusterSettings.addAffixUpdateConsumer(REMOTE_CLUSTER_SKIP_UNAVAILABLE, this::updateSkipUnavailable, (alias, value) -> {});
    }

    private synchronized void updateSkipUnavailable(String clusterAlias, Boolean skipUnavailable) {
        RemoteClusterConnection remote = this.remoteClusters.get(clusterAlias);
        if (remote != null) {
            remote.updateSkipUnavailable(skipUnavailable);
        }
    }

    @Override
    protected void updateRemoteCluster(String clusterAlias, Settings settings) {
        CountDownLatch latch = new CountDownLatch(1);
        updateRemoteCluster(clusterAlias, settings, ActionListener.wrap(latch::countDown));

        try {
            // Wait 10 seconds for a connections. We must use a latch instead of a future because we
            // are on the cluster state thread and our custom future implementation will throw an
            // assertion.
            if (latch.await(10, TimeUnit.SECONDS) == false) {
                logger.error("failed to connect to new remote cluster {} within {}", clusterAlias, TimeValue.timeValueSeconds(10));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * This method updates the list of remote clusters. It's intended to be used as an update consumer on the settings infrastructure
     *
     * @param clusterAlias a cluster alias to discovery node mapping representing the remote clusters seeds nodes
     * @param newSettings the updated settings for the remote connection
     * @param listener a listener invoked once every configured cluster has been connected to
     */
    synchronized void updateRemoteCluster(String clusterAlias, Settings newSettings, ActionListener<Void> listener) {
        if (LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            throw new IllegalArgumentException("remote clusters must not have the empty string as its key");
        }

        RemoteClusterConnection remote = this.remoteClusters.get(clusterAlias);
        if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, newSettings) == false) {
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                logger.warn("failed to close remote cluster connections for cluster: " + clusterAlias, e);
            }
            remoteClusters.remove(clusterAlias);
            listener.onResponse(null);
            return;
        }

        if (remote == null) {
            // this is a new cluster we have to add a new representation
            Settings finalSettings = Settings.builder().put(this.settings, false).put(newSettings, false).build();
            remote = new RemoteClusterConnection(finalSettings, clusterAlias, transportService);
            remoteClusters.put(clusterAlias, remote);
            remote.ensureConnected(listener);
        } else if (remote.shouldRebuildConnection(newSettings)) {
            // Changes to connection configuration. Must tear down existing connection
            try {
                IOUtils.close(remote);
            } catch (IOException e) {
                logger.warn("failed to close remote cluster connections for cluster: " + clusterAlias, e);
            }
            remoteClusters.remove(clusterAlias);
            Settings finalSettings = Settings.builder().put(this.settings, false).put(newSettings, false).build();
            remote = new RemoteClusterConnection(finalSettings, clusterAlias, transportService);
            remoteClusters.put(clusterAlias, remote);
            remote.ensureConnected(listener);
        } else {
            // No changes to connection configuration.
            listener.onResponse(null);
        }
    }

    /**
     * Connects to all remote clusters in a non-blocking fashion. This should be called on node startup to establish an initial connection
     * to all configured seed nodes.
     */
    void initializeRemoteClusters(ActionListener<Void> listener) {
        Set<String> enabledClusters = RemoteClusterAware.getEnabledRemoteClusters(settings);
        if (enabledClusters.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        GroupedActionListener<Void> groupListener = new GroupedActionListener<>(
            ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure),
            enabledClusters.size()
        );

        for (String clusterAlias : enabledClusters) {
            updateRemoteCluster(clusterAlias, settings, groupListener);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(remoteClusters.values());
    }

    public Stream<RemoteConnectionInfo> getRemoteConnectionInfos() {
        return remoteClusters.values().stream().map(RemoteClusterConnection::getConnectionInfo);
    }

    /**
     * Collects all nodes of the given clusters and returns / passes a (clusterAlias, nodeId) to {@link DiscoveryNode}
     * function on success.
     */
    public void collectNodes(Set<String> clusters, ActionListener<BiFunction<String, String, DiscoveryNode>> listener) {
        if (enabled == false) {
            throw new IllegalArgumentException(
                "this node does not have the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role"
            );
        }
        Map<String, RemoteClusterConnection> remoteClusters = this.remoteClusters;
        for (String cluster : clusters) {
            if (remoteClusters.containsKey(cluster) == false) {
                listener.onFailure(new NoSuchRemoteClusterException(cluster));
                return;
            }
        }

        final Map<String, Function<String, DiscoveryNode>> clusterMap = new HashMap<>();
        CountDown countDown = new CountDown(clusters.size());
        Function<String, DiscoveryNode> nullFunction = s -> null;
        for (final String cluster : clusters) {
            RemoteClusterConnection connection = remoteClusters.get(cluster);
            connection.collectNodes(new ActionListener<Function<String, DiscoveryNode>>() {
                @Override
                public void onResponse(Function<String, DiscoveryNode> nodeLookup) {
                    synchronized (clusterMap) {
                        clusterMap.put(cluster, nodeLookup);
                    }
                    if (countDown.countDown()) {
                        listener.onResponse((clusterAlias, nodeId) -> clusterMap.getOrDefault(clusterAlias, nullFunction).apply(nodeId));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (countDown.fastForward()) { // we need to check if it's true since we could have multiple failures
                        listener.onFailure(e);
                    }
                }
            });
        }
    }

    /**
     * Returns a client to the remote cluster if the given cluster alias exists.
     *
     * @param threadPool   the {@link ThreadPool} for the client
     * @param clusterAlias the cluster alias the remote cluster is registered under
     * @throws IllegalArgumentException if the given clusterAlias doesn't exist
     */
    public Client getRemoteClusterClient(ThreadPool threadPool, String clusterAlias) {
        if (transportService.getRemoteClusterService().isEnabled() == false) {
            throw new IllegalArgumentException(
                "this node does not have the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role"
            );
        }
        if (transportService.getRemoteClusterService().getRemoteClusterNames().contains(clusterAlias) == false) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        }
        return new RemoteClusterAwareClient(settings, threadPool, transportService, clusterAlias);
    }

    Collection<RemoteClusterConnection> getConnections() {
        return remoteClusters.values();
    }

    /**
     * Internal class to hold cluster alias and key and track a remote connection
     *
     * @opensearch.internal
     */
    private static class RemoteConnectionEnabled<T> implements Setting.Validator<T> {

        private final String clusterAlias;
        private final String key;

        private RemoteConnectionEnabled(String clusterAlias, String key) {
            this.clusterAlias = clusterAlias;
            this.key = key;
        }

        @Override
        public void validate(T value) {}

        @Override
        public void validate(T value, Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent && RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings) == false) {
                throw new IllegalArgumentException("Cannot configure setting [" + key + "] if remote cluster is not enabled.");
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return Stream.concat(
                Stream.of(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias)),
                settingsStream()
            ).iterator();
        }

        private Stream<Setting<?>> settingsStream() {
            return Arrays.stream(RemoteConnectionStrategy.ConnectionStrategy.values())
                .flatMap(strategy -> strategy.getEnablementSettings().get())
                .map(as -> as.getConcreteSettingForNamespace(clusterAlias));
        }
    };
}
