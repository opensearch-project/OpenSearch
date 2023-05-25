/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateResponse;
import org.opensearch.cluster.ProtobufClusterName;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.Booleans;
import org.opensearch.common.SetOnce;
import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.ProtobufTransportAddress;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.threadpool.ProtobufThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.common.settings.Setting.intSetting;

/**
 * Sniff for initial seed nodes
*
* @opensearch.internal
*/
public class ProtobufSniffConnectionStrategy extends ProtobufRemoteConnectionStrategy {

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
    */
    public static final Setting.AffixSetting<List<String>> REMOTE_CLUSTER_SEEDS = Setting.affixKeySetting(
        "cluster.remote.",
        "seeds",
        (ns, key) -> Setting.listSetting(key, Collections.emptyList(), s -> {
            // validate seed address
            parsePort(s);
            return s;
        }, new StrategyValidator<>(ns, key, ProtobufConnectionStrategy.SNIFF), Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    /**
     * A proxy address for the remote cluster. By default this is not set, meaning that OpenSearch will connect directly to the nodes in
    * the remote cluster using their publish addresses. If this setting is set to an IP address or hostname then OpenSearch will connect
    * to the nodes in the remote cluster using this address instead. Use of this setting is not recommended and it is deliberately
    * undocumented as it does not work well with all proxies.
    */
    public static final Setting.AffixSetting<String> REMOTE_CLUSTERS_PROXY = Setting.affixKeySetting(
        "cluster.remote.",
        "proxy",
        (ns, key) -> Setting.simpleString(key, new StrategyValidator<>(ns, key, ProtobufConnectionStrategy.SNIFF, s -> {
            if (Strings.hasLength(s)) {
                parsePort(s);
            }
        }), Setting.Property.Dynamic, Setting.Property.NodeScope),
        () -> REMOTE_CLUSTER_SEEDS
    );

    /**
     * The maximum number of connections that will be established to a remote cluster. For instance if there is only a single
    * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
    */
    public static final Setting<Integer> REMOTE_CONNECTIONS_PER_CLUSTER = intSetting(
        "cluster.remote.connections_per_cluster",
        3,
        1,
        Setting.Property.NodeScope
    );
    /**
     * The maximum number of node connections that will be established to a remote cluster. For instance if there is only a single
    * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
    */
    public static final Setting.AffixSetting<Integer> REMOTE_NODE_CONNECTIONS = Setting.affixKeySetting(
        "cluster.remote.",
        "node_connections",
        (ns, key) -> intSetting(
            key,
            REMOTE_CONNECTIONS_PER_CLUSTER,
            1,
            new StrategyValidator<>(ns, key, ProtobufConnectionStrategy.SNIFF),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    static final int CHANNELS_PER_CONNECTION = 6;

    private static final Predicate<ProtobufDiscoveryNode> DEFAULT_NODE_PREDICATE = (node) -> Version.CURRENT.isCompatible(node.getVersion())
        && (node.isClusterManagerNode() == false || node.isDataNode() || node.isIngestNode());

    private final List<String> configuredSeedNodes;
    private final List<Supplier<ProtobufDiscoveryNode>> seedNodes;
    private final int maxNumRemoteConnections;
    private final Predicate<ProtobufDiscoveryNode> nodePredicate;
    private final SetOnce<ProtobufClusterName> remoteClusterName = new SetOnce<>();
    private final String proxyAddress;

    ProtobufSniffConnectionStrategy(
        String clusterAlias,
        ProtobufTransportService transportService,
        ProtobufRemoteConnectionManager connectionManager,
        Settings settings
    ) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterAlias).get(settings),
            settings,
            REMOTE_NODE_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(settings),
            getNodePredicate(settings),
            REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(settings)
        );
    }

    ProtobufSniffConnectionStrategy(
        String clusterAlias,
        ProtobufTransportService transportService,
        ProtobufRemoteConnectionManager connectionManager,
        String proxyAddress,
        Settings settings,
        int maxNumRemoteConnections,
        Predicate<ProtobufDiscoveryNode> nodePredicate,
        List<String> configuredSeedNodes
    ) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            proxyAddress,
            settings,
            maxNumRemoteConnections,
            nodePredicate,
            configuredSeedNodes,
            configuredSeedNodes.stream()
                .map(seedAddress -> (Supplier<ProtobufDiscoveryNode>) () -> resolveSeedNode(clusterAlias, seedAddress, proxyAddress))
                .collect(Collectors.toList())
        );
    }

    ProtobufSniffConnectionStrategy(
        String clusterAlias,
        ProtobufTransportService transportService,
        ProtobufRemoteConnectionManager connectionManager,
        String proxyAddress,
        Settings settings,
        int maxNumRemoteConnections,
        Predicate<ProtobufDiscoveryNode> nodePredicate,
        List<String> configuredSeedNodes,
        List<Supplier<ProtobufDiscoveryNode>> seedNodes
    ) {
        super(clusterAlias, transportService, connectionManager, settings);
        this.proxyAddress = proxyAddress;
        this.maxNumRemoteConnections = maxNumRemoteConnections;
        this.nodePredicate = nodePredicate;
        this.configuredSeedNodes = configuredSeedNodes;
        this.seedNodes = seedNodes;
    }

    static Stream<Setting.AffixSetting<?>> enablementSettings() {
        return Stream.of(ProtobufSniffConnectionStrategy.REMOTE_CLUSTER_SEEDS);
    }

    static ProtobufWriteable.Reader<ProtobufRemoteConnectionInfo.ModeInfo> infoReader() {
        return SniffModeInfo::new;
    }

    @Override
    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumRemoteConnections;
    }

    @Override
    protected boolean strategyMustBeRebuilt(Settings newSettings) {
        String proxy = REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        List<String> addresses = REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        int nodeConnections = REMOTE_NODE_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        return nodeConnections != maxNumRemoteConnections
            || seedsChanged(configuredSeedNodes, addresses)
            || proxyChanged(proxyAddress, proxy);
    }

    @Override
    protected ProtobufConnectionStrategy strategyType() {
        return ProtobufConnectionStrategy.SNIFF;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        collectRemoteNodes(seedNodes.iterator(), listener);
    }

    @Override
    protected ProtobufRemoteConnectionInfo.ModeInfo getModeInfo() {
        return new SniffModeInfo(configuredSeedNodes, maxNumRemoteConnections, connectionManager.size());
    }

    private void collectRemoteNodes(Iterator<Supplier<ProtobufDiscoveryNode>> seedNodes, ActionListener<Void> listener) {
        if (Thread.currentThread().isInterrupted()) {
            listener.onFailure(new InterruptedException("remote connect thread got interrupted"));
            return;
        }

        if (seedNodes.hasNext()) {
            final Consumer<Exception> onFailure = e -> {
                if (e instanceof ConnectTransportException || e instanceof IOException || e instanceof IllegalStateException) {
                    // ISE if we fail the handshake with an version incompatible node
                    if (seedNodes.hasNext()) {
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "fetching nodes from external cluster [{}] failed moving to next seed node",
                                clusterAlias
                            ),
                            e
                        );
                        collectRemoteNodes(seedNodes, listener);
                        return;
                    }
                }
                logger.warn(new ParameterizedMessage("fetching nodes from external cluster [{}] failed", clusterAlias), e);
                listener.onFailure(e);
            };

            final ProtobufDiscoveryNode seedNode = seedNodes.next().get();
            logger.trace("[{}] opening transient connection to seed node: [{}]", clusterAlias, seedNode);
            final StepListener<ProtobufTransport.Connection> openConnectionStep = new StepListener<>();
            try {
                connectionManager.openConnection(seedNode, null, openConnectionStep);
            } catch (Exception e) {
                onFailure.accept(e);
            }

            final StepListener<ProtobufTransportService.HandshakeResponse> handshakeStep = new StepListener<>();
            openConnectionStep.whenComplete(connection -> {
                ProtobufConnectionProfile connectionProfile = connectionManager.getConnectionProfile();
                transportService.handshake(
                    connection,
                    connectionProfile.getHandshakeTimeout().millis(),
                    getRemoteClusterNamePredicate(),
                    handshakeStep
                );
            }, onFailure);

            final StepListener<Void> fullConnectionStep = new StepListener<>();
            handshakeStep.whenComplete(handshakeResponse -> {
                final ProtobufDiscoveryNode handshakeNode = handshakeResponse.getDiscoveryNode();

                if (nodePredicate.test(handshakeNode) && shouldOpenMoreConnections()) {
                    logger.trace(
                        "[{}] opening managed connection to seed node: [{}] proxy address: [{}]",
                        clusterAlias,
                        handshakeNode,
                        proxyAddress
                    );
                    final ProtobufDiscoveryNode handshakeNodeWithProxy = maybeAddProxyAddress(proxyAddress, handshakeNode);
                    connectionManager.connectToNode(
                        handshakeNodeWithProxy,
                        null,
                        transportService.connectionValidator(handshakeNodeWithProxy),
                        fullConnectionStep
                    );
                } else {
                    fullConnectionStep.onResponse(null);
                }
            }, e -> {
                final ProtobufTransport.Connection connection = openConnectionStep.result();
                final ProtobufDiscoveryNode node = connection.getNode();
                logger.debug(() -> new ParameterizedMessage("[{}] failed to handshake with seed node: [{}]", clusterAlias, node), e);
                IOUtils.closeWhileHandlingException(connection);
                onFailure.accept(e);
            });

            fullConnectionStep.whenComplete(aVoid -> {
                if (remoteClusterName.get() == null) {
                    ProtobufTransportService.HandshakeResponse handshakeResponse = handshakeStep.result();
                    assert handshakeResponse.getClusterName().value() != null;
                    remoteClusterName.set(handshakeResponse.getClusterName());
                }
                final ProtobufTransport.Connection connection = openConnectionStep.result();

                ProtobufClusterStateRequest request = new ProtobufClusterStateRequest();
                request.clear();
                request.nodes(true);
                // here we pass on the connection since we can only close it once the sendRequest returns otherwise
                // due to the async nature (it will return before it's actually sent) this can cause the request to fail
                // due to an already closed connection.
                ProtobufThreadPool threadPool = transportService.getThreadPool();
                ThreadContext threadContext = threadPool.getThreadContext();
                ProtobufTransportService.ContextRestoreResponseHandler<ProtobufClusterStateResponse> responseHandler =
                    new ProtobufTransportService.ContextRestoreResponseHandler<>(
                        threadContext.newRestorableContext(false),
                        new SniffClusterStateResponseHandler(connection, listener, seedNodes)
                    );
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    // we stash any context here since this is an internal execution and should not leak any
                    // existing context information.
                    threadContext.markAsSystemContext();
                    transportService.sendRequest(
                        connection,
                        ClusterStateAction.NAME,
                        request,
                        TransportRequestOptions.EMPTY,
                        responseHandler
                    );
                }
            }, e -> {
                final ProtobufTransport.Connection connection = openConnectionStep.result();
                final ProtobufDiscoveryNode node = connection.getNode();
                logger.debug(
                    () -> new ParameterizedMessage("[{}] failed to open managed connection to seed node: [{}]", clusterAlias, node),
                    e
                );
                IOUtils.closeWhileHandlingException(openConnectionStep.result());
                onFailure.accept(e);
            });
        } else {
            listener.onFailure(new NoSeedNodeLeftException(clusterAlias));
        }
    }

    /* This class handles the _state response from the remote cluster when sniffing nodes to connect to */
    private class SniffClusterStateResponseHandler implements ProtobufTransportResponseHandler<ProtobufClusterStateResponse> {

        private final ProtobufTransport.Connection connection;
        private final ActionListener<Void> listener;
        private final Iterator<Supplier<ProtobufDiscoveryNode>> seedNodes;

        SniffClusterStateResponseHandler(
            ProtobufTransport.Connection connection,
            ActionListener<Void> listener,
            Iterator<Supplier<ProtobufDiscoveryNode>> seedNodes
        ) {
            this.connection = connection;
            this.listener = listener;
            this.seedNodes = seedNodes;
        }

        @Override
        public ProtobufClusterStateResponse read(CodedInputStream in) throws IOException {
            return new ProtobufClusterStateResponse(in);
        }

        @Override
        public void handleResponse(ProtobufClusterStateResponse response) {
            handleNodes(response.getState().nodes().getNodes().valuesIt());
        }

        private void handleNodes(Iterator<ProtobufDiscoveryNode> nodesIter) {
            while (nodesIter.hasNext()) {
                final ProtobufDiscoveryNode node = nodesIter.next();
                if (nodePredicate.test(node) && shouldOpenMoreConnections()) {
                    logger.trace("[{}] opening managed connection to node: [{}] proxy address: [{}]", clusterAlias, node, proxyAddress);
                    final ProtobufDiscoveryNode nodeWithProxy = maybeAddProxyAddress(proxyAddress, node);
                    connectionManager.connectToNode(
                        nodeWithProxy,
                        null,
                        transportService.connectionValidator(node),
                        new ActionListener<Void>() {
                            @Override
                            public void onResponse(Void aVoid) {
                                handleNodes(nodesIter);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (e instanceof ConnectTransportException || e instanceof IllegalStateException) {
                                    // ISE if we fail the handshake with an version incompatible node
                                    // fair enough we can't connect just move on
                                    logger.debug(
                                        () -> new ParameterizedMessage(
                                            "[{}] failed to open managed connection to node [{}]",
                                            clusterAlias,
                                            node
                                        ),
                                        e
                                    );
                                    handleNodes(nodesIter);
                                } else {
                                    logger.warn(
                                        new ParameterizedMessage("[{}] failed to open managed connection to node [{}]", clusterAlias, node),
                                        e
                                    );
                                    IOUtils.closeWhileHandlingException(connection);
                                    collectRemoteNodes(seedNodes, listener);
                                }
                            }
                        }
                    );
                    return;
                }
            }
            // We have to close this connection before we notify listeners - this is mainly needed for test correctness
            // since if we do it afterwards we might fail assertions that check if all high level connections are closed.
            // from a code correctness perspective we could also close it afterwards.
            IOUtils.closeWhileHandlingException(connection);
            int openConnections = connectionManager.size();
            if (openConnections == 0) {
                listener.onFailure(new IllegalStateException("Unable to open any connections to remote cluster [" + clusterAlias + "]"));
            } else {
                listener.onResponse(null);
            }
        }

        @Override
        public void handleException(ProtobufTransportException exp) {
            logger.warn(new ParameterizedMessage("fetching nodes from external cluster {} failed", clusterAlias), exp);
            try {
                IOUtils.closeWhileHandlingException(connection);
            } finally {
                // once the connection is closed lets try the next node
                collectRemoteNodes(seedNodes, listener);
            }
        }

        @Override
        public String executor() {
            return ProtobufThreadPool.Names.MANAGEMENT;
        }
    }

    private Predicate<ProtobufClusterName> getRemoteClusterNamePredicate() {
        return new Predicate<ProtobufClusterName>() {
            @Override
            public boolean test(ProtobufClusterName c) {
                return remoteClusterName.get() == null || c.equals(remoteClusterName.get());
            }

            @Override
            public String toString() {
                return remoteClusterName.get() == null
                    ? "any cluster name"
                    : "expected remote cluster name [" + remoteClusterName.get().value() + "]";
            }
        };
    }

    private static ProtobufDiscoveryNode resolveSeedNode(String clusterAlias, String address, String proxyAddress) {
        if (proxyAddress == null || proxyAddress.isEmpty()) {
            ProtobufTransportAddress transportAddress = new ProtobufTransportAddress(parseConfiguredAddress(address));
            return new ProtobufDiscoveryNode(
                clusterAlias + "#" + transportAddress.toString(),
                transportAddress,
                Version.CURRENT.minimumCompatibilityVersion()
            );
        } else {
            ProtobufTransportAddress transportAddress = new ProtobufTransportAddress(parseConfiguredAddress(proxyAddress));
            String hostName = ProtobufRemoteConnectionStrategy.parseHost(proxyAddress);
            return new ProtobufDiscoveryNode(
                "",
                clusterAlias + "#" + address,
                UUIDs.randomBase64UUID(),
                hostName,
                address,
                transportAddress,
                Collections.singletonMap("server_name", hostName),
                DiscoveryNodeRole.BUILT_IN_ROLES,
                Version.CURRENT.minimumCompatibilityVersion()
            );
        }
    }

    // Default visibility for tests
    static Predicate<ProtobufDiscoveryNode> getNodePredicate(Settings settings) {
        if (RemoteClusterService.REMOTE_NODE_ATTRIBUTE.exists(settings)) {
            // nodes can be tagged with node.attr.remote_gateway: true to allow a node to be a gateway node for cross cluster search
            String attribute = RemoteClusterService.REMOTE_NODE_ATTRIBUTE.get(settings);
            return DEFAULT_NODE_PREDICATE.and((node) -> Booleans.parseBoolean(node.getAttributes().getOrDefault(attribute, "false")));
        }
        return DEFAULT_NODE_PREDICATE;
    }

    private static ProtobufDiscoveryNode maybeAddProxyAddress(String proxyAddress, ProtobufDiscoveryNode node) {
        if (proxyAddress == null || proxyAddress.isEmpty()) {
            return node;
        } else {
            // resolve proxy address lazy here
            InetSocketAddress proxyInetAddress = parseConfiguredAddress(proxyAddress);
            return new ProtobufDiscoveryNode(
                node.getName(),
                node.getId(),
                node.getEphemeralId(),
                node.getHostName(),
                node.getHostAddress(),
                new ProtobufTransportAddress(proxyInetAddress),
                node.getAttributes(),
                node.getRoles(),
                node.getVersion()
            );
        }
    }

    private boolean seedsChanged(final List<String> oldSeedNodes, final List<String> newSeedNodes) {
        if (oldSeedNodes.size() != newSeedNodes.size()) {
            return true;
        }
        Set<String> oldSeeds = new HashSet<>(oldSeedNodes);
        Set<String> newSeeds = new HashSet<>(newSeedNodes);
        return oldSeeds.equals(newSeeds) == false;
    }

    private boolean proxyChanged(String oldProxy, String newProxy) {
        if (oldProxy == null || oldProxy.isEmpty()) {
            return (newProxy == null || newProxy.isEmpty()) == false;
        }

        return Objects.equals(oldProxy, newProxy) == false;
    }

    /**
     * Information about the sniff mode
    *
    * @opensearch.internal
    */
    public static class SniffModeInfo implements ProtobufRemoteConnectionInfo.ModeInfo {

        final List<String> seedNodes;
        final int maxConnectionsPerCluster;
        final int numNodesConnected;

        public SniffModeInfo(List<String> seedNodes, int maxConnectionsPerCluster, int numNodesConnected) {
            this.seedNodes = seedNodes;
            this.maxConnectionsPerCluster = maxConnectionsPerCluster;
            this.numNodesConnected = numNodesConnected;
        }

        private SniffModeInfo(CodedInputStream input) throws IOException {
            ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(input);
            seedNodes = Arrays.asList(protobufStreamInput.readStringArray());
            maxConnectionsPerCluster = protobufStreamInput.readVInt();
            numNodesConnected = protobufStreamInput.readVInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray("seeds");
            for (String address : seedNodes) {
                builder.value(address);
            }
            builder.endArray();
            builder.field("num_nodes_connected", numNodesConnected);
            builder.field("max_connections_per_cluster", maxConnectionsPerCluster);
            return builder;
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            protobufStreamOutput.writeStringArray(seedNodes.toArray(new String[0]));
            out.writeInt32NoTag(maxConnectionsPerCluster);
            out.writeInt32NoTag(numNodesConnected);
        }

        @Override
        public boolean isConnected() {
            return numNodesConnected > 0;
        }

        @Override
        public String modeName() {
            return "sniff";
        }

        public List<String> getSeedNodes() {
            return seedNodes;
        }

        public int getMaxConnectionsPerCluster() {
            return maxConnectionsPerCluster;
        }

        public int getNumNodesConnected() {
            return numNodesConnected;
        }

        @Override
        public ProtobufRemoteConnectionStrategy.ProtobufConnectionStrategy modeType() {
            return ProtobufRemoteConnectionStrategy.ProtobufConnectionStrategy.SNIFF;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SniffModeInfo sniff = (SniffModeInfo) o;
            return maxConnectionsPerCluster == sniff.maxConnectionsPerCluster
                && numNodesConnected == sniff.numNodesConnected
                && Objects.equals(seedNodes, sniff.seedNodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(seedNodes, maxConnectionsPerCluster, numNodesConnected);
        }
    }
}
