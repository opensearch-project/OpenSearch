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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ProtobufActionListenerResponseHandler;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ProtobufClusterName;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.ProtobufBoundTransportAddress;
import org.opensearch.common.transport.ProtobufTransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.node.ProtobufNodeClosedException;
import org.opensearch.node.ProtobufReportingService;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.ProtobufTaskManager;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ProtobufThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * The main OpenSearch transport service
*
* @opensearch.internal
*/
public class ProtobufTransportService extends AbstractLifecycleComponent
    implements
        ProtobufReportingService<ProtobufTransportInfo>,
        ProtobufTransportMessageListener,
        ProtobufTransportConnectionListener {
    private static final Logger logger = LogManager.getLogger(ProtobufTransportService.class);

    public static final String DIRECT_RESPONSE_PROFILE = ".direct";
    public static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    private final AtomicBoolean handleIncomingRequests = new AtomicBoolean();
    private final DelegatingTransportMessageListener messageListener = new DelegatingTransportMessageListener();
    protected final ProtobufTransport transport;
    protected final ProtobufConnectionManager connectionManager;
    protected final ProtobufThreadPool threadPool;
    protected final ProtobufClusterName clusterName;
    protected final ProtobufTaskManager taskManager;
    private final ProtobufTransportInterceptor.AsyncSender asyncSender;
    private final Function<ProtobufBoundTransportAddress, ProtobufDiscoveryNode> localNodeFactory;
    private final boolean remoteClusterClient;
    private final ProtobufTransport.ResponseHandlers responseHandlers;
    private final ProtobufTransportInterceptor interceptor;

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers = Collections.synchronizedMap(
        new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > 100;
            }
        }
    );

    public static final ProtobufTransportInterceptor NOOP_TRANSPORT_INTERCEPTOR = new ProtobufTransportInterceptor() {
    };

    // tracer log

    private final Logger tracerLog;

    volatile String[] tracerLogInclude;
    volatile String[] tracerLogExclude;

    private final ProtobufRemoteClusterService remoteClusterService;

    /** if set will call requests sent to this id to shortcut and executed locally */
    volatile ProtobufDiscoveryNode localNode = null;
    private final ProtobufTransport.Connection localNodeConnection = new ProtobufTransport.Connection() {
        @Override
        public ProtobufDiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public void sendRequest(long requestId, String action, ProtobufTransportRequest request, TransportRequestOptions options)
            throws ProtobufTransportException {
            sendLocalRequest(requestId, action, request, options);
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {}
    };

    /**
     * Build the service.
    *
    * @param clusterSettings if non null, the {@linkplain ProtobufTransportService} will register with the {@link ClusterSettings} for settings
    *   *    updates for {@link TransportSettings#TRACE_LOG_EXCLUDE_SETTING} and {@link TransportSettings#TRACE_LOG_INCLUDE_SETTING}.
    */
    public ProtobufTransportService(
        Settings settings,
        ProtobufTransport transport,
        ProtobufThreadPool threadPool,
        ProtobufTransportInterceptor transportInterceptor,
        Function<ProtobufBoundTransportAddress, ProtobufDiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders
    ) {
        this(
            settings,
            transport,
            threadPool,
            transportInterceptor,
            localNodeFactory,
            clusterSettings,
            taskHeaders,
            new ProtobufClusterConnectionManager(settings, transport)
        );
    }

    public ProtobufTransportService(
        Settings settings,
        ProtobufTransport transport,
        ProtobufThreadPool threadPool,
        ProtobufTransportInterceptor transportInterceptor,
        Function<ProtobufBoundTransportAddress, ProtobufDiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders,
        ProtobufConnectionManager connectionManager
    ) {
        this.transport = transport;
        transport.setSlowLogThreshold(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING.get(settings));
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.connectionManager = connectionManager;
        this.clusterName = ProtobufClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TransportSettings.TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TransportSettings.TRACE_LOG_EXCLUDE_SETTING.get(settings));
        tracerLog = Loggers.getLogger(logger, ".tracer");
        taskManager = createTaskManager(settings, clusterSettings, threadPool, taskHeaders);
        this.interceptor = transportInterceptor;
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        this.remoteClusterClient = ProtobufDiscoveryNode.isRemoteClusterClient(settings);
        remoteClusterService = new ProtobufRemoteClusterService(settings, this);
        responseHandlers = transport.getResponseHandlers();
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.TRACE_LOG_INCLUDE_SETTING, this::setTracerLogInclude);
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.TRACE_LOG_EXCLUDE_SETTING, this::setTracerLogExclude);
            if (remoteClusterClient) {
                remoteClusterService.listenForUpdates(clusterSettings);
            }
            clusterSettings.addSettingsUpdateConsumer(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING, transport::setSlowLogThreshold);
        }
        registerRequestHandler(
            HANDSHAKE_ACTION_NAME,
            ProtobufThreadPool.Names.SAME,
            false,
            false,
            HandshakeRequest::new,
            (request, channel, task) -> channel.sendResponse(new HandshakeResponse(localNode, clusterName, localNode.getVersion()))
        );
    }

    public ProtobufRemoteClusterService getRemoteClusterService() {
        return remoteClusterService;
    }

    public ProtobufDiscoveryNode getLocalNode() {
        return localNode;
    }

    public ProtobufTaskManager getTaskManager() {
        return taskManager;
    }

    protected ProtobufTaskManager createTaskManager(
        Settings settings,
        ClusterSettings clusterSettings,
        ProtobufThreadPool threadPool,
        Set<String> taskHeaders
    ) {
        if (clusterSettings != null) {
            return ProtobufTaskManager.createTaskManagerWithClusterSettings(settings, clusterSettings, threadPool, taskHeaders);
        } else {
            return new ProtobufTaskManager(settings, threadPool, taskHeaders);
        }
    }

    /**
     * The executor service for this transport service.
    *
    * @return the executor service
    */
    private ExecutorService getExecutorService() {
        return threadPool.generic();
    }

    void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }

    @Override
    protected void doStart() {
        transport.setMessageListener(this);
        connectionManager.addListener(this);
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
            for (Map.Entry<String, ProtobufBoundTransportAddress> entry : transport.profileBoundAddresses().entrySet()) {
                logger.info("profile [{}]: {}", entry.getKey(), entry.getValue());
            }
        }
        localNode = localNodeFactory.apply(transport.boundAddress());

        if (remoteClusterClient) {
            // here we start to connect to the remote clusters
            remoteClusterService.initializeRemoteClusters();
        }
    }

    @Override
    protected void doStop() {
        try {
            IOUtils.close(connectionManager, remoteClusterService, transport::stop);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            for (final ProtobufTransport.ResponseContext holderToNotify : responseHandlers.prune(h -> true)) {
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                getExecutorService().execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                holderToNotify.action()
                            ),
                            e
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                holderToNotify.action()
                            ),
                            e
                        );
                    }

                    @Override
                    public void doRun() {
                        ProtobufTransportException ex = new ProtobufSendRequestTransportException(
                            holderToNotify.connection().getNode(),
                            holderToNotify.action(),
                            new ProtobufNodeClosedException(localNode)
                        );
                        holderToNotify.handler().handleException(ex);
                    }
                });
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        transport.close();
    }

    /**
     * start accepting incoming requests.
    * when the transport layer starts up it will block any incoming requests until
    * this method is called
    */
    public final void acceptIncomingRequests() {
        handleIncomingRequests.set(true);
    }

    @Override
    public ProtobufTransportInfo info() {
        ProtobufBoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new ProtobufTransportInfo(boundTransportAddress, transport.profileBoundAddresses());
    }

    public TransportStats stats() {
        return transport.getStats();
    }

    public boolean isTransportSecure() {
        return transport.isSecure();
    }

    public ProtobufBoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public List<String> getDefaultSeedAddresses() {
        return transport.getDefaultSeedAddresses();
    }

    /**
     * Returns <code>true</code> iff the given node is already connected.
    */
    public boolean nodeConnected(ProtobufDiscoveryNode node) {
        return isLocalNode(node) || connectionManager.nodeConnected(node);
    }

    /**
     * Connect to the specified node with the default connection profile
    *
    * @param node the node to connect to
    */
    public void connectToNode(ProtobufDiscoveryNode node) throws ProtobufConnectTransportException {
        connectToNode(node, (ProtobufConnectionProfile) null);
    }

    // We are skipping node validation for extensibility as extensionNode and opensearchNode(LocalNode) will have different ephemeral id's
    public void connectToExtensionNode(final ProtobufDiscoveryNode node) {
        PlainActionFuture.get(fut -> connectToExtensionNode(node, (ProtobufConnectionProfile) null, ActionListener.map(fut, x -> null)));
    }

    /**
     * Connect to the specified node with the given connection profile
    *
    * @param node the node to connect to
    * @param connectionProfile the connection profile to use when connecting to this node
    */
    public void connectToNode(final ProtobufDiscoveryNode node, ProtobufConnectionProfile connectionProfile) {
        PlainActionFuture.get(fut -> connectToNode(node, connectionProfile, ActionListener.map(fut, x -> null)));
    }

    public void connectToExtensionNode(final ProtobufDiscoveryNode node, ProtobufConnectionProfile connectionProfile) {
        PlainActionFuture.get(fut -> connectToExtensionNode(node, connectionProfile, ActionListener.map(fut, x -> null)));
    }

    /**
     * Connect to the specified node with the given connection profile.
    * The ActionListener will be called on the calling thread or the generic thread pool.
    *
    * @param node the node to connect to
    * @param listener the action listener to notify
    */
    public void connectToNode(ProtobufDiscoveryNode node, ActionListener<Void> listener) throws ProtobufConnectTransportException {
        connectToNode(node, null, listener);
    }

    public void connectToExtensionNode(ProtobufDiscoveryNode node, ActionListener<Void> listener) throws ProtobufConnectTransportException {
        connectToExtensionNode(node, null, listener);
    }

    /**
     * Connect to the specified node with the given connection profile.
    * The ActionListener will be called on the calling thread or the generic thread pool.
    *
    * @param node the node to connect to
    * @param connectionProfile the connection profile to use when connecting to this node
    * @param listener the action listener to notify
    */
    public void connectToNode(
        final ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ActionListener<Void> listener
    ) {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        connectionManager.connectToNode(node, connectionProfile, connectionValidator(node), listener);
    }

    public void connectToExtensionNode(
        final ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ActionListener<Void> listener
    ) {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        connectionManager.connectToNode(node, connectionProfile, extensionConnectionValidator(node), listener);
    }

    public ProtobufConnectionManager.ConnectionValidator connectionValidator(ProtobufDiscoveryNode node) {
        return (newConnection, actualProfile, listener) -> {
            // We don't validate cluster names to allow for CCS connections.
            handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true, ActionListener.map(listener, resp -> {
                final ProtobufDiscoveryNode remote = resp.discoveryNode;

                if (node.equals(remote) == false) {
                    throw new ProtobufConnectTransportException(node, "handshake failed. unexpected remote node " + remote);
                }

                return null;
            }));
        };
    }

    public ProtobufConnectionManager.ConnectionValidator extensionConnectionValidator(ProtobufDiscoveryNode node) {
        return (newConnection, actualProfile, listener) -> {
            // We don't validate cluster names to allow for CCS connections.
            handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true, ActionListener.map(listener, resp -> {
                final ProtobufDiscoveryNode remote = resp.discoveryNode;
                logger.info("Connection validation was skipped");
                return null;
            }));
        };
    }

    /**
     * Establishes and returns a new connection to the given node. The connection is NOT maintained by this service, it's the callers
    * responsibility to close the connection once it goes out of scope.
    * The ActionListener will be called on the calling thread or the generic thread pool.
    * @param node the node to connect to
    * @param connectionProfile the connection profile to use
    */
    public ProtobufTransport.Connection openConnection(final ProtobufDiscoveryNode node, ProtobufConnectionProfile connectionProfile) {
        return PlainActionFuture.get(fut -> openConnection(node, connectionProfile, fut));
    }

    /**
     * Establishes a new connection to the given node. The connection is NOT maintained by this service, it's the callers
    * responsibility to close the connection once it goes out of scope.
    * The ActionListener will be called on the calling thread or the generic thread pool.
    * @param node the node to connect to
    * @param connectionProfile the connection profile to use
    * @param listener the action listener to notify
    */
    public void openConnection(
        final ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ActionListener<ProtobufTransport.Connection> listener
    ) {
        if (isLocalNode(node)) {
            listener.onResponse(localNodeConnection);
        } else {
            connectionManager.openConnection(node, connectionProfile, listener);
        }
    }

    /**
     * Executes a high-level handshake using the given connection
    * and returns the discovery node of the node the connection
    * was established with. The handshake will fail if the cluster
    * name on the target node mismatches the local cluster name.
    * The ActionListener will be called on the calling thread or the generic thread pool.
    *
    * @param connection       the connection to a specific node
    * @param handshakeTimeout handshake timeout
    * @param listener         action listener to notify
    * @throws ProtobufConnectTransportException if the connection failed
    * @throws IllegalStateException if the handshake failed
    */
    public void handshake(
        final ProtobufTransport.Connection connection,
        final long handshakeTimeout,
        final ActionListener<ProtobufDiscoveryNode> listener
    ) {
        handshake(
            connection,
            handshakeTimeout,
            clusterName.getEqualityPredicate(),
            ActionListener.map(listener, HandshakeResponse::getDiscoveryNode)
        );
    }

    /**
     * Executes a high-level handshake using the given connection
    * and returns the discovery node of the node the connection
    * was established with. The handshake will fail if the cluster
    * name on the target node doesn't match the local cluster name.
    * The ActionListener will be called on the calling thread or the generic thread pool.
    *
    * @param connection       the connection to a specific node
    * @param handshakeTimeout handshake timeout
    * @param clusterNamePredicate cluster name validation predicate
    * @param listener         action listener to notify
    * @throws IllegalStateException if the handshake failed
    */
    public void handshake(
        final ProtobufTransport.Connection connection,
        final long handshakeTimeout,
        Predicate<ProtobufClusterName> clusterNamePredicate,
        final ActionListener<HandshakeResponse> listener
    ) {
        final ProtobufDiscoveryNode node = connection.getNode();
        sendRequest(
            connection,
            HANDSHAKE_ACTION_NAME,
            HandshakeRequest.INSTANCE,
            TransportRequestOptions.builder().withTimeout(handshakeTimeout).build(),
            new ProtobufActionListenerResponseHandler<>(new ActionListener<HandshakeResponse>() {
                @Override
                public void onResponse(HandshakeResponse response) {
                    if (clusterNamePredicate.test(response.clusterName) == false) {
                        listener.onFailure(
                            new IllegalStateException(
                                "handshake with ["
                                    + node
                                    + "] failed: remote cluster name ["
                                    + response.clusterName.value()
                                    + "] does not match "
                                    + clusterNamePredicate
                            )
                        );
                    } else if (response.version.isCompatible(localNode.getVersion()) == false) {
                        listener.onFailure(
                            new IllegalStateException(
                                "handshake with ["
                                    + node
                                    + "] failed: remote node version ["
                                    + response.version
                                    + "] is incompatible with local node version ["
                                    + localNode.getVersion()
                                    + "]"
                            )
                        );
                    } else {
                        listener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }, HandshakeResponse::new, ProtobufThreadPool.Names.GENERIC)
        );
    }

    public ProtobufConnectionManager getConnectionManager() {
        return connectionManager;
    }

    /**
     * Internal Handshake request
    *
    * @opensearch.internal
    */
    static class HandshakeRequest extends ProtobufTransportRequest {

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        HandshakeRequest(CodedInputStream in) throws IOException {
            super(in);
        }

        private HandshakeRequest() {}

    }

    /**
     * Internal handshake response
    *
    * @opensearch.internal
    */
    public static class HandshakeResponse extends ProtobufTransportResponse {
        private final ProtobufDiscoveryNode discoveryNode;
        private final ProtobufClusterName clusterName;
        private final Version version;

        public HandshakeResponse(ProtobufDiscoveryNode discoveryNode, ProtobufClusterName clusterName, Version version) {
            this.discoveryNode = discoveryNode;
            this.version = version;
            this.clusterName = clusterName;
        }

        public HandshakeResponse(CodedInputStream in) throws IOException {
            super(in);
            ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
            discoveryNode = protobufStreamInput.readOptionalWriteable(ProtobufDiscoveryNode::new);
            clusterName = new ProtobufClusterName(in);
            version = Version.readVersionProtobuf(in);
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            protobufStreamOutput.writeOptionalWriteable(discoveryNode);
            clusterName.writeTo(out);
            out.writeInt32NoTag(version.id);
        }

        public ProtobufDiscoveryNode getDiscoveryNode() {
            return discoveryNode;
        }

        public ProtobufClusterName getClusterName() {
            return clusterName;
        }
    }

    public void disconnectFromNode(ProtobufDiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        connectionManager.disconnectFromNode(node);
    }

    public void addMessageListener(ProtobufTransportMessageListener listener) {
        messageListener.listeners.add(listener);
    }

    public boolean removeMessageListener(ProtobufTransportMessageListener listener) {
        return messageListener.listeners.remove(listener);
    }

    public void addConnectionListener(ProtobufTransportConnectionListener listener) {
        connectionManager.addListener(listener);
    }

    public void removeConnectionListener(ProtobufTransportConnectionListener listener) {
        connectionManager.removeListener(listener);
    }

    public <T extends ProtobufTransportResponse> TransportFuture<T> submitRequest(
        ProtobufDiscoveryNode node,
        String action,
        ProtobufTransportRequest request,
        ProtobufTransportResponseHandler<T> handler
    ) throws ProtobufTransportException {
        return submitRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends ProtobufTransportResponse> TransportFuture<T> submitRequest(
        ProtobufDiscoveryNode node,
        String action,
        ProtobufTransportRequest request,
        TransportRequestOptions options,
        ProtobufTransportResponseHandler<T> handler
    ) throws ProtobufTransportException {
        ProtobufPlainTransportFuture<T> futureHandler = new ProtobufPlainTransportFuture<>(handler);
        try {
            ProtobufTransport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, futureHandler);
        } catch (ProtobufNodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            futureHandler.handleException(ex);
        }
        return futureHandler;
    }

    public <T extends ProtobufTransportResponse> void sendRequest(
        final ProtobufDiscoveryNode node,
        final String action,
        final ProtobufTransportRequest request,
        final ProtobufTransportResponseHandler<T> handler
    ) {
        final ProtobufTransport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final ProtobufNodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendRequest(connection, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public final <T extends ProtobufTransportResponse> void sendRequest(
        final ProtobufDiscoveryNode node,
        final String action,
        final ProtobufTransportRequest request,
        final TransportRequestOptions options,
        ProtobufTransportResponseHandler<T> handler
    ) {
        final ProtobufTransport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final ProtobufNodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendRequest(connection, action, request, options, handler);
    }

    /**
     * Sends a request on the specified connection. If there is a failure sending the request, the specified handler is invoked.
    *
    * @param connection the connection to send the request on
    * @param action     the name of the action
    * @param request    the request
    * @param options    the options for this request
    * @param handler    the response handler
    * @param <T>        the type of the transport response
    */
    public final <T extends ProtobufTransportResponse> void sendRequest(
        final ProtobufTransport.Connection connection,
        final String action,
        final ProtobufTransportRequest request,
        final TransportRequestOptions options,
        final ProtobufTransportResponseHandler<T> handler
    ) {
        try {
            logger.debug("Action: " + action);
            final ProtobufTransportResponseHandler<T> delegate;
            if (request.getParentTask().isSet()) {
                // TODO: capture the connection instead so that we can cancel child tasks on the remote connections.
                final Releasable unregisterChildNode = taskManager.registerChildNode(request.getParentTask().getId(), connection.getNode());
                delegate = new ProtobufTransportResponseHandler<T>() {
                    @Override
                    public void handleResponse(T response) {
                        unregisterChildNode.close();
                        handler.handleResponse(response);
                    }

                    @Override
                    public void handleException(ProtobufTransportException exp) {
                        unregisterChildNode.close();
                        handler.handleException(exp);
                    }

                    @Override
                    public String executor() {
                        return handler.executor();
                    }

                    @Override
                    public T read(CodedInputStream in) throws IOException {
                        return handler.read(in);
                    }

                    @Override
                    public String toString() {
                        return getClass().getName() + "/[" + action + "]:" + handler.toString();
                    }
                };
            } else {
                delegate = handler;
            }
            asyncSender.sendRequest(connection, action, request, options, delegate);
        } catch (final Exception ex) {
            // the caller might not handle this so we invoke the handler
            final ProtobufTransportException te;
            if (ex instanceof ProtobufTransportException) {
                te = (ProtobufTransportException) ex;
            } else {
                te = new ProtobufTransportException("failure to send", ex);
            }
            handler.handleException(te);
        }
    }

    /**
     * Returns either a real transport connection or a local node connection if we are using the local node optimization.
    * @throws ProtobufNodeNotConnectedException if the given node is not connected
    */
    public ProtobufTransport.Connection getConnection(ProtobufDiscoveryNode node) {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return connectionManager.getConnection(node);
        }
    }

    public final <T extends ProtobufTransportResponse> void sendChildRequest(
        final ProtobufDiscoveryNode node,
        final String action,
        final ProtobufTransportRequest request,
        final ProtobufTask parentTask,
        final TransportRequestOptions options,
        final ProtobufTransportResponseHandler<T> handler
    ) {
        final ProtobufTransport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (final ProtobufNodeNotConnectedException ex) {
            // the caller might not handle this so we invoke the handler
            handler.handleException(ex);
            return;
        }
        sendChildRequest(connection, action, request, parentTask, options, handler);
    }

    public <T extends ProtobufTransportResponse> void sendChildRequest(
        final ProtobufTransport.Connection connection,
        final String action,
        final ProtobufTransportRequest request,
        final ProtobufTask parentTask,
        final ProtobufTransportResponseHandler<T> handler
    ) {
        sendChildRequest(connection, action, request, parentTask, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends ProtobufTransportResponse> void sendChildRequest(
        final ProtobufTransport.Connection connection,
        final String action,
        final ProtobufTransportRequest request,
        final ProtobufTask parentTask,
        final TransportRequestOptions options,
        final ProtobufTransportResponseHandler<T> handler
    ) {
        request.setParentTask(localNode.getId(), parentTask.getId());
        sendRequest(connection, action, request, options, handler);
    }

    private <T extends ProtobufTransportResponse> void sendRequestInternal(
        final ProtobufTransport.Connection connection,
        final String action,
        final ProtobufTransportRequest request,
        final TransportRequestOptions options,
        ProtobufTransportResponseHandler<T> handler
    ) {
        if (connection == null) {
            throw new IllegalStateException("can't send request to a null connection");
        }
        ProtobufDiscoveryNode node = connection.getNode();

        Supplier<ThreadContext.StoredContext> storedContextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        ContextRestoreResponseHandler<T> responseHandler = new ContextRestoreResponseHandler<>(storedContextSupplier, handler);
        // TODO we can probably fold this entire request ID dance into connection.sendReqeust but it will be a bigger refactoring
        final long requestId = responseHandlers.add(new ProtobufTransport.ResponseContext<>(responseHandler, connection, action));
        final TimeoutHandler timeoutHandler;
        if (options.timeout() != null) {
            timeoutHandler = new TimeoutHandler(requestId, connection.getNode(), action);
            responseHandler.setTimeoutHandler(timeoutHandler);
        } else {
            timeoutHandler = null;
        }
        try {
            if (lifecycle.stoppedOrClosed()) {
                /*
                * If we are not started the exception handling will remove the request holder again and calls the handler to notify the
                * caller. It will only notify if toStop hasn't done the work yet.
                */
                throw new ProtobufNodeClosedException(localNode);
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                timeoutHandler.scheduleTimeout(options.timeout());
            }
            connection.sendRequest(requestId, action, request, options); // local node optimization happens upstream
        } catch (final Exception e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final ProtobufTransport.ResponseContext<? extends ProtobufTransportResponse> contextToNotify = responseHandlers.remove(
                requestId
            );
            // If holderToNotify == null then handler has already been taken care of.
            if (contextToNotify != null) {
                if (timeoutHandler != null) {
                    timeoutHandler.cancel();
                }
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows. In the special case of running into a closing node we run on the current
                // thread on a best effort basis though.
                final ProtobufSendRequestTransportException sendRequestException = new ProtobufSendRequestTransportException(
                    node,
                    action,
                    e
                );
                final String executor = lifecycle.stoppedOrClosed() ? ProtobufThreadPool.Names.SAME : ProtobufThreadPool.Names.GENERIC;
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    public void onRejection(Exception e) {
                        // if we get rejected during node shutdown we don't wanna bubble it up
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on rejection, action: {}",
                                contextToNotify.action()
                            ),
                            e
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "failed to notify response handler on exception, action: {}",
                                contextToNotify.action()
                            ),
                            e
                        );
                    }

                    @Override
                    protected void doRun() throws Exception {
                        contextToNotify.handler().handleException(sendRequestException);
                    }
                });
            } else {
                logger.debug("Exception while sending request, handler likely already notified due to timeout", e);
            }
        }
    }

    private void sendLocalRequest(
        long requestId,
        final String action,
        final ProtobufTransportRequest request,
        TransportRequestOptions options
    ) {
        final DirectResponseChannel channel = new DirectResponseChannel(localNode, action, requestId, this, threadPool);
        try {
            onRequestSent(localNode, requestId, action, request, options);
            onRequestReceived(requestId, action);
            final ProtobufRequestHandlerRegistry reg = getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ProtobufThreadPool.Names.SAME.equals(executor)) {
                // noinspection unchecked
                reg.processMessageReceived(request, channel);
            } else {
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        // noinspection unchecked
                        reg.processMessageReceived(request, channel);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.warn(
                                () -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action),
                                inner
                            );
                        }
                    }

                    @Override
                    public String toString() {
                        return "processing of [" + requestId + "][" + action + "]: " + request;
                    }
                });
            }

        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn(() -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action), inner);
            }
        }
    }

    private boolean shouldTraceAction(String action) {
        return shouldTraceAction(action, tracerLogInclude, tracerLogExclude);
    }

    public static boolean shouldTraceAction(String action, String[] include, String[] exclude) {
        if (include.length > 0) {
            if (Regex.simpleMatch(include, action) == false) {
                return false;
            }
        }
        if (exclude.length > 0) {
            return !Regex.simpleMatch(exclude, action);
        }
        return true;
    }

    public ProtobufTransportAddress[] addressesFromString(String address) throws UnknownHostException {
        return transport.addressesFromString(address);
    }

    /**
     * A set of all valid action prefixes.
    */
    public static final Set<String> VALID_ACTION_PREFIXES = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                "indices:admin",
                "indices:monitor",
                "indices:data/write",
                "indices:data/read",
                "indices:internal",
                "cluster:admin",
                "cluster:monitor",
                "cluster:internal",
                "internal:"
            )
        )
    );

    private void validateActionName(String actionName) {
        // TODO we should makes this a hard validation and throw an exception but we need a good way to add backwards layer
        // for it. Maybe start with a deprecation layer
        if (isValidActionName(actionName) == false) {
            logger.warn(
                "invalid action name [" + actionName + "] must start with one of: " + ProtobufTransportService.VALID_ACTION_PREFIXES
            );
        }
    }

    /**
     * Returns <code>true</code> iff the action name starts with a valid prefix.
    *
    * @see #VALID_ACTION_PREFIXES
    */
    public static boolean isValidActionName(String actionName) {
        for (String prefix : VALID_ACTION_PREFIXES) {
            if (actionName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Registers a new request handler
    *
    * @param action         The action the request handler is associated with
    * @param requestReader  a callable to be used construct new instances for streaming
    * @param executor       The executor the request handling will be executed on
    * @param handler        The handler itself that implements the request handling
    */
    public <Request extends ProtobufTransportRequest> void registerRequestHandler(
        String action,
        String executor,
        ProtobufWriteable.Reader<Request> requestReader,
        ProtobufTransportRequestHandler<Request> handler
    ) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, false, handler);
        ProtobufRequestHandlerRegistry<Request> reg = new ProtobufRequestHandlerRegistry<>(
            action,
            requestReader,
            taskManager,
            handler,
            executor,
            false,
            true
        );
        transport.registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
    *
    * @param action                The action the request handler is associated with
    * @param requestReader               The request class that will be used to construct new instances for streaming
    * @param executor              The executor the request handling will be executed on
    * @param forceExecution        Force execution on the executor queue and never reject it
    * @param canTripCircuitBreaker Check the request size and raise an exception in case the limit is breached.
    * @param handler               The handler itself that implements the request handling
    */
    public <Request extends ProtobufTransportRequest> void registerRequestHandler(
        String action,
        String executor,
        boolean forceExecution,
        boolean canTripCircuitBreaker,
        ProtobufWriteable.Reader<Request> requestReader,
        ProtobufTransportRequestHandler<Request> handler
    ) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        ProtobufRequestHandlerRegistry<Request> reg = new ProtobufRequestHandlerRegistry<>(
            action,
            requestReader,
            taskManager,
            handler,
            executor,
            forceExecution,
            canTripCircuitBreaker
        );
        transport.registerRequestHandler(reg);
    }

    /**
     * called by the {@link ProtobufTransport} implementation when an incoming request arrives but before
    * any parsing of it has happened (with the exception of the requestId and action)
    */
    @Override
    public void onRequestReceived(long requestId, String action) {
        if (handleIncomingRequests.get() == false) {
            throw new IllegalStateException("transport not ready yet to handle incoming requests");
        }
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }
        messageListener.onRequestReceived(requestId, action);
    }

    /** called by the {@link ProtobufTransport} implementation once a request has been sent */
    @Override
    public void onRequestSent(
        ProtobufDiscoveryNode node,
        long requestId,
        String action,
        ProtobufTransportRequest request,
        TransportRequestOptions options
    ) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout());
        }
        messageListener.onRequestSent(node, requestId, action, request, options);
    }

    @Override
    public void onResponseReceived(long requestId, ProtobufTransport.ResponseContext holder) {
        if (holder == null) {
            checkForTimeout(requestId);
        } else if (tracerLog.isTraceEnabled() && shouldTraceAction(holder.action())) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, holder.action(), holder.connection().getNode());
        }
        messageListener.onResponseReceived(requestId, holder);
    }

    /** called by the {@link ProtobufTransport} implementation once a response was sent to calling node */
    @Override
    public void onResponseSent(long requestId, String action, ProtobufTransportResponse response) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] sent response", requestId, action);
        }
        messageListener.onResponseSent(requestId, action, response);
    }

    /** called by the {@link ProtobufTransport} implementation after an exception was sent as a response to an incoming request */
    @Override
    public void onResponseSent(long requestId, String action, Exception e) {
        if (tracerLog.isTraceEnabled() && shouldTraceAction(action)) {
            tracerLog.trace(() -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
        }
        messageListener.onResponseSent(requestId, action, e);
    }

    public ProtobufRequestHandlerRegistry<? extends ProtobufTransportRequest> getRequestHandler(String action) {
        return transport.getRequestHandlers().getHandler(action);
    }

    private void checkForTimeout(long requestId) {
        // lets see if its in the timeout holder, but sync on mutex to make sure any ongoing timeout handling has finished
        final ProtobufDiscoveryNode sourceNode;
        final String action;
        assert responseHandlers.contains(requestId) == false;
        TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
        if (timeoutInfoHolder != null) {
            long time = threadPool.relativeTimeInMillis();
            logger.warn(
                "Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, "
                    + "action [{}], node [{}], id [{}]",
                time - timeoutInfoHolder.sentTime(),
                time - timeoutInfoHolder.timeoutTime(),
                timeoutInfoHolder.action(),
                timeoutInfoHolder.node(),
                requestId
            );
            action = timeoutInfoHolder.action();
            sourceNode = timeoutInfoHolder.node();
        } else {
            logger.warn("ProtobufTransport response handler not found of id [{}]", requestId);
            action = null;
            sourceNode = null;
        }
        // call tracer out of lock
        if (tracerLog.isTraceEnabled() == false) {
            return;
        }
        if (action == null) {
            assert sourceNode == null;
            tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
        } else if (shouldTraceAction(action)) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
        }
    }

    @Override
    public void onConnectionClosed(ProtobufTransport.Connection connection) {
        try {
            List<ProtobufTransport.ResponseContext<? extends ProtobufTransportResponse>> pruned = responseHandlers.prune(
                h -> h.connection().getCacheKey().equals(connection.getCacheKey())
            );
            // callback that an exception happened, but on a different thread since we don't
            // want handlers to worry about stack overflows
            getExecutorService().execute(new Runnable() {
                @Override
                public void run() {
                    for (ProtobufTransport.ResponseContext holderToNotify : pruned) {
                        holderToNotify.handler()
                            .handleException(new ProtobufNodeDisconnectedException(connection.getNode(), holderToNotify.action()));
                    }
                }

                @Override
                public String toString() {
                    return "onConnectionClosed(" + connection.getNode() + ")";
                }
            });
        } catch (OpenSearchRejectedExecutionException ex) {
            logger.debug("Rejected execution on onConnectionClosed", ex);
        }
    }

    final class TimeoutHandler implements Runnable {

        private final long requestId;
        private final long sentTime = threadPool.relativeTimeInMillis();
        private final String action;
        private final ProtobufDiscoveryNode node;
        volatile Scheduler.Cancellable cancellable;

        TimeoutHandler(long requestId, ProtobufDiscoveryNode node, String action) {
            this.requestId = requestId;
            this.node = node;
            this.action = action;
        }

        @Override
        public void run() {
            if (responseHandlers.contains(requestId)) {
                long timeoutTime = threadPool.relativeTimeInMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(node, action, sentTime, timeoutTime));
                // now that we have the information visible via timeoutInfoHandlers, we try to remove the request id
                final ProtobufTransport.ResponseContext<? extends ProtobufTransportResponse> holder = responseHandlers.remove(requestId);
                if (holder != null) {
                    assert holder.action().equals(action);
                    assert holder.connection().getNode().equals(node);
                    holder.handler()
                        .handleException(
                            new ProtobufReceiveTimeoutTransportException(
                                holder.connection().getNode(),
                                holder.action(),
                                "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"
                            )
                        );
                } else {
                    // response was processed, remove timeout info.
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        /**
         * cancels timeout handling. this is a best effort only to avoid running it. remove the requestId from {@link #responseHandlers}
        * to make sure this doesn't run.
        */
        public void cancel() {
            assert responseHandlers.contains(requestId) == false : "cancel must be called after the requestId ["
                + requestId
                + "] has been removed from clientHandlers";
            if (cancellable != null) {
                cancellable.cancel();
            }
        }

        @Override
        public String toString() {
            return "timeout handler for [" + requestId + "][" + action + "]";
        }

        private void scheduleTimeout(TimeValue timeout) {
            this.cancellable = threadPool.schedule(this, timeout, ProtobufThreadPool.Names.GENERIC);
        }
    }

    /**
     * Holder for timeout information
    *
    * @opensearch.internal
    */
    static class TimeoutInfoHolder {

        private final ProtobufDiscoveryNode node;
        private final String action;
        private final long sentTime;
        private final long timeoutTime;

        TimeoutInfoHolder(ProtobufDiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public ProtobufDiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        public long sentTime() {
            return sentTime;
        }

        public long timeoutTime() {
            return timeoutTime;
        }
    }

    /**
     * This handler wrapper ensures that the response thread executes with the correct thread context. Before any of the handle methods
    * are invoked we restore the context.
    */
    public static final class ContextRestoreResponseHandler<T extends ProtobufTransportResponse>
        implements
            ProtobufTransportResponseHandler<T> {

        private final ProtobufTransportResponseHandler<T> delegate;
        private final Supplier<ThreadContext.StoredContext> contextSupplier;
        private volatile TimeoutHandler handler;

        public ContextRestoreResponseHandler(
            Supplier<ThreadContext.StoredContext> contextSupplier,
            ProtobufTransportResponseHandler<T> delegate
        ) {
            this.delegate = delegate;
            this.contextSupplier = contextSupplier;
        }

        @Override
        public T read(CodedInputStream in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            if (handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleResponse(response);
            }
        }

        @Override
        public void handleException(ProtobufTransportException exp) {
            if (handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleException(exp);
            }
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }

        void setTimeoutHandler(TimeoutHandler handler) {
            this.handler = handler;
        }

    }

    /**
     * A channel for a direct response
    *
    * @opensearch.internal
    */
    static class DirectResponseChannel implements ProtobufTransportChannel {
        final ProtobufDiscoveryNode localNode;
        private final String action;
        private final long requestId;
        final ProtobufTransportService service;
        final ProtobufThreadPool threadPool;

        DirectResponseChannel(
            ProtobufDiscoveryNode localNode,
            String action,
            long requestId,
            ProtobufTransportService service,
            ProtobufThreadPool threadPool
        ) {
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.service = service;
            this.threadPool = threadPool;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(ProtobufTransportResponse response) throws IOException {
            service.onResponseSent(requestId, action, response);
            final ProtobufTransportResponseHandler handler = service.responseHandlers.onResponseReceived(requestId, service);
            // ignore if its null, the service logs it
            if (handler != null) {
                final String executor = handler.executor();
                if (ProtobufThreadPool.Names.SAME.equals(executor)) {
                    processResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(new Runnable() {
                        @Override
                        public void run() {
                            processResponse(handler, response);
                        }

                        @Override
                        public String toString() {
                            return "delivery of response to [" + requestId + "][" + action + "]: " + response;
                        }
                    });
                }
            }
        }

        @SuppressWarnings("unchecked")
        protected void processResponse(ProtobufTransportResponseHandler handler, ProtobufTransportResponse response) {
            try {
                handler.handleResponse(response);
            } catch (Exception e) {
                processException(handler, wrapInRemote(new ResponseHandlerFailureTransportException(e)));
            }
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            service.onResponseSent(requestId, action, exception);
            final ProtobufTransportResponseHandler handler = service.responseHandlers.onResponseReceived(requestId, service);
            // ignore if its null, the service logs it
            if (handler != null) {
                final ProtobufRemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ProtobufThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(handler.executor()).execute(new Runnable() {
                        @Override
                        public void run() {
                            processException(handler, rtx);
                        }

                        @Override
                        public String toString() {
                            return "delivery of failure response to [" + requestId + "][" + action + "]: " + exception;
                        }
                    });
                }
            }
        }

        protected ProtobufRemoteTransportException wrapInRemote(Exception e) {
            if (e instanceof ProtobufRemoteTransportException) {
                return (ProtobufRemoteTransportException) e;
            }
            return new ProtobufRemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        protected void processException(final ProtobufTransportResponseHandler handler, final ProtobufRemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage("failed to handle exception for action [{}], handler [{}]", action, handler),
                    e
                );
            }
        }

        @Override
        public String getChannelType() {
            return "direct";
        }

        @Override
        public Version getVersion() {
            return localNode.getVersion();
        }
    }

    /**
     * Returns the internal thread pool
    */
    public ProtobufThreadPool getThreadPool() {
        return threadPool;
    }

    private boolean isLocalNode(ProtobufDiscoveryNode discoveryNode) {
        return Objects.requireNonNull(discoveryNode, "discovery node must not be null").equals(localNode);
    }

    private static final class DelegatingTransportMessageListener implements ProtobufTransportMessageListener {

        private final List<ProtobufTransportMessageListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onRequestReceived(long requestId, String action) {
            for (ProtobufTransportMessageListener listener : listeners) {
                listener.onRequestReceived(requestId, action);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, ProtobufTransportResponse response) {
            for (ProtobufTransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, response);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, Exception error) {
            for (ProtobufTransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, error);
            }
        }

        @Override
        public void onRequestSent(
            ProtobufDiscoveryNode node,
            long requestId,
            String action,
            ProtobufTransportRequest request,
            TransportRequestOptions finalOptions
        ) {
            for (ProtobufTransportMessageListener listener : listeners) {
                listener.onRequestSent(node, requestId, action, request, finalOptions);
            }
        }

        @Override
        public void onResponseReceived(long requestId, ProtobufTransport.ResponseContext holder) {
            for (ProtobufTransportMessageListener listener : listeners) {
                listener.onResponseReceived(requestId, holder);
            }
        }
    }
}
