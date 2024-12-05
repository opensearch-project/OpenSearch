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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ListenableFuture;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.RunOnce;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages node connections within a cluster. The connection is opened by the underlying transport.
 * Once the connection is opened, this class manages the connection. This includes closing the connection when
 * the connection manager is closed.
 *
 * @opensearch.internal
 */
public class ClusterConnectionManager implements ConnectionManager {

    private static final Logger logger = LogManager.getLogger(ClusterConnectionManager.class);

    private final ConcurrentMap<DiscoveryNode, Transport.Connection> connectedNodes = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<DiscoveryNode, ListenableFuture<Void>> pendingConnections = ConcurrentCollections.newConcurrentMap();
    /**
     This set is used only by cluster-manager nodes.
     Nodes are marked as pending disconnect right before cluster state publish phase.
     They are cleared up as part of cluster state apply commit phase
     This is to avoid connections from being made to nodes that are in the process of leaving the cluster
     Note: If a disconnect is initiated while a connect is in progress, this Set will not handle this case.
     Callers need to ensure that connects and disconnects are sequenced.
     */
    private final Set<DiscoveryNode> pendingDisconnections = ConcurrentCollections.newConcurrentSet();
    private final AbstractRefCounted connectingRefCounter = new AbstractRefCounted("connection manager") {
        @Override
        protected void closeInternal() {
            Iterator<Map.Entry<DiscoveryNode, Transport.Connection>> iterator = connectedNodes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<DiscoveryNode, Transport.Connection> next = iterator.next();
                try {
                    IOUtils.closeWhileHandlingException(next.getValue());
                } finally {
                    iterator.remove();
                }
            }
            closeLatch.countDown();
        }
    };
    private final Transport transport;
    private final ConnectionProfile defaultProfile;
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private final DelegatingNodeConnectionListener connectionListener = new DelegatingNodeConnectionListener();

    public ClusterConnectionManager(Settings settings, Transport transport) {
        this(ConnectionProfile.buildDefaultConnectionProfile(settings), transport);
    }

    public ClusterConnectionManager(ConnectionProfile connectionProfile, Transport transport) {
        this.transport = transport;
        this.defaultProfile = connectionProfile;
    }

    @Override
    public void addListener(TransportConnectionListener listener) {
        this.connectionListener.addListener(listener);
    }

    @Override
    public void removeListener(TransportConnectionListener listener) {
        this.connectionListener.removeListener(listener);
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Transport.Connection> listener) {
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        internalOpenConnection(node, resolvedProfile, listener);
    }

    /**
     * Connects to a node with the given connection profile. If the node is already connected this method has no effect.
     * Once a successful is established, it can be validated before being exposed.
     * The ActionListener will be called on the calling thread or the generic thread pool.
     */
    @Override
    public void connectToNode(
        DiscoveryNode node,
        ConnectionProfile connectionProfile,
        ConnectionValidator connectionValidator,
        ActionListener<Void> listener
    ) throws ConnectTransportException {
        logger.trace("connecting to node [{}]", node);
        ConnectionProfile resolvedProfile = ConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        if (node == null) {
            listener.onFailure(new ConnectTransportException(null, "can't connect to a null node"));
            return;
        }

        // if node-left is still in progress, we fail the connect request early
        if (pendingDisconnections.contains(node)) {
            listener.onFailure(new IllegalStateException("cannot make a new connection as disconnect to node [" + node + "] is pending"));
            return;
        }

        if (connectingRefCounter.tryIncRef() == false) {
            listener.onFailure(new IllegalStateException("connection manager is closed"));
            return;
        }

        if (connectedNodes.containsKey(node)) {
            connectingRefCounter.decRef();
            listener.onResponse(null);
            return;
        }

        final ListenableFuture<Void> currentListener = new ListenableFuture<>();
        final ListenableFuture<Void> existingListener = pendingConnections.putIfAbsent(node, currentListener);
        if (existingListener != null) {
            try {
                // wait on previous entry to complete connection attempt
                existingListener.addListener(listener, OpenSearchExecutors.newDirectExecutorService());
            } finally {
                connectingRefCounter.decRef();
            }
            return;
        }

        currentListener.addListener(listener, OpenSearchExecutors.newDirectExecutorService());

        final RunOnce releaseOnce = new RunOnce(connectingRefCounter::decRef);
        internalOpenConnection(node, resolvedProfile, ActionListener.wrap(conn -> {
            connectionValidator.validate(conn, resolvedProfile, ActionListener.wrap(ignored -> {
                assert Transports.assertNotTransportThread("connection validator success");
                try {
                    if (connectedNodes.putIfAbsent(node, conn) != null) {
                        logger.debug("existing connection to node [{}], closing new redundant connection", node);
                        IOUtils.closeWhileHandlingException(conn);
                    } else {
                        logger.debug("connected to node [{}]", node);
                        try {
                            connectionListener.onNodeConnected(node, conn);
                        } finally {
                            final Transport.Connection finalConnection = conn;
                            conn.addCloseListener(ActionListener.wrap(() -> {
                                logger.trace("unregistering {} after connection close and marking as disconnected", node);
                                connectedNodes.remove(node, finalConnection);
                                pendingDisconnections.remove(node);
                                connectionListener.onNodeDisconnected(node, conn);
                            }));
                        }
                    }
                } finally {
                    ListenableFuture<Void> future = pendingConnections.remove(node);
                    assert future == currentListener : "Listener in pending map is different than the expected listener";
                    releaseOnce.run();
                    future.onResponse(null);
                }
            }, e -> {
                assert Transports.assertNotTransportThread("connection validator failure");
                IOUtils.closeWhileHandlingException(conn);
                failConnectionListeners(node, releaseOnce, e, currentListener);
            }));
        }, e -> {
            assert Transports.assertNotTransportThread("internalOpenConnection failure");
            failConnectionListeners(node, releaseOnce, e, currentListener);
        }));
    }

    /**
     * Returns a connection for the given node if the node is connected.
     * Connections returned from this method must not be closed. The lifecycle of this connection is
     * maintained by this connection manager
     *
     * @throws NodeNotConnectedException if the node is not connected
     * @see #connectToNode(DiscoveryNode, ConnectionProfile, ConnectionValidator, ActionListener)
     */
    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        Transport.Connection connection = connectedNodes.get(node);
        if (connection == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return connection;
    }

    /**
     * Returns {@code true} if the node is connected.
     */
    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    /**
     * Disconnected from the given node, if not connected, will do nothing.
     */
    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        Transport.Connection nodeChannels = connectedNodes.remove(node);
        if (nodeChannels != null) {
            // if we found it and removed it we close
            nodeChannels.close();
        }
        pendingDisconnections.remove(node);
        logger.trace("Removed node [{}] from pending disconnections list", node);
    }

    @Override
    public void setPendingDisconnection(DiscoveryNode node) {
        logger.trace("marking disconnection as pending for node: [{}]", node);
        pendingDisconnections.add(node);
    }

    @Override
    public void clearPendingDisconnections() {
        pendingDisconnections.clear();
    }

    /**
     * Returns the number of nodes this manager is connected to.
     */
    @Override
    public int size() {
        return connectedNodes.size();
    }

    @Override
    public Set<DiscoveryNode> getAllConnectedNodes() {
        return Collections.unmodifiableSet(connectedNodes.keySet());
    }

    @Override
    public void close() {
        internalClose(true);
    }

    @Override
    public void closeNoBlock() {
        internalClose(false);
    }

    private void internalClose(boolean waitForPendingConnections) {
        assert Transports.assertNotTransportThread("Closing ConnectionManager");
        if (closing.compareAndSet(false, true)) {
            connectingRefCounter.decRef();
            if (waitForPendingConnections) {
                try {
                    closeLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private void internalOpenConnection(
        DiscoveryNode node,
        ConnectionProfile connectionProfile,
        ActionListener<Transport.Connection> listener
    ) {
        transport.openConnection(node, connectionProfile, ActionListener.map(listener, connection -> {
            assert Transports.assertNotTransportThread("internalOpenConnection success");
            try {
                connectionListener.onConnectionOpened(connection);
            } finally {
                connection.addCloseListener(ActionListener.wrap(() -> connectionListener.onConnectionClosed(connection)));
            }
            if (connection.isClosed()) {
                throw new ConnectTransportException(node, "a channel closed while connecting");
            }
            return connection;
        }));
    }

    private void failConnectionListeners(DiscoveryNode node, RunOnce releaseOnce, Exception e, ListenableFuture<Void> expectedListener) {
        ListenableFuture<Void> future = pendingConnections.remove(node);
        releaseOnce.run();
        if (future != null) {
            assert future == expectedListener : "Listener in pending map is different than the expected listener";
            future.onFailure(e);
        }
    }

    @Override
    public ConnectionProfile getConnectionProfile() {
        return defaultProfile;
    }

}
