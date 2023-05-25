/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ListenableFuture;
import org.opensearch.common.util.concurrent.RunOnce;
import org.opensearch.common.util.io.IOUtils;

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
public class ProtobufClusterConnectionManager implements ProtobufConnectionManager {

    private static final Logger logger = LogManager.getLogger(ProtobufClusterConnectionManager.class);

    private final ConcurrentMap<ProtobufDiscoveryNode, ProtobufTransport.Connection> connectedNodes = ConcurrentCollections
        .newConcurrentMap();
    private final ConcurrentMap<ProtobufDiscoveryNode, ListenableFuture<Void>> pendingConnections = ConcurrentCollections
        .newConcurrentMap();
    private final AbstractRefCounted connectingRefCounter = new AbstractRefCounted("connection manager") {
        @Override
        protected void closeInternal() {
            Iterator<Map.Entry<ProtobufDiscoveryNode, ProtobufTransport.Connection>> iterator = connectedNodes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ProtobufDiscoveryNode, ProtobufTransport.Connection> next = iterator.next();
                try {
                    IOUtils.closeWhileHandlingException(next.getValue());
                } finally {
                    iterator.remove();
                }
            }
            closeLatch.countDown();
        }
    };
    private final ProtobufTransport transport;
    private final ProtobufConnectionProfile defaultProfile;
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private final DelegatingNodeConnectionListener connectionListener = new DelegatingNodeConnectionListener();

    public ProtobufClusterConnectionManager(Settings settings, ProtobufTransport transport) {
        this(ProtobufConnectionProfile.buildDefaultConnectionProfile(settings), transport);
    }

    public ProtobufClusterConnectionManager(ProtobufConnectionProfile connectionProfile, ProtobufTransport transport) {
        this.transport = transport;
        this.defaultProfile = connectionProfile;
    }

    @Override
    public void addListener(ProtobufTransportConnectionListener listener) {
        this.connectionListener.addListener(listener);
    }

    @Override
    public void removeListener(ProtobufTransportConnectionListener listener) {
        this.connectionListener.removeListener(listener);
    }

    @Override
    public void openConnection(
        ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ActionListener<ProtobufTransport.Connection> listener
    ) {
        ProtobufConnectionProfile resolvedProfile = ProtobufConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        internalOpenConnection(node, resolvedProfile, listener);
    }

    /**
     * Connects to a node with the given connection profile. If the node is already connected this method has no effect.
    * Once a successful is established, it can be validated before being exposed.
    * The ActionListener will be called on the calling thread or the generic thread pool.
    */
    @Override
    public void connectToNode(
        ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ConnectionValidator connectionValidator,
        ActionListener<Void> listener
    ) throws ProtobufConnectTransportException {
        ProtobufConnectionProfile resolvedProfile = ProtobufConnectionProfile.resolveConnectionProfile(connectionProfile, defaultProfile);
        if (node == null) {
            listener.onFailure(new ProtobufConnectTransportException(null, "can't connect to a null node"));
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
                            final ProtobufTransport.Connection finalConnection = conn;
                            conn.addCloseListener(ActionListener.wrap(() -> {
                                logger.trace("unregistering {} after connection close and marking as disconnected", node);
                                connectedNodes.remove(node, finalConnection);
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
    * @throws ProtobufNodeNotConnectedException if the node is not connected
    * @see #connectToNode(ProtobufDiscoveryNode, ProtobufConnectionProfile, ConnectionValidator, ActionListener)
    */
    @Override
    public ProtobufTransport.Connection getConnection(ProtobufDiscoveryNode node) {
        ProtobufTransport.Connection connection = connectedNodes.get(node);
        if (connection == null) {
            throw new ProtobufNodeNotConnectedException(node, "Node not connected");
        }
        return connection;
    }

    /**
     * Returns {@code true} if the node is connected.
    */
    @Override
    public boolean nodeConnected(ProtobufDiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    /**
     * Disconnected from the given node, if not connected, will do nothing.
    */
    @Override
    public void disconnectFromNode(ProtobufDiscoveryNode node) {
        ProtobufTransport.Connection nodeChannels = connectedNodes.remove(node);
        if (nodeChannels != null) {
            // if we found it and removed it we close
            nodeChannels.close();
        }
    }

    /**
     * Returns the number of nodes this manager is connected to.
    */
    @Override
    public int size() {
        return connectedNodes.size();
    }

    @Override
    public Set<ProtobufDiscoveryNode> getAllConnectedNodes() {
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
        assert Transports.assertNotTransportThread("Closing ProtobufConnectionManager");
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
        ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ActionListener<ProtobufTransport.Connection> listener
    ) {
        transport.openConnection(node, connectionProfile, ActionListener.map(listener, connection -> {
            assert Transports.assertNotTransportThread("internalOpenConnection success");
            try {
                connectionListener.onConnectionOpened(connection);
            } finally {
                connection.addCloseListener(ActionListener.wrap(() -> connectionListener.onConnectionClosed(connection)));
            }
            if (connection.isClosed()) {
                throw new ProtobufConnectTransportException(node, "a channel closed while connecting");
            }
            return connection;
        }));
    }

    private void failConnectionListeners(
        ProtobufDiscoveryNode node,
        RunOnce releaseOnce,
        Exception e,
        ListenableFuture<Void> expectedListener
    ) {
        ListenableFuture<Void> future = pendingConnections.remove(node);
        releaseOnce.run();
        if (future != null) {
            assert future == expectedListener : "Listener in pending map is different than the expected listener";
            future.onFailure(e);
        }
    }

    @Override
    public ProtobufConnectionProfile getConnectionProfile() {
        return defaultProfile;
    }

}
