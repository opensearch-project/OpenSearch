/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manager for connecting to remote nodes
*
* @opensearch.internal
*/
public class ProtobufRemoteConnectionManager implements ProtobufConnectionManager {

    private final String clusterAlias;
    private final ProtobufConnectionManager delegate;
    private final AtomicLong counter = new AtomicLong();
    private volatile List<ProtobufDiscoveryNode> connectedNodes = Collections.emptyList();

    ProtobufRemoteConnectionManager(String clusterAlias, ProtobufConnectionManager delegate) {
        this.clusterAlias = clusterAlias;
        this.delegate = delegate;
        this.delegate.addListener(new ProtobufTransportConnectionListener() {
            @Override
            public void onNodeConnected(ProtobufDiscoveryNode node, ProtobufTransport.Connection connection) {
                addConnectedNode(node);
            }

            @Override
            public void onNodeDisconnected(ProtobufDiscoveryNode node, ProtobufTransport.Connection connection) {
                removeConnectedNode(node);
            }
        });
    }

    @Override
    public void connectToNode(
        ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ProtobufConnectionManager.ConnectionValidator connectionValidator,
        ActionListener<Void> listener
    ) throws ConnectTransportException {
        delegate.connectToNode(node, connectionProfile, connectionValidator, listener);
    }

    @Override
    public void addListener(ProtobufTransportConnectionListener listener) {
        delegate.addListener(listener);
    }

    @Override
    public void removeListener(ProtobufTransportConnectionListener listener) {
        delegate.removeListener(listener);
    }

    @Override
    public void openConnection(
        ProtobufDiscoveryNode node,
        ProtobufConnectionProfile profile,
        ActionListener<ProtobufTransport.Connection> listener
    ) {
        delegate.openConnection(node, profile, listener);
    }

    @Override
    public ProtobufTransport.Connection getConnection(ProtobufDiscoveryNode node) {
        try {
            return delegate.getConnection(node);
        } catch (NodeNotConnectedException e) {
            return new ProxyConnection(getAnyRemoteConnection(), node);
        }
    }

    @Override
    public boolean nodeConnected(ProtobufDiscoveryNode node) {
        return delegate.nodeConnected(node);
    }

    @Override
    public void disconnectFromNode(ProtobufDiscoveryNode node) {
        delegate.disconnectFromNode(node);
    }

    @Override
    public ProtobufConnectionProfile getConnectionProfile() {
        return delegate.getConnectionProfile();
    }

    public ProtobufTransport.Connection getAnyRemoteConnection() {
        List<ProtobufDiscoveryNode> localConnectedNodes = this.connectedNodes;
        long curr;
        while ((curr = counter.incrementAndGet()) == Long.MIN_VALUE)
            ;
        if (localConnectedNodes.isEmpty() == false) {
            ProtobufDiscoveryNode nextNode = localConnectedNodes.get(
                Math.toIntExact(Math.floorMod(curr, (long) localConnectedNodes.size()))
            );
            try {
                return delegate.getConnection(nextNode);
            } catch (NodeNotConnectedException e) {
                // Ignore. We will manually create an iterator of open nodes
            }
        }
        Set<ProtobufDiscoveryNode> allConnectionNodes = getAllConnectedNodes();
        for (ProtobufDiscoveryNode connectedNode : allConnectionNodes) {
            try {
                return delegate.getConnection(connectedNode);
            } catch (NodeNotConnectedException e) {
                // Ignore. We will try the next one until all are exhausted.
            }
        }
        throw new NoSuchRemoteClusterException(clusterAlias);
    }

    @Override
    public Set<ProtobufDiscoveryNode> getAllConnectedNodes() {
        return delegate.getAllConnectedNodes();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void close() {
        delegate.closeNoBlock();
    }

    @Override
    public void closeNoBlock() {
        delegate.closeNoBlock();
    }

    private synchronized void addConnectedNode(ProtobufDiscoveryNode addedNode) {
        ArrayList<ProtobufDiscoveryNode> newConnections = new ArrayList<>(this.connectedNodes);
        newConnections.add(addedNode);
        this.connectedNodes = Collections.unmodifiableList(newConnections);
    }

    private synchronized void removeConnectedNode(ProtobufDiscoveryNode removedNode) {
        int newSize = this.connectedNodes.size() - 1;
        ArrayList<ProtobufDiscoveryNode> newConnectedNodes = new ArrayList<>(newSize);
        for (ProtobufDiscoveryNode connectedNode : this.connectedNodes) {
            if (connectedNode.equals(removedNode) == false) {
                newConnectedNodes.add(connectedNode);
            }
        }
        assert newConnectedNodes.size() == newSize : "Expected connection node count: " + newSize + ", Found: " + newConnectedNodes.size();
        this.connectedNodes = Collections.unmodifiableList(newConnectedNodes);
    }

    static final class ProxyConnection implements ProtobufTransport.Connection {
        private final ProtobufTransport.Connection connection;
        private final ProtobufDiscoveryNode targetNode;

        private ProxyConnection(ProtobufTransport.Connection connection, ProtobufDiscoveryNode targetNode) {
            this.connection = connection;
            this.targetNode = targetNode;
        }

        @Override
        public ProtobufDiscoveryNode getNode() {
            return targetNode;
        }

        @Override
        public void sendRequest(long requestId, String action, ProtobufTransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            connection.sendRequest(
                requestId,
                ProtobufTransportActionProxy.getProxyAction(action),
                ProtobufTransportActionProxy.wrapRequest(targetNode, request),
                options
            );
        }

        @Override
        public void close() {
            assert false : "proxy connections must not be closed";
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            connection.addCloseListener(listener);
        }

        @Override
        public boolean isClosed() {
            return connection.isClosed();
        }

        @Override
        public Version getVersion() {
            return connection.getVersion();
        }

        @Override
        public Object getCacheKey() {
            return connection.getCacheKey();
        }
    }
}
