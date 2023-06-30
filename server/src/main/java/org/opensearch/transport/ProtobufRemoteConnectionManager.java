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
import org.opensearch.cluster.node.DiscoveryNode;

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
    private volatile List<DiscoveryNode> connectedNodes = Collections.emptyList();

    ProtobufRemoteConnectionManager(String clusterAlias, ProtobufConnectionManager delegate) {
        this.clusterAlias = clusterAlias;
        this.delegate = delegate;
        this.delegate.addListener(new ProtobufTransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.ProtobufConnection connection) {
                addConnectedNode(node);
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.ProtobufConnection connection) {
                removeConnectedNode(node);
            }
        });
    }

    @Override
    public void connectToNode(
        DiscoveryNode node,
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
        DiscoveryNode node,
        ProtobufConnectionProfile profile,
        ActionListener<Transport.ProtobufConnection> listener
    ) {
        delegate.openConnection(node, profile, listener);
    }

    @Override
    public Transport.ProtobufConnection getConnection(DiscoveryNode node) {
        try {
            return delegate.getConnection(node);
        } catch (NodeNotConnectedException e) {
            return new ProxyConnection(getAnyRemoteConnection(), node);
        }
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return delegate.nodeConnected(node);
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        delegate.disconnectFromNode(node);
    }

    @Override
    public ProtobufConnectionProfile getConnectionProfile() {
        return delegate.getConnectionProfile();
    }

    public Transport.ProtobufConnection getAnyRemoteConnection() {
        List<DiscoveryNode> localConnectedNodes = this.connectedNodes;
        long curr;
        while ((curr = counter.incrementAndGet()) == Long.MIN_VALUE)
            ;
        if (localConnectedNodes.isEmpty() == false) {
            DiscoveryNode nextNode = localConnectedNodes.get(Math.toIntExact(Math.floorMod(curr, (long) localConnectedNodes.size())));
            try {
                return delegate.getConnection(nextNode);
            } catch (NodeNotConnectedException e) {
                // Ignore. We will manually create an iterator of open nodes
            }
        }
        Set<DiscoveryNode> allConnectionNodes = getAllConnectedNodes();
        for (DiscoveryNode connectedNode : allConnectionNodes) {
            try {
                return delegate.getConnection(connectedNode);
            } catch (NodeNotConnectedException e) {
                // Ignore. We will try the next one until all are exhausted.
            }
        }
        throw new NoSuchRemoteClusterException(clusterAlias);
    }

    @Override
    public Set<DiscoveryNode> getAllConnectedNodes() {
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

    private synchronized void addConnectedNode(DiscoveryNode addedNode) {
        ArrayList<DiscoveryNode> newConnections = new ArrayList<>(this.connectedNodes);
        newConnections.add(addedNode);
        this.connectedNodes = Collections.unmodifiableList(newConnections);
    }

    private synchronized void removeConnectedNode(DiscoveryNode removedNode) {
        int newSize = this.connectedNodes.size() - 1;
        ArrayList<DiscoveryNode> newConnectedNodes = new ArrayList<>(newSize);
        for (DiscoveryNode connectedNode : this.connectedNodes) {
            if (connectedNode.equals(removedNode) == false) {
                newConnectedNodes.add(connectedNode);
            }
        }
        assert newConnectedNodes.size() == newSize : "Expected connection node count: " + newSize + ", Found: " + newConnectedNodes.size();
        this.connectedNodes = Collections.unmodifiableList(newConnectedNodes);
    }

    static final class ProxyConnection implements Transport.ProtobufConnection {
        private final Transport.ProtobufConnection connection;
        private final DiscoveryNode targetNode;

        private ProxyConnection(Transport.ProtobufConnection connection, DiscoveryNode targetNode) {
            this.connection = connection;
            this.targetNode = targetNode;
        }

        @Override
        public DiscoveryNode getNode() {
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
