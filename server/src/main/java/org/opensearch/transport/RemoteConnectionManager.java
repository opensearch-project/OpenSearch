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

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;

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
public class RemoteConnectionManager implements ConnectionManager {

    private final String clusterAlias;
    private final ConnectionManager delegate;
    private final AtomicLong counter = new AtomicLong();
    private volatile List<DiscoveryNode> connectedNodes = Collections.emptyList();

    RemoteConnectionManager(String clusterAlias, ConnectionManager delegate) {
        this.clusterAlias = clusterAlias;
        this.delegate = delegate;
        this.delegate.addListener(new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                addConnectedNode(node);
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                removeConnectedNode(node);
            }
        });
    }

    @Override
    public void connectToNode(
        DiscoveryNode node,
        ConnectionProfile connectionProfile,
        ConnectionManager.ConnectionValidator connectionValidator,
        ActionListener<Void> listener
    ) throws ConnectTransportException {
        delegate.connectToNode(node, connectionProfile, connectionValidator, listener);
    }

    @Override
    public void addListener(TransportConnectionListener listener) {
        delegate.addListener(listener);
    }

    @Override
    public void removeListener(TransportConnectionListener listener) {
        delegate.removeListener(listener);
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener) {
        delegate.openConnection(node, profile, listener);
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
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
    public void setPendingDisconnection(DiscoveryNode node) {
        delegate.setPendingDisconnection(node);
    }

    @Override
    public void clearPendingDisconnections() {
        delegate.clearPendingDisconnections();
    }

    @Override
    public ConnectionProfile getConnectionProfile() {
        return delegate.getConnectionProfile();
    }

    public Transport.Connection getAnyRemoteConnection() {
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

    static final class ProxyConnection implements Transport.Connection {
        private final Transport.Connection connection;
        private final DiscoveryNode targetNode;

        private ProxyConnection(Transport.Connection connection, DiscoveryNode targetNode) {
            this.connection = connection;
            this.targetNode = targetNode;
        }

        @Override
        public DiscoveryNode getNode() {
            return targetNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            connection.sendRequest(
                requestId,
                TransportActionProxy.getProxyAction(action),
                TransportActionProxy.wrapRequest(targetNode, request),
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
