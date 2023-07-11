/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/** not used
 * ProtobufTransport connection manager.
*
* @opensearch.internal
*/
public interface ProtobufConnectionManager extends Closeable {

    void addListener(TransportConnectionListener listener);

    void removeListener(TransportConnectionListener listener);

    void openConnection(
        DiscoveryNode node,
        ConnectionProfile connectionProfile,
        ActionListener<Transport.Connection> listener
    );

    void connectToNode(
        DiscoveryNode node,
        ConnectionProfile connectionProfile,
        ConnectionValidator connectionValidator,
        ActionListener<Void> listener
    ) throws ConnectTransportException;

    Transport.Connection getConnection(DiscoveryNode node);

    boolean nodeConnected(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    Set<DiscoveryNode> getAllConnectedNodes();

    int size();

    @Override
    void close();

    void closeNoBlock();

    ConnectionProfile getConnectionProfile();

    /**
     * Validates a connection
    *
    * @opensearch.internal
    */
    @FunctionalInterface
    interface ConnectionValidator {
        void validate(Transport.Connection connection, ConnectionProfile profile, ActionListener<Void> listener);
    }

    /**
     * Connection listener for a delegating node
    *
    * @opensearch.internal
    */
    final class DelegatingNodeConnectionListener implements TransportConnectionListener {

        private final CopyOnWriteArrayList<TransportConnectionListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onNodeDisconnected(DiscoveryNode key, Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onNodeDisconnected(key, connection);
            }
        }

        @Override
        public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onNodeConnected(node, connection);
            }
        }

        @Override
        public void onConnectionOpened(Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onConnectionOpened(connection);
            }
        }

        @Override
        public void onConnectionClosed(Transport.Connection connection) {
            for (TransportConnectionListener listener : listeners) {
                listener.onConnectionClosed(connection);
            }
        }

        public void addListener(TransportConnectionListener listener) {
            listeners.addIfAbsent(listener);
        }

        public void removeListener(TransportConnectionListener listener) {
            listeners.remove(listener);
        }
    }
}
