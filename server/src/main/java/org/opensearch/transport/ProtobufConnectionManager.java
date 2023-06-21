/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ProtobufTransport connection manager.
*
* @opensearch.internal
*/
public interface ProtobufConnectionManager extends Closeable {

    void addListener(ProtobufTransportConnectionListener listener);

    void removeListener(ProtobufTransportConnectionListener listener);

    void openConnection(
        ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ActionListener<Transport.ProtobufConnection> listener
    );

    void connectToNode(
        ProtobufDiscoveryNode node,
        ProtobufConnectionProfile connectionProfile,
        ConnectionValidator connectionValidator,
        ActionListener<Void> listener
    ) throws ConnectTransportException;

    Transport.ProtobufConnection getConnection(ProtobufDiscoveryNode node);

    boolean nodeConnected(ProtobufDiscoveryNode node);

    void disconnectFromNode(ProtobufDiscoveryNode node);

    Set<ProtobufDiscoveryNode> getAllConnectedNodes();

    int size();

    @Override
    void close();

    void closeNoBlock();

    ProtobufConnectionProfile getConnectionProfile();

    /**
     * Validates a connection
    *
    * @opensearch.internal
    */
    @FunctionalInterface
    interface ConnectionValidator {
        void validate(Transport.ProtobufConnection connection, ProtobufConnectionProfile profile, ActionListener<Void> listener);
    }

    /**
     * Connection listener for a delegating node
    *
    * @opensearch.internal
    */
    final class DelegatingNodeConnectionListener implements ProtobufTransportConnectionListener {

        private final CopyOnWriteArrayList<ProtobufTransportConnectionListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onNodeDisconnected(ProtobufDiscoveryNode key, Transport.ProtobufConnection connection) {
            for (ProtobufTransportConnectionListener listener : listeners) {
                listener.onNodeDisconnected(key, connection);
            }
        }

        @Override
        public void onNodeConnected(ProtobufDiscoveryNode node, Transport.ProtobufConnection connection) {
            for (ProtobufTransportConnectionListener listener : listeners) {
                listener.onNodeConnected(node, connection);
            }
        }

        @Override
        public void onConnectionOpened(Transport.ProtobufConnection connection) {
            for (ProtobufTransportConnectionListener listener : listeners) {
                listener.onConnectionOpened(connection);
            }
        }

        @Override
        public void onConnectionClosed(Transport.ProtobufConnection connection) {
            for (ProtobufTransportConnectionListener listener : listeners) {
                listener.onConnectionClosed(connection);
            }
        }

        public void addListener(ProtobufTransportConnectionListener listener) {
            listeners.addIfAbsent(listener);
        }

        public void removeListener(ProtobufTransportConnectionListener listener) {
            listeners.remove(listener);
        }
    }
}
