/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.cluster.node.ProtobufDiscoveryNode;

/**
 * A listener interface that allows to react on transport events. All methods may be
* executed on network threads. Consumers must fork in the case of long running or blocking
* operations.
*
* @opensearch.internal
*/
public interface ProtobufTransportConnectionListener {

    /**
     * Called once a connection was opened
    * @param connection the connection
    */
    default void onConnectionOpened(Transport.ProtobufConnection connection) {}

    /**
     * Called once a connection ws closed.
    * @param connection the closed connection
    */
    default void onConnectionClosed(Transport.ProtobufConnection connection) {}

    /**
     * Called once a node connection is opened and registered.
    */
    default void onNodeConnected(ProtobufDiscoveryNode node, Transport.ProtobufConnection connection) {}

    /**
     * Called once a node connection is closed and unregistered.
    */
    default void onNodeDisconnected(ProtobufDiscoveryNode node, Transport.ProtobufConnection connection) {}
}
