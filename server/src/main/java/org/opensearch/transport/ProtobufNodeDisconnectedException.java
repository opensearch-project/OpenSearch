/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;

import java.io.IOException;

/**
 * Exception thrown if a node disconnects
*
* @opensearch.internal
*/
public class ProtobufNodeDisconnectedException extends ProtobufConnectTransportException {

    public ProtobufNodeDisconnectedException(ProtobufDiscoveryNode node, String action) {
        super(node, "disconnected", action, null);
    }

    public ProtobufNodeDisconnectedException(CodedInputStream in) throws IOException {
        super(in);
    }

    // stack trace is meaningless...

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
