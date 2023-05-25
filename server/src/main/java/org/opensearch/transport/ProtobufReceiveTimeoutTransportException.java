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
 * Thrown when receiving a timeout
*
* @opensearch.internal
*/
public class ProtobufReceiveTimeoutTransportException extends ProtobufActionTransportException {

    public ProtobufReceiveTimeoutTransportException(ProtobufDiscoveryNode node, String action, String msg) {
        super(node.getName(), node.getAddress(), action, msg, null);
    }

    public ProtobufReceiveTimeoutTransportException(CodedInputStream in) throws IOException {
        super(in);
    }

}
