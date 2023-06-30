/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import org.opensearch.cluster.node.DiscoveryNode;

import java.io.IOException;

/**
 * An exception indicating that a message is sent to a node that is not connected.
*
* @opensearch.internal
*/
public class ProtobufNodeNotConnectedException extends ProtobufConnectTransportException {

    public ProtobufNodeNotConnectedException(DiscoveryNode node, String msg) {
        super(node, msg, (String) null);
    }

    public ProtobufNodeNotConnectedException(CodedInputStream in) throws IOException {
        super(in);
    }
}
