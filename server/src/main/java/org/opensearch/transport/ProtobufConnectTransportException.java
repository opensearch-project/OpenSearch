/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;

import java.io.IOException;

/**
 * Transport connection exception
*
* @opensearch.internal
*/
public class ProtobufConnectTransportException extends ProtobufActionTransportException {

    private final ProtobufDiscoveryNode node;

    public ProtobufConnectTransportException(ProtobufDiscoveryNode node, String msg) {
        this(node, msg, null, null);
    }

    public ProtobufConnectTransportException(ProtobufDiscoveryNode node, String msg, String action) {
        this(node, msg, action, null);
    }

    public ProtobufConnectTransportException(ProtobufDiscoveryNode node, String msg, Throwable cause) {
        this(node, msg, null, cause);
    }

    public ProtobufConnectTransportException(ProtobufDiscoveryNode node, String msg, String action, Throwable cause) {
        super(node == null ? null : node.getName(), node == null ? null : node.getAddress(), action, msg, cause);
        this.node = node;
    }

    public ProtobufConnectTransportException(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        node = protobufStreamInput.readOptionalWriteable(ProtobufDiscoveryNode::new);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeOptionalWriteable(node);
    }

    public ProtobufDiscoveryNode node() {
        return node;
    }
}
