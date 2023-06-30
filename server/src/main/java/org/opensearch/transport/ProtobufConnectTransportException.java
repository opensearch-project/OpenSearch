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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;

import java.io.IOException;

/**
 * Transport connection exception
*
* @opensearch.internal
*/
public class ProtobufConnectTransportException extends ProtobufActionTransportException {

    private final DiscoveryNode node;

    public ProtobufConnectTransportException(DiscoveryNode node, String msg) {
        this(node, msg, null, null);
    }

    public ProtobufConnectTransportException(DiscoveryNode node, String msg, String action) {
        this(node, msg, action, null);
    }

    public ProtobufConnectTransportException(DiscoveryNode node, String msg, Throwable cause) {
        this(node, msg, null, cause);
    }

    public ProtobufConnectTransportException(DiscoveryNode node, String msg, String action, Throwable cause) {
        super(node == null ? null : node.getName(), node == null ? null : node.getProtobufAddress(), action, msg, cause);
        this.node = node;
    }

    public ProtobufConnectTransportException(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        node = protobufStreamInput.readOptionalWriteable(DiscoveryNode::new);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeOptionalWriteable(node);
    }

    public DiscoveryNode node() {
        return node;
    }
}
