/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.ProtobufOpenSearchException;
import org.opensearch.common.io.stream.ProtobufWriteable;

import java.io.IOException;

/**
 * Base exception for a failed node
*
* @opensearch.internal
*/
public class ProtobufFailedNodeException extends ProtobufOpenSearchException implements ProtobufWriteable {

    private final String nodeId;

    public ProtobufFailedNodeException(String nodeId, String msg, Throwable cause) {
        super(msg);
        this.nodeId = nodeId;
    }

    public String nodeId() {
        return this.nodeId;
    }

    public ProtobufFailedNodeException(CodedInputStream in) throws IOException {
        super(in);
        nodeId = in.readString();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        out.writeStringNoTag(nodeId);
    }
}
