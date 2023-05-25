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

import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.transport.ProtobufTransportAddress;

import java.io.IOException;

/**
 * An action invocation failure.
*
* @opensearch.internal
*/
public class ProtobufActionTransportException extends ProtobufTransportException {

    private final ProtobufTransportAddress address;

    private final String action;

    public ProtobufActionTransportException(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        address = protobufStreamInput.readOptionalWriteable(ProtobufTransportAddress::new);
        action = protobufStreamInput.readOptionalString();
    }

    public ProtobufActionTransportException(String name, ProtobufTransportAddress address, String action, Throwable cause) {
        super(buildMessage(name, address, action, null), cause);
        this.address = address;
        this.action = action;
    }

    public ProtobufActionTransportException(String name, ProtobufTransportAddress address, String action, String msg, Throwable cause) {
        super(buildMessage(name, address, action, msg), cause);
        this.address = address;
        this.action = action;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeOptionalWriteable(address);
        protobufStreamOutput.writeOptionalString(action);
    }

    /**
     * The target address to invoke the action on.
    */
    public ProtobufTransportAddress address() {
        return address;
    }

    /**
     * The action to invoke.
    */
    public String action() {
        return action;
    }

    private static String buildMessage(String name, ProtobufTransportAddress address, String action, String msg) {
        StringBuilder sb = new StringBuilder();
        if (name != null) {
            sb.append('[').append(name).append(']');
        }
        if (address != null) {
            sb.append('[').append(address).append(']');
        }
        if (action != null) {
            sb.append('[').append(action).append(']');
        }
        if (msg != null) {
            sb.append(" ").append(msg);
        }
        return sb.toString();
    }
}
