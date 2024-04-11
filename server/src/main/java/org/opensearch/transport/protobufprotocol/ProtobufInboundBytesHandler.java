/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.protobufprotocol;

import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.InboundBytesHandler;
import org.opensearch.transport.ProtocolInboundMessage;
import org.opensearch.transport.TcpChannel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.function.BiConsumer;

/**
  * Handler for inbound bytes for the protobuf protocol.
  */
public class ProtobufInboundBytesHandler implements InboundBytesHandler {

    public ProtobufInboundBytesHandler() {}

    @Override
    public void doHandleBytes(
        TcpChannel channel,
        ReleasableBytesReference reference,
        BiConsumer<TcpChannel, ProtocolInboundMessage> messageHandler
    ) throws IOException {
        // removing the first byte we added for protobuf message
        byte[] incomingBytes = BytesReference.toBytes(reference.slice(3, reference.length() - 3));
        ProtobufInboundMessage protobufMessage = new ProtobufInboundMessage(new ByteArrayInputStream(incomingBytes));
        messageHandler.accept(channel, protobufMessage);
    }

    @Override
    public boolean canHandleBytes(ReleasableBytesReference reference) {
        if (reference.get(0) == 'O' && reference.get(1) == 'S' && reference.get(2) == 'P') {
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        // no-op
    }

}
