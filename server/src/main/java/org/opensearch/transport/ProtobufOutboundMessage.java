/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedOutputStream;
import org.opensearch.Version;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Outbound data as a message
*
* @opensearch.internal
*/
abstract class ProtobufOutboundMessage extends ProtobufNetworkMessage {

    private final ProtobufWriteable message;

    ProtobufOutboundMessage(ThreadContext threadContext, Version version, byte status, long requestId, ProtobufWriteable message) {
        super(threadContext, version, status, requestId);
        this.message = message;
    }

    BytesReference serialize(CodedOutputStream out, byte[] bytes) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.setVersion(version);
        // bytesStream.skip(TcpHeader.headerSize(version));

        // The compressible bytes stream will not close the underlying bytes stream
        BytesReference reference;
        int variableHeaderLength = -1;
        // final long preHeaderPosition = out.position();
        writeVariableHeader(out);
        // variableHeaderLength = Math.toIntExact(out.position() - preHeaderPosition);

        if (TransportStatus.isCompress(status)) {
            protobufStreamOutput.setVersion(version);
            protobufStreamOutput.setFeatures(protobufStreamOutput.getFeatures());
        }
        // try (CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(out, TransportStatus.isCompress(status))) {
        // stream.setVersion(version);
        // stream.setFeatures(out.getFeatures());

        // if (variableHeaderLength == -1) {
        // writeVariableHeader(stream);
        // }
        reference = writeMessage(out, bytes);
        // }

        // out.seek(0);
        final int contentSize = reference.length() - TcpHeader.headerSize(version);
        TcpHeader.writeHeaderProtobuf(out, requestId, status, version, contentSize, variableHeaderLength);
        return reference;
    }

    protected void writeVariableHeader(CodedOutputStream stream) throws IOException {
        threadContext.writeTo(stream);
    }

    protected BytesReference writeMessage(CodedOutputStream stream, byte[] bytes) throws IOException {
        final BytesReference zeroCopyBuffer;
        if (message instanceof ProtobufBytesTransportRequest) {
            ProtobufBytesTransportRequest bRequest = (ProtobufBytesTransportRequest) message;
            bRequest.writeThin(stream);
            zeroCopyBuffer = bRequest.bytes;
        } else if (message instanceof ProtobufRemoteTransportException) {
            // stream.writeStringNoTag((ProtobufRemoteTransportException) message.toString());
            zeroCopyBuffer = BytesArray.EMPTY;
        } else {
            message.writeTo(stream);
            zeroCopyBuffer = BytesArray.EMPTY;
        }
        // we have to call materializeBytes() here before accessing the bytes. A CompressibleBytesOutputStream
        // might be implementing compression. And materializeBytes() ensures that some marker bytes (EOS marker)
        // are written. Otherwise we barf on the decompressing end when we read past EOF on purpose in the
        // #validateRequest method. this might be a problem in deflate after all but it's important to write
        // the marker bytes.
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        final BytesReference message = BytesReference.fromByteBuffer(byteBuffer);
        // if (zeroCopyBuffer.length() == 0) {
        // return message;
        // } else {
        // return CompositeBytesReference.of(message, zeroCopyBuffer);
        // }
        return message;
    }

    /**
     * Internal outbound message request
    *
    * @opensearch.internal
    */
    static class Request extends ProtobufOutboundMessage {

        private final String[] features;
        private final String action;

        Request(
            ThreadContext threadContext,
            String[] features,
            ProtobufWriteable message,
            Version version,
            String action,
            long requestId,
            boolean isHandshake,
            boolean compress
        ) {
            super(threadContext, version, setStatus(compress, isHandshake, message), requestId, message);
            this.features = features;
            this.action = action;
        }

        @Override
        protected void writeVariableHeader(CodedOutputStream out) throws IOException {
            super.writeVariableHeader(out);
            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            protobufStreamOutput.writeStringArray(features);
            out.writeStringNoTag(action);
        }

        private static byte setStatus(boolean compress, boolean isHandshake, ProtobufWriteable message) {
            byte status = 0;
            status = TransportStatus.setRequest(status);
            if (compress && ProtobufOutboundMessage.canCompress(message)) {
                status = TransportStatus.setCompress(status);
            }
            if (isHandshake) {
                status = TransportStatus.setHandshake(status);
            }

            return status;
        }
    }

    /**
     * Internal message response
    *
    * @opensearch.internal
    */
    static class Response extends ProtobufOutboundMessage {

        private final Set<String> features;

        Response(
            ThreadContext threadContext,
            Set<String> features,
            ProtobufWriteable message,
            Version version,
            long requestId,
            boolean isHandshake,
            boolean compress
        ) {
            super(threadContext, version, setStatus(compress, isHandshake, message), requestId, message);
            this.features = features;
        }

        @Override
        protected void writeVariableHeader(CodedOutputStream out) throws IOException {
            super.writeVariableHeader(out);
            ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            protobufStreamOutput.setFeatures(features);
        }

        private static byte setStatus(boolean compress, boolean isHandshake, ProtobufWriteable message) {
            byte status = 0;
            status = TransportStatus.setResponse(status);
            if (message instanceof RemoteTransportException) {
                status = TransportStatus.setError(status);
            }
            if (compress) {
                status = TransportStatus.setCompress(status);
            }
            if (isHandshake) {
                status = TransportStatus.setHandshake(status);
            }

            return status;
        }
    }

    private static boolean canCompress(ProtobufWriteable message) {
        return message instanceof ProtobufBytesTransportRequest == false;
    }
}
