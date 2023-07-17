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
import org.opensearch.common.bytes.CompositeBytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Outbound data as a message
*
* @opensearch.internal
*/
abstract class ProtobufOutboundMessage extends NetworkMessage {

    private final TransportMessage message;

    ProtobufOutboundMessage(ThreadContext threadContext, Version version, byte status, long requestId, TransportMessage message) {
        super(threadContext, version, status, requestId);
        this.message = message;
    }

    BytesReference serialize(BytesStreamOutput bytesStream) throws IOException {
        // TcpHeader.writeHeaderProtobuf(outputStream, requestId, status, version, TcpHeader.headerSize(version), TcpHeader.headerSize(version));
        // writeVariableHeader(outputStream);
        // message.writeTo(outputStream);
        // System.out.println("Inside ProtobufOutboundMessage.serialize");
        // CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(bytesStream);
        // ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(codedOutputStream);
        // protobufStreamOutput.setVersion(version);
        // // bytesStream.setVersion(version);
        // // bytesStream.skip(TcpHeader.headerSize(version));

        // // The compressible bytes stream will not close the underlying bytes stream
        // BytesReference reference;
        // int variableHeaderLength = -1;
        // final long preHeaderPosition = codedOutputStream.getTotalBytesWritten();
        // writeVariableHeader(codedOutputStream);
        // variableHeaderLength = Math.toIntExact(codedOutputStream.getTotalBytesWritten() - preHeaderPosition);

        // protobufStreamOutput.setFeatures(bytesStream.getFeatures());
        // if (variableHeaderLength == -1) {
        //         writeVariableHeader(codedOutputStream);
        // }
        // reference = writeMessage(codedOutputStream);
        // // try (CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bytesStream, TransportStatus.isCompress(status))) {
        // //     stream.setVersion(version);
        // //     stream.setFeatures(bytesStream.getFeatures());

        // //     if (variableHeaderLength == -1) {
        // //         writeVariableHeader(stream);
        // //     }
        // //     reference = writeMessage(stream);
        // // }

        // // bytesStream.seek(0);
        // final int contentSize = reference.length() - TcpHeader.headerSize(version);
        // TcpHeader.writeHeaderProtobuf(codedOutputStream, requestId, status, version, contentSize, variableHeaderLength);
        // System.out.println("Bytes serialized length: " + reference.length());
        // System.out.println("Bytes serialized: " + reference);
        // System.out.println("Now the bytes: ");
        // for (int i = 0; i < reference.length(); i++) {
        //     System.out.print(reference.get(i) + " ");
        // }
        // System.out.println();
        // return reference;

        
        bytesStream.setVersion(version);
        bytesStream.skip(TcpHeader.headerSize(version));

        // The compressible bytes stream will not close the underlying bytes stream
        BytesReference reference;
        int variableHeaderLength = -1;
        final long preHeaderPosition = bytesStream.position();
        writeVariableHeader(bytesStream);
        variableHeaderLength = Math.toIntExact(bytesStream.position() - preHeaderPosition);

        try (CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bytesStream, TransportStatus.isCompress(status))) {
            stream.setVersion(version);
            stream.setFeatures(bytesStream.getFeatures());

            if (variableHeaderLength == -1) {
                writeVariableHeader(stream);
            }
            reference = writeMessage(stream);
        }

        bytesStream.seek(0);
        final int contentSize = reference.length() - TcpHeader.headerSize(version);
        TcpHeader.writeHeader(bytesStream, requestId, status, version, contentSize, variableHeaderLength);
        System.out.println("Bytes serialized length: " + reference.length());
        System.out.println("Bytes serialized: " + reference);
        System.out.println("Now the bytes: ");
        for (int i = 0; i < reference.length(); i++) {
            System.out.print(reference.get(i) + " ");
        }
        System.out.println();
        // convert output stream to bytes
        // ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // baos.write(outputStream);
        // outputStream.writeTo(baos);
        // byte[] bytes = baos.toByteArray();
        // System.out.println("Bytes serialized length: " + bytes.length);
        // System.out.println("Bytes serialized: " + bytes);
        // System.out.println("Now the bytes: ");

        return reference;
    }

    protected void writeVariableHeader(StreamOutput stream) throws IOException {
        threadContext.writeTo(stream);
    }

    protected BytesReference writeMessage(CompressibleBytesOutputStream stream) throws IOException {
        // final BytesReference zeroCopyBuffer;
        // if (message instanceof BytesTransportRequest) {
        //     BytesTransportRequest bRequest = (BytesTransportRequest) message;
        //     bRequest.writeThinProtobuf(stream);
        //     zeroCopyBuffer = bRequest.bytes;
        // } else if (message instanceof ProtobufRemoteTransportException) {
        //     ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(stream);
        //     protobufStreamOutput.writeException((ProtobufRemoteTransportException) message);
        //     zeroCopyBuffer = BytesArray.EMPTY;
        // } else {
        //     message.writeTo(stream);
        //     zeroCopyBuffer = BytesArray.EMPTY;
        // }
        // // we have to call materializeBytes() here before accessing the bytes. A CompressibleBytesOutputStream
        // // might be implementing compression. And materializeBytes() ensures that some marker bytes (EOS marker)
        // // are written. Otherwise we barf on the decompressing end when we read past EOF on purpose in the
        // // #validateRequest method. this might be a problem in deflate after all but it's important to write
        // // the marker bytes.
        // ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        // final BytesReference message = stream
        // if (zeroCopyBuffer.length() == 0) {
        //     return message;
        // } else {
        //     return CompositeBytesReference.of(message, zeroCopyBuffer);
        // }
        final BytesReference zeroCopyBuffer;
        if (message instanceof BytesTransportRequest) {
            BytesTransportRequest bRequest = (BytesTransportRequest) message;
            bRequest.writeThin(stream);
            zeroCopyBuffer = bRequest.bytes;
        // } else if (message instanceof RemoteTransportException) {
        //     stream.writeException((RemoteTransportException) message);
        //     zeroCopyBuffer = BytesArray.EMPTY;
        } else {
            // CodedOutputStream outputStream = CodedOutputStream.newInstance(stream);
            message.writeTo(stream);
            zeroCopyBuffer = BytesArray.EMPTY;
        }
        // we have to call materializeBytes() here before accessing the bytes. A CompressibleBytesOutputStream
        // might be implementing compression. And materializeBytes() ensures that some marker bytes (EOS marker)
        // are written. Otherwise we barf on the decompressing end when we read past EOF on purpose in the
        // #validateRequest method. this might be a problem in deflate after all but it's important to write
        // the marker bytes.
        final BytesReference message = stream.materializeBytes();
        if (zeroCopyBuffer.length() == 0) {
            return message;
        } else {
            return CompositeBytesReference.of(message, zeroCopyBuffer);
        }
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
            TransportMessage message,
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
        protected void writeVariableHeader(StreamOutput stream) throws IOException {
            // super.writeVariableHeader(out);
            // ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            // protobufStreamOutput.writeStringArray(features);
            // out.writeStringNoTag(action);
            super.writeVariableHeader(stream);
            stream.writeStringArray(features);
            stream.writeString(action);
        }

        private static byte setStatus(boolean compress, boolean isHandshake, TransportMessage message) {
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
            TransportMessage message,
            Version version,
            long requestId,
            boolean isHandshake,
            boolean compress
        ) {
            super(threadContext, version, setStatus(compress, isHandshake, message), requestId, message);
            this.features = features;
        }

        @Override
        protected void writeVariableHeader(StreamOutput stream) throws IOException {
            // super.writeVariableHeader(out);
            // ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
            // protobufStreamOutput.setFeatures(features);
            super.writeVariableHeader(stream);
            stream.setFeatures(features);
        }

        private static byte setStatus(boolean compress, boolean isHandshake, TransportMessage message) {
            byte status = 0;
            status = TransportStatus.setResponse(status);
            // if (message instanceof RemoteTransportException) {
            //     status = TransportStatus.setError(status);
            // }
            if (compress) {
                status = TransportStatus.setCompress(status);
            }
            if (isHandshake) {
                status = TransportStatus.setHandshake(status);
            }

            return status;
        }
    }

    private static boolean canCompress(TransportMessage message) {
        return message instanceof ProtobufBytesTransportRequest == false;
    }
}
