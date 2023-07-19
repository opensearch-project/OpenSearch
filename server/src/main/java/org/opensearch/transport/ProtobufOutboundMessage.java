/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.opensearch.Version;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.example.proto.ExampleRequestProto.ExampleRequest;
import org.opensearch.example.proto.OutboundMessageProto.OutboundMsg;
import org.opensearch.example.proto.OutboundMessageProto.OutboundMsg.Header;
import org.opensearch.example.proto.OutboundMessageProto.OutboundMsg.ResponseHandlersList;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Outbound data as a message
*
* @opensearch.internal
*/
public class ProtobufOutboundMessage {

    private final OutboundMsg message;
    private static final byte[] PREFIX = { (byte) 'E', (byte) 'S' };

    public ProtobufOutboundMessage(long requestId, byte[] status, Version version, ThreadContext threadContext, ExampleRequest exampleRequest, String[] features, String action) {
        Header header = Header.newBuilder()
                            .addAllPrefix(Arrays.asList(ByteString.copyFrom(PREFIX)))
                            .setRequestId(requestId)
                            .setStatus(ByteString.copyFrom(status))
                            .setVersionId(version.id)
                            .build();
        Map<String, String> requestHeaders = threadContext.getHeaders();
        Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        Map<String, ResponseHandlersList> responseHandlers = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : responseHeaders.entrySet()) {
            String key = entry.getKey();
            List<String> value = entry.getValue();
            ResponseHandlersList responseHandlersList = ResponseHandlersList.newBuilder()
                                                                        .addAllSetOfResponseHandlers(value)
                                                                        .build();
            responseHandlers.put(key, responseHandlersList);
        }
        this.message = OutboundMsg.newBuilder()
                            .setHeader(header)
                            .putAllRequestHeaders(requestHeaders)
                            .putAllResponseHandlers(responseHandlers)
                            .setVersion(version.toString())
                            .setStatus(ByteString.copyFrom(status))
                            .setRequestId(requestId)
                            .setMessage(exampleRequest)
                            .setAction(action)
                            .addAllFeatures(Arrays.asList(features))
                            .setIsProtobuf(true)
                            .build();
                            
    }

    public ProtobufOutboundMessage(byte[] data) throws InvalidProtocolBufferException {
        this.message = OutboundMsg.parseFrom(data);
    }

    public void writeTo(OutputStream out) throws IOException {
        // super.writeTo(out);
        out.write(this.message.toByteArray());
    }

    public OutboundMsg getMessage() {
        return this.message;
    }

    @Override
    public String toString() {
        return "ProtobufOutboundMessage [message=" + message + "]";
    }

    public Header getHeader() {
        return this.message.getHeader();
    }

    public Map<String, String> getRequestHeaders() {
        return this.message.getRequestHeadersMap();
    }

    public Map<String, Set<String>> getResponseHandlers() {
        Map<String, ResponseHandlersList> responseHandlers = this.message.getResponseHandlersMap();
        Map<String, Set<String>> responseHandlersMap = new HashMap<>();
        for (Map.Entry<String, ResponseHandlersList> entry : responseHandlers.entrySet()) {
            String key = entry.getKey();
            ResponseHandlersList value = entry.getValue();
            Set<String> setOfResponseHandlers = value.getSetOfResponseHandlersList().stream().collect(Collectors.toSet());
            responseHandlersMap.put(key, setOfResponseHandlers);
        }
        return responseHandlersMap;
    }

    public boolean isProtobuf() {
        return this.message.getIsProtobuf();
    }


    // private final TransportMessage message;

    // ProtobufOutboundMessage(ThreadContext threadContext, Version version, byte status, long requestId, TransportMessage message) {
    //     super(threadContext, version, status, requestId);
    //     this.message = message;
    // }

    // BytesReference serialize(BytesStreamOutput bytesStream) throws IOException {
    //     // TcpHeader.writeHeaderProtobuf(outputStream, requestId, status, version, TcpHeader.headerSize(version), TcpHeader.headerSize(version));
    //     // writeVariableHeader(outputStream);
    //     // message.writeTo(outputStream);
    //     // System.out.println("Inside ProtobufOutboundMessage.serialize");
    //     // CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(bytesStream);
    //     // ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(codedOutputStream);
    //     // protobufStreamOutput.setVersion(version);
    //     // // bytesStream.setVersion(version);
    //     // // bytesStream.skip(TcpHeader.headerSize(version));

    //     // // The compressible bytes stream will not close the underlying bytes stream
    //     // BytesReference reference;
    //     // int variableHeaderLength = -1;
    //     // final long preHeaderPosition = codedOutputStream.getTotalBytesWritten();
    //     // writeVariableHeader(codedOutputStream);
    //     // variableHeaderLength = Math.toIntExact(codedOutputStream.getTotalBytesWritten() - preHeaderPosition);

    //     // protobufStreamOutput.setFeatures(bytesStream.getFeatures());
    //     // if (variableHeaderLength == -1) {
    //     //         writeVariableHeader(codedOutputStream);
    //     // }
    //     // reference = writeMessage(codedOutputStream);
    //     // // try (CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bytesStream, TransportStatus.isCompress(status))) {
    //     // //     stream.setVersion(version);
    //     // //     stream.setFeatures(bytesStream.getFeatures());

    //     // //     if (variableHeaderLength == -1) {
    //     // //         writeVariableHeader(stream);
    //     // //     }
    //     // //     reference = writeMessage(stream);
    //     // // }

    //     // // bytesStream.seek(0);
    //     // final int contentSize = reference.length() - TcpHeader.headerSize(version);
    //     // TcpHeader.writeHeaderProtobuf(codedOutputStream, requestId, status, version, contentSize, variableHeaderLength);
    //     // System.out.println("Bytes serialized length: " + reference.length());
    //     // System.out.println("Bytes serialized: " + reference);
    //     // System.out.println("Now the bytes: ");
    //     // for (int i = 0; i < reference.length(); i++) {
    //     //     System.out.print(reference.get(i) + " ");
    //     // }
    //     // System.out.println();
    //     // return reference;

        
    //     bytesStream.setVersion(version);
    //     bytesStream.skip(TcpHeader.headerSize(version));

    //     // The compressible bytes stream will not close the underlying bytes stream
    //     BytesReference reference;
    //     int variableHeaderLength = -1;
    //     final long preHeaderPosition = bytesStream.position();
    //     writeVariableHeader(bytesStream);
    //     variableHeaderLength = Math.toIntExact(bytesStream.position() - preHeaderPosition);

    //     try (CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bytesStream, TransportStatus.isCompress(status))) {
    //         stream.setVersion(version);
    //         stream.setFeatures(bytesStream.getFeatures());

    //         if (variableHeaderLength == -1) {
    //             writeVariableHeader(stream);
    //         }
    //         reference = writeMessage(stream);
    //     }

    //     bytesStream.seek(0);
    //     final int contentSize = reference.length() - TcpHeader.headerSize(version);
    //     TcpHeader.writeHeader(bytesStream, requestId, status, version, contentSize, variableHeaderLength);
    //     System.out.println("Bytes serialized length: " + reference.length());
    //     System.out.println("Bytes serialized: " + reference);
    //     System.out.println("Now the bytes: ");
    //     for (int i = 0; i < reference.length(); i++) {
    //         System.out.print(reference.get(i) + " ");
    //     }
    //     System.out.println();
    //     // convert output stream to bytes
    //     // ByteArrayOutputStream baos = new ByteArrayOutputStream();
    //     // baos.write(outputStream);
    //     // outputStream.writeTo(baos);
    //     // byte[] bytes = baos.toByteArray();
    //     // System.out.println("Bytes serialized length: " + bytes.length);
    //     // System.out.println("Bytes serialized: " + bytes);
    //     // System.out.println("Now the bytes: ");

    //     return reference;
    // }

    // protected void writeVariableHeader(StreamOutput stream) throws IOException {
    //     threadContext.writeTo(stream);
    // }

    // protected BytesReference writeMessage(CompressibleBytesOutputStream stream) throws IOException {
    //     // final BytesReference zeroCopyBuffer;
    //     // if (message instanceof BytesTransportRequest) {
    //     //     BytesTransportRequest bRequest = (BytesTransportRequest) message;
    //     //     bRequest.writeThinProtobuf(stream);
    //     //     zeroCopyBuffer = bRequest.bytes;
    //     // } else if (message instanceof ProtobufRemoteTransportException) {
    //     //     ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(stream);
    //     //     protobufStreamOutput.writeException((ProtobufRemoteTransportException) message);
    //     //     zeroCopyBuffer = BytesArray.EMPTY;
    //     // } else {
    //     //     message.writeTo(stream);
    //     //     zeroCopyBuffer = BytesArray.EMPTY;
    //     // }
    //     // // we have to call materializeBytes() here before accessing the bytes. A CompressibleBytesOutputStream
    //     // // might be implementing compression. And materializeBytes() ensures that some marker bytes (EOS marker)
    //     // // are written. Otherwise we barf on the decompressing end when we read past EOF on purpose in the
    //     // // #validateRequest method. this might be a problem in deflate after all but it's important to write
    //     // // the marker bytes.
    //     // ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    //     // final BytesReference message = stream
    //     // if (zeroCopyBuffer.length() == 0) {
    //     //     return message;
    //     // } else {
    //     //     return CompositeBytesReference.of(message, zeroCopyBuffer);
    //     // }
    //     final BytesReference zeroCopyBuffer;
    //     if (message instanceof BytesTransportRequest) {
    //         BytesTransportRequest bRequest = (BytesTransportRequest) message;
    //         bRequest.writeThin(stream);
    //         zeroCopyBuffer = bRequest.bytes;
    //     // } else if (message instanceof RemoteTransportException) {
    //     //     stream.writeException((RemoteTransportException) message);
    //     //     zeroCopyBuffer = BytesArray.EMPTY;
    //     } else {
    //         // CodedOutputStream outputStream = CodedOutputStream.newInstance(stream);
    //         message.writeTo(stream);
    //         zeroCopyBuffer = BytesArray.EMPTY;
    //     }
    //     // we have to call materializeBytes() here before accessing the bytes. A CompressibleBytesOutputStream
    //     // might be implementing compression. And materializeBytes() ensures that some marker bytes (EOS marker)
    //     // are written. Otherwise we barf on the decompressing end when we read past EOF on purpose in the
    //     // #validateRequest method. this might be a problem in deflate after all but it's important to write
    //     // the marker bytes.
    //     final BytesReference message = stream.materializeBytes();
    //     if (zeroCopyBuffer.length() == 0) {
    //         return message;
    //     } else {
    //         return CompositeBytesReference.of(message, zeroCopyBuffer);
    //     }
    // }

    // /**
    //  * Internal outbound message request
    // *
    // * @opensearch.internal
    // */
    // static class Request extends ProtobufOutboundMessage {

    //     private final String[] features;
    //     private final String action;

    //     Request(
    //         ThreadContext threadContext,
    //         String[] features,
    //         TransportMessage message,
    //         Version version,
    //         String action,
    //         long requestId,
    //         boolean isHandshake,
    //         boolean compress
    //     ) {
    //         super(threadContext, version, setStatus(compress, isHandshake, message), requestId, message);
    //         this.features = features;
    //         this.action = action;
    //     }

    //     @Override
    //     protected void writeVariableHeader(StreamOutput stream) throws IOException {
    //         // super.writeVariableHeader(out);
    //         // ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
    //         // protobufStreamOutput.writeStringArray(features);
    //         // out.writeStringNoTag(action);
    //         super.writeVariableHeader(stream);
    //         stream.writeStringArray(features);
    //         stream.writeString(action);
    //     }

    //     private static byte setStatus(boolean compress, boolean isHandshake, TransportMessage message) {
    //         byte status = 0;
    //         status = TransportStatus.setRequest(status);
    //         if (compress && ProtobufOutboundMessage.canCompress(message)) {
    //             status = TransportStatus.setCompress(status);
    //         }
    //         if (isHandshake) {
    //             status = TransportStatus.setHandshake(status);
    //         }

    //         return status;
    //     }
    // }

    // /**
    //  * Internal message response
    // *
    // * @opensearch.internal
    // */
    // static class Response extends ProtobufOutboundMessage {

    //     private final Set<String> features;

    //     Response(
    //         ThreadContext threadContext,
    //         Set<String> features,
    //         TransportMessage message,
    //         Version version,
    //         long requestId,
    //         boolean isHandshake,
    //         boolean compress
    //     ) {
    //         super(threadContext, version, setStatus(compress, isHandshake, message), requestId, message);
    //         this.features = features;
    //     }

    //     @Override
    //     protected void writeVariableHeader(StreamOutput stream) throws IOException {
    //         // super.writeVariableHeader(out);
    //         // ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
    //         // protobufStreamOutput.setFeatures(features);
    //         super.writeVariableHeader(stream);
    //         stream.setFeatures(features);
    //     }

    //     private static byte setStatus(boolean compress, boolean isHandshake, TransportMessage message) {
    //         byte status = 0;
    //         status = TransportStatus.setResponse(status);
    //         // if (message instanceof RemoteTransportException) {
    //         //     status = TransportStatus.setError(status);
    //         // }
    //         if (compress) {
    //             status = TransportStatus.setCompress(status);
    //         }
    //         if (isHandshake) {
    //             status = TransportStatus.setHandshake(status);
    //         }

    //         return status;
    //     }
    // }

    // private static boolean canCompress(TransportMessage message) {
    //     return message instanceof ProtobufBytesTransportRequest == false;
    // }
}
