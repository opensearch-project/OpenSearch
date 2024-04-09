/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport.protobufprotocol;

import com.google.protobuf.ByteString;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.server.proto.NodeToNodeMessageProto;
import org.opensearch.server.proto.NodeToNodeMessageProto.NodeToNodeMessage.Header;
import org.opensearch.server.proto.NodeToNodeMessageProto.NodeToNodeMessage.ResponseHandlersList;
import org.opensearch.server.proto.QueryFetchSearchResultProto.QueryFetchSearchResult;
import org.opensearch.server.proto.QuerySearchResultProto.QuerySearchResult;
import org.opensearch.transport.ProtocolInboundMessage;
import org.opensearch.transport.TcpHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Outbound data as a message
*
* @opensearch.internal
*/
public class ProtobufInboundMessage implements ProtocolInboundMessage {

    /**
     * The protocol used to encode this message
     */
    public static String PROTOBUF_PROTOCOL = "protobuf";

    private final NodeToNodeMessageProto.NodeToNodeMessage message;
    private static final byte[] PREFIX = { (byte) 'E', (byte) 'S' };

    public ProtobufInboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        QueryFetchSearchResult queryFetchSearchResult,
        Set<String> features,
        String action
    ) {
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
            ResponseHandlersList responseHandlersList = ResponseHandlersList.newBuilder().addAllSetOfResponseHandlers(value).build();
            responseHandlers.put(key, responseHandlersList);
        }
        this.message = NodeToNodeMessageProto.NodeToNodeMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setQueryFetchSearchResult(queryFetchSearchResult)
            .setAction(action)
            .addAllFeatures(features)
            .build();
    }

    public ProtobufInboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        QuerySearchResult querySearchResult,
        Set<String> features,
        String action
    ) {
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
            ResponseHandlersList responseHandlersList = ResponseHandlersList.newBuilder().addAllSetOfResponseHandlers(value).build();
            responseHandlers.put(key, responseHandlersList);
        }
        this.message = NodeToNodeMessageProto.NodeToNodeMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setQuerySearchResult(querySearchResult)
            .setAction(action)
            .addAllFeatures(features)
            .build();
    }

    public ProtobufInboundMessage(InputStream in) throws IOException {
        this.message = NodeToNodeMessageProto.NodeToNodeMessage.parseFrom(in);
    }

    public void writeTo(OutputStream out) throws IOException {
        this.message.writeTo(out);
    }

    public BytesReference serialize(BytesStreamOutput bytesStream) throws IOException {
        NodeToNodeMessageProto.NodeToNodeMessage message = getMessage();
        TcpHeader.writeHeaderForProtobuf(bytesStream);
        message.writeTo(bytesStream);
        return bytesStream.bytes();
    }

    public NodeToNodeMessageProto.NodeToNodeMessage getMessage() {
        return this.message;
    }

    @Override
    public String toString() {
        return "ProtobufInboundMessage [message=" + message + "]";
    }

    public org.opensearch.server.proto.NodeToNodeMessageProto.NodeToNodeMessage.Header getHeader() {
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

    @Override
    public String getProtocol() {
        return PROTOBUF_PROTOCOL;
    }

}
