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
import org.opensearch.server.proto.ClusterStateRequestProto.ClusterStateReq;
import org.opensearch.server.proto.ClusterStateResponseProto.ClusterStateRes;
import org.opensearch.server.proto.NodesInfoProto.NodesInfo;
import org.opensearch.server.proto.NodesInfoRequestProto.NodesInfoReq;
import org.opensearch.server.proto.NodesStatsProto.NodesStats;
import org.opensearch.server.proto.NodesStatsRequestProto.NodesStatsReq;
import org.opensearch.server.proto.OutboundMessageProto.OutboundMsg;
import org.opensearch.server.proto.OutboundMessageProto.OutboundMsg.Header;
import org.opensearch.server.proto.OutboundMessageProto.OutboundMsg.ResponseHandlersList;
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

    public ProtobufOutboundMessage(long requestId, byte[] status, Version version, ThreadContext threadContext, ClusterStateReq clusterStateReq, String[] features, String action) {
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
                            .setClusterStateReq(clusterStateReq)
                            .setAction(action)
                            .addAllFeatures(Arrays.asList(features))
                            .setIsProtobuf(true)
                            .build();
                            
    }

    public ProtobufOutboundMessage(long requestId, byte[] status, Version version, ThreadContext threadContext, ClusterStateRes clusterStateRes, Set<String> features, String action) {
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
                            .setClusterStateRes(clusterStateRes)
                            .setAction(action)
                            .addAllFeatures(features)
                            .setIsProtobuf(true)
                            .build();
                            
    }

    public ProtobufOutboundMessage(long requestId, byte[] status, Version version, ThreadContext threadContext, NodesInfoReq nodesInfoReq, String[] features, String action) {
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
                            .setNodesInfoReq(nodesInfoReq)
                            .setAction(action)
                            .addAllFeatures(Arrays.asList(features))
                            .setIsProtobuf(true)
                            .build();
                            
    }

    public ProtobufOutboundMessage(long requestId, byte[] status, Version version, ThreadContext threadContext, NodesInfo nodesInfoRes, Set<String> features, String action) {
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
                            .setNodesInfoRes(nodesInfoRes)
                            .setAction(action)
                            .addAllFeatures(features)
                            .setIsProtobuf(true)
                            .build();
                            
    }

    public ProtobufOutboundMessage(long requestId, byte[] status, Version version, ThreadContext threadContext, NodesStatsReq nodesStatsReq, String[] features, String action) {
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
                            .setNodesStatsReq(nodesStatsReq)
                            .setAction(action)
                            .addAllFeatures(Arrays.asList(features))
                            .setIsProtobuf(true)
                            .build();
                            
    }

    public ProtobufOutboundMessage(long requestId, byte[] status, Version version, ThreadContext threadContext, NodesStats nodesStatsRes, Set<String> features, String action) {
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
                            .setNodesStatsRes(nodesStatsRes)
                            .setAction(action)
                            .addAllFeatures(features)
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
}
