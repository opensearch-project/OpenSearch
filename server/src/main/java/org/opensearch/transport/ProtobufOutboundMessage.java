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
import org.opensearch.server.proto.ClusterStateRequestProto.ClusterStateRequest;
import org.opensearch.server.proto.ClusterStateResponseProto.ClusterStateResponse;
import org.opensearch.server.proto.NodesInfoProto.NodesInfo;
import org.opensearch.server.proto.NodesInfoRequestProto.NodesInfoRequest;
import org.opensearch.server.proto.NodesStatsProto.NodesStats;
import org.opensearch.server.proto.NodesStatsRequestProto.NodesStatsRequest;
import org.opensearch.server.proto.QueryFetchSearchResultProto.QueryFetchSearchResult;
import org.opensearch.server.proto.ShardSearchRequestProto.ShardSearchRequest;
import org.opensearch.server.proto.MessageProto.OutboundInboundMessage;
import org.opensearch.server.proto.MessageProto.OutboundInboundMessage.Header;
import org.opensearch.server.proto.MessageProto.OutboundInboundMessage.ResponseHandlersList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Outbound data as a message
*
* @opensearch.internal
*/
public class ProtobufOutboundMessage {

    private final OutboundInboundMessage message;
    private static final byte[] PREFIX = { (byte) 'E', (byte) 'S' };

    public ProtobufOutboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        ClusterStateRequest clusterStateReq,
        String[] features,
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
        this.message = OutboundInboundMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setClusterStateRequest(clusterStateReq)
            .setAction(action)
            .addAllFeatures(Arrays.asList(features))
            .setIsProtobuf(true)
            .build();

    }

    public ProtobufOutboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        ClusterStateResponse clusterStateRes,
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
        this.message = OutboundInboundMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setClusterStateResponse(clusterStateRes)
            .setAction(action)
            .addAllFeatures(features)
            .setIsProtobuf(true)
            .build();

    }

    public ProtobufOutboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        NodesInfoRequest nodesInfoReq,
        String[] features,
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
        this.message = OutboundInboundMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setNodesInfoRequest(nodesInfoReq)
            .setAction(action)
            .addAllFeatures(Arrays.asList(features))
            .setIsProtobuf(true)
            .build();

    }

    public ProtobufOutboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        NodesInfo nodesInfoRes,
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
        this.message = OutboundInboundMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setNodesInfoResponse(nodesInfoRes)
            .setAction(action)
            .addAllFeatures(features)
            .setIsProtobuf(true)
            .build();

    }

    public ProtobufOutboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        NodesStatsRequest nodesStatsReq,
        String[] features,
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
        this.message = OutboundInboundMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setNodesStatsRequest(nodesStatsReq)
            .setAction(action)
            .addAllFeatures(Arrays.asList(features))
            .setIsProtobuf(true)
            .build();

    }

    public ProtobufOutboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        NodesStats nodesStatsRes,
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
        this.message = OutboundInboundMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setNodesStatsResponse(nodesStatsRes)
            .setAction(action)
            .addAllFeatures(features)
            .setIsProtobuf(true)
            .build();

    }

     public ProtobufOutboundMessage(
        long requestId,
        byte[] status,
        Version version,
        ThreadContext threadContext,
        ShardSearchRequest shardSearchReq,
        String[] features,
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
        this.message = OutboundInboundMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setShardSearchRequest(shardSearchReq)
            .setAction(action)
            .addAllFeatures(Arrays.asList(features))
            .setIsProtobuf(true)
            .build();

    }

    public ProtobufOutboundMessage(
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
        this.message = OutboundInboundMessage.newBuilder()
            .setHeader(header)
            .putAllRequestHeaders(requestHeaders)
            .putAllResponseHandlers(responseHandlers)
            .setVersion(version.toString())
            .setStatus(ByteString.copyFrom(status))
            .setRequestId(requestId)
            .setQueryFetchSearchResult(queryFetchSearchResult)
            .setAction(action)
            .addAllFeatures(features)
            .setIsProtobuf(true)
            .build();

    }

    public ProtobufOutboundMessage(byte[] data) throws InvalidProtocolBufferException {
        this.message = OutboundInboundMessage.parseFrom(data);
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(this.message.toByteArray());
    }

    public OutboundInboundMessage getMessage() {
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
