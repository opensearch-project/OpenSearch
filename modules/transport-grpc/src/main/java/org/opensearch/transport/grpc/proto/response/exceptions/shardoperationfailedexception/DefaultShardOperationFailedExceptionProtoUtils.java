/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.GlobalParams;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.transport.grpc.proto.response.exceptions.opensearchexception.OpenSearchExceptionProtoUtils;

import java.io.IOException;

/**
 * Utility class for converting DefaultShardOperationFailedException objects to Protocol Buffers.
 */
public class DefaultShardOperationFailedExceptionProtoUtils {

    private DefaultShardOperationFailedExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts the metadata from a DefaultShardOperationFailedException to a Protocol Buffer Struct.
     * Similar to {@link DefaultShardOperationFailedException#toXContent(XContentBuilder, ToXContent.Params)}     *
     * This method is overridden by various exception classes, which are hardcoded here.
     *
     * @param exception The DefaultShardOperationFailedException to convert
     * @param params The global gRPC request parameters
     * @return A Protocol Buffer Struct containing the exception metadata
     */
    public static ShardFailure toProto(DefaultShardOperationFailedException exception, GlobalParams params) throws IOException {
        ShardFailure.Builder shardFailureBuilder = ShardFailure.newBuilder();

        switch (exception) {
            case AddIndexBlockResponse.AddBlockShardResult.Failure addBlockFailure -> innerToProto(
                shardFailureBuilder,
                addBlockFailure,
                params
            );
            case IndicesShardStoresResponse.Failure indicesShardStoresFailure -> innerToProto(
                shardFailureBuilder,
                indicesShardStoresFailure,
                params
            );
            case CloseIndexResponse.ShardResult.Failure closeIndexFailure -> innerToProto(shardFailureBuilder, closeIndexFailure, params);
            default -> parentInnerToProto(shardFailureBuilder, exception, params);
        }
        return shardFailureBuilder.build();
    }

    /**
     * Converts the metadata from a AddIndexBlockResponse.AddBlockShardResult.Failure to a Protocol Buffer Struct.
     * Similar to {@link AddIndexBlockResponse.AddBlockShardResult.Failure#innerToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param shardFailureBuilder the builder to populate with failure information
     * @param exception The AddIndexBlockResponse.AddBlockShardResult.Failure to convert
     * @param params The global gRPC request parameters
     * @throws IOException if there's an error during conversion
     */
    public static void innerToProto(
        ShardFailure.Builder shardFailureBuilder,
        AddIndexBlockResponse.AddBlockShardResult.Failure exception,
        GlobalParams params
    ) throws IOException {
        if (exception.getNodeId() != null) {
            shardFailureBuilder.setNode(exception.getNodeId());
        }
        parentInnerToProto(shardFailureBuilder, exception, params);
    }

    /**
     * Converts the metadata from a IndicesShardStoresResponse.Failure to a Protocol Buffer Struct.
     * Similar to {@link IndicesShardStoresResponse.Failure#innerToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param shardFailureBuilder the builder to populate with failure information
     * @param exception The IndicesShardStoresResponse.Failure to convert
     * @param params The global gRPC request parameters
     * @throws IOException if there's an error during conversion
     */
    public static void innerToProto(
        ShardFailure.Builder shardFailureBuilder,
        IndicesShardStoresResponse.Failure exception,
        GlobalParams params
    ) throws IOException {
        shardFailureBuilder.setNode(exception.nodeId());
        parentInnerToProto(shardFailureBuilder, exception, params);
    }

    /**
     * Converts the metadata from a CloseIndexResponse.ShardResult.Failure to a Protocol Buffer Struct.
     * Similar to {@link CloseIndexResponse.ShardResult.Failure#innerToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param shardFailureBuilder the builder to populate with failure information
     * @param exception The CloseIndexResponse.ShardResult.Failure to convert
     * @param params The global gRPC request parameters
     * @throws IOException if there's an error during conversion
     */
    public static void innerToProto(
        ShardFailure.Builder shardFailureBuilder,
        CloseIndexResponse.ShardResult.Failure exception,
        GlobalParams params
    ) throws IOException {
        if (exception.getNodeId() != null) {
            shardFailureBuilder.setNode(exception.getNodeId());
        }
        parentInnerToProto(shardFailureBuilder, exception, params);
    }

    /**
     * Converts the metadata from a DefaultShardOperationFailedException to a Protocol Buffer Struct.
     * Similar to {@link DefaultShardOperationFailedException#innerToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param shardFailureBuilder the builder to populate with failure information
     * @param exception The DefaultShardOperationFailedException to convert
     * @param params The global gRPC request parameters
     * @throws IOException if there's an error during conversion
     */
    public static void parentInnerToProto(
        ShardFailure.Builder shardFailureBuilder,
        DefaultShardOperationFailedException exception,
        GlobalParams params
    ) throws IOException {
        shardFailureBuilder.setShard(exception.shardId());
        if (exception.index() != null) {
            shardFailureBuilder.setIndex(exception.index());
        }
        shardFailureBuilder.setStatus(exception.status().name());
        if (exception.reason() != null) {
            shardFailureBuilder.setReason(OpenSearchExceptionProtoUtils.generateThrowableProto(exception.getCause(), params));
        }
    }
}
