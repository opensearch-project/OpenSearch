/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.protobufs.IndexDocumentResponse;
import org.opensearch.protobufs.IndexDocumentResponseBody;
import org.opensearch.transport.grpc.proto.response.common.DocWriteResponseProtoUtils;

/**
 * Utility class for converting OpenSearch IndexResponse to protobuf IndexDocumentResponse.
 * <p>
 * This class provides functionality similar to the REST layer's index response serialization.
 * The response mapping and field serialization should be kept consistent with the corresponding
 * REST XContent implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.action.index.IndexResponse#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) REST equivalent for response serialization
 * @see org.opensearch.action.index.IndexResponse#fromXContent(org.opensearch.core.xcontent.XContentParser) REST equivalent for response parsing
 * @see org.opensearch.action.DocWriteResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) Base class XContent serialization
 * @see org.opensearch.action.index.IndexResponse OpenSearch internal index response representation
 * @see org.opensearch.protobufs.IndexDocumentResponse Protobuf definition for gRPC index responses
 */
public class IndexDocumentResponseProtoUtils {

    private IndexDocumentResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an OpenSearch IndexResponse to a protobuf IndexDocumentResponse.
     * <p>
     * This method serializes index response fields similar to how
     * {@link org.opensearch.action.index.IndexResponse#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params)}
     * serializes responses in REST. Field mapping includes index name, document ID, version,
     * sequence number, primary term, result status, forced refresh, and shard information.
     *
     * @param indexResponse The OpenSearch IndexResponse to convert
     * @return The corresponding protobuf IndexDocumentResponse
     * @see org.opensearch.action.index.IndexResponse#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) REST equivalent
     * @see org.opensearch.action.DocWriteResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) Base class serialization
     */
    public static IndexDocumentResponse toProto(IndexResponse indexResponse) {
        IndexDocumentResponseBody.Builder responseBodyBuilder = IndexDocumentResponseBody.newBuilder()
            .setXIndex(indexResponse.getIndex())
            .setXId(indexResponse.getId())
            .setXPrimaryTerm(indexResponse.getPrimaryTerm())
            .setXSeqNo(indexResponse.getSeqNo())
            .setXVersion(indexResponse.getVersion())
            .setResult(DocWriteResponseProtoUtils.resultToProto(indexResponse.getResult()));

        // Note: ShardInfo not available in IndexDocumentResponseBody protobuf definition

        return IndexDocumentResponse.newBuilder().setIndexDocumentResponseBody(responseBodyBuilder.build()).build();
    }

    /**
     * Creates an error response from an exception.
     * For now, we'll use gRPC status exceptions instead of structured error responses.
     *
     * @param exception The exception that occurred
     * @param statusCode The HTTP status code
     * @return The protobuf IndexDocumentResponse with error details
     */
    public static IndexDocumentResponse toErrorProto(Exception exception, RestStatus statusCode) {
        // For now, return a simple error response without the complex Error structure
        // This can be enhanced later with proper Error protobuf mapping
        throw new UnsupportedOperationException("Use GrpcErrorHandler.convertToGrpcError() instead");
    }
}
