/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document;

import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.protobufs.DeleteDocumentResponse;
import org.opensearch.protobufs.DeleteDocumentResponseBody;
import org.opensearch.transport.grpc.proto.response.common.DocWriteResponseProtoUtils;

/**
 * Utility class for converting OpenSearch DeleteResponse to protobuf DeleteDocumentResponse.
 * <p>
 * This class provides functionality similar to the REST layer's delete response serialization.
 * The response mapping and field serialization should be kept consistent with the corresponding
 * REST XContent implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.action.delete.DeleteResponse#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) REST equivalent for response serialization
 * @see org.opensearch.action.delete.DeleteResponse#fromXContent(org.opensearch.core.xcontent.XContentParser) REST equivalent for response parsing
 * @see org.opensearch.action.DocWriteResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) Base class XContent serialization
 * @see org.opensearch.action.delete.DeleteResponse OpenSearch internal delete response representation
 * @see org.opensearch.protobufs.DeleteDocumentResponse Protobuf definition for gRPC delete responses
 */
public class DeleteDocumentResponseProtoUtils {

    private DeleteDocumentResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an OpenSearch DeleteResponse to a protobuf DeleteDocumentResponse.
     * <p>
     * This method serializes delete response fields similar to how
     * {@link org.opensearch.action.DocWriteResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params)}
     * serializes responses in REST. Field mapping includes index name, document ID, version,
     * sequence number, primary term, result status, forced refresh, and shard information.
     *
     * @param deleteResponse The OpenSearch DeleteResponse to convert
     * @return The corresponding protobuf DeleteDocumentResponse
     * @see org.opensearch.action.DocWriteResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) REST equivalent
     */
    public static DeleteDocumentResponse toProto(DeleteResponse deleteResponse) {
        DeleteDocumentResponseBody.Builder responseBodyBuilder = DeleteDocumentResponseBody.newBuilder()
            .setXIndex(deleteResponse.getIndex())
            .setXId(deleteResponse.getId())
            .setXPrimaryTerm(deleteResponse.getPrimaryTerm())
            .setXSeqNo(deleteResponse.getSeqNo())
            .setXVersion(deleteResponse.getVersion())
            .setResult(DocWriteResponseProtoUtils.resultToProto(deleteResponse.getResult()));

        // Note: ShardInfo not available in DeleteDocumentResponseBody protobuf definition

        return DeleteDocumentResponse.newBuilder().setDeleteDocumentResponseBody(responseBodyBuilder.build()).build();
    }

    /**
     * Creates an error response from an exception.
     *
     * @param exception The exception that occurred
     * @param statusCode The HTTP status code
     * @return The protobuf DeleteDocumentResponse with error details
     */
    public static DeleteDocumentResponse toErrorProto(Exception exception, RestStatus statusCode) {
        throw new UnsupportedOperationException("Use GrpcErrorHandler.convertToGrpcError() instead");
    }
}
