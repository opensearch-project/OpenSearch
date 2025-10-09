/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document;

import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.protobufs.UpdateDocumentResponse;
import org.opensearch.protobufs.UpdateDocumentResponseBody;
import org.opensearch.transport.grpc.proto.response.common.DocWriteResponseProtoUtils;

/**
 * Utility class for converting OpenSearch UpdateResponse to protobuf UpdateDocumentResponse.
 * <p>
 * This class provides functionality similar to the REST layer's update response serialization.
 * The response mapping and field serialization should be kept consistent with the corresponding
 * REST XContent implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.action.update.UpdateResponse#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) REST equivalent for response serialization
 * @see org.opensearch.action.update.UpdateResponse#fromXContent(org.opensearch.core.xcontent.XContentParser) REST equivalent for response parsing
 * @see org.opensearch.action.update.UpdateResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) Update-specific XContent serialization
 * @see org.opensearch.action.update.UpdateResponse OpenSearch internal update response representation
 * @see org.opensearch.protobufs.UpdateDocumentResponse Protobuf definition for gRPC update responses
 */
public class UpdateDocumentResponseProtoUtils {

    private UpdateDocumentResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an OpenSearch UpdateResponse to a protobuf UpdateDocumentResponse.
     * <p>
     * This method serializes update response fields similar to how
     * {@link org.opensearch.action.update.UpdateResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params)}
     * serializes responses in REST. Field mapping includes index name, document ID, version,
     * sequence number, primary term, result status, forced refresh, shard information,
     * and optional get result for updated documents.
     *
     * @param updateResponse The OpenSearch UpdateResponse to convert
     * @return The corresponding protobuf UpdateDocumentResponse
     * @see org.opensearch.action.update.UpdateResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) REST equivalent
     * @see org.opensearch.action.DocWriteResponse#innerToXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) Base class serialization
     */
    public static UpdateDocumentResponse toProto(UpdateResponse updateResponse) {
        UpdateDocumentResponseBody.Builder responseBodyBuilder = UpdateDocumentResponseBody.newBuilder()
            .setXIndex(updateResponse.getIndex())
            .setXId(updateResponse.getId())
            .setXPrimaryTerm(updateResponse.getPrimaryTerm())
            .setXSeqNo(updateResponse.getSeqNo())
            .setXVersion(updateResponse.getVersion())
            .setResult(DocWriteResponseProtoUtils.resultToProto(updateResponse.getResult()));

        // Note: ShardInfo not available in UpdateDocumentResponseBody protobuf definition

        // Note: GetResult conversion skipped for now due to protobuf complexity
        // This can be added later with proper GetResult protobuf mapping

        return UpdateDocumentResponse.newBuilder().setUpdateDocumentResponseBody(responseBodyBuilder.build()).build();
    }

    /**
     * Creates an error response from an exception.
     *
     * @param exception The exception that occurred
     * @param statusCode The HTTP status code
     * @return The protobuf UpdateDocumentResponse with error details
     */
    public static UpdateDocumentResponse toErrorProto(Exception exception, RestStatus statusCode) {
        throw new UnsupportedOperationException("Use GrpcErrorHandler.convertToGrpcError() instead");
    }
}
