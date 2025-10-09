/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document;

import org.opensearch.action.get.GetResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.protobufs.GetDocumentResponse;
import org.opensearch.protobufs.GetDocumentResponseBody;

/**
 * Utility class for converting OpenSearch GetResponse to protobuf GetDocumentResponse.
 * <p>
 * This class provides functionality similar to the REST layer's get response serialization.
 * The response mapping and field serialization should be kept consistent with the corresponding
 * REST XContent implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.action.get.GetResponse#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) REST equivalent for response serialization
 * @see org.opensearch.action.get.GetResponse#fromXContent(org.opensearch.core.xcontent.XContentParser) REST equivalent for response parsing
 * @see org.opensearch.index.get.GetResult#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) GetResult XContent serialization
 * @see org.opensearch.action.get.GetResponse OpenSearch internal get response representation
 * @see org.opensearch.protobufs.GetDocumentResponse Protobuf definition for gRPC get responses
 */
public class GetDocumentResponseProtoUtils {

    private GetDocumentResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an OpenSearch GetResponse to a protobuf GetDocumentResponse.
     * <p>
     * This method serializes get response fields similar to how
     * {@link org.opensearch.action.get.GetResponse#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params)}
     * serializes responses in REST. Field mapping includes index name, document ID, version,
     * sequence number, primary term, found status, and document source content.
     *
     * @param getResponse The OpenSearch GetResponse to convert
     * @return The corresponding protobuf GetDocumentResponse
     * @see org.opensearch.action.get.GetResponse#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) REST equivalent
     * @see org.opensearch.index.get.GetResult#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params) GetResult serialization
     */
    public static GetDocumentResponse toProto(GetResponse getResponse) {
        GetDocumentResponseBody.Builder responseBodyBuilder = GetDocumentResponseBody.newBuilder()
            .setXIndex(getResponse.getIndex())
            .setXId(getResponse.getId())
            .setFound(getResponse.isExists())
            .setXVersion(getResponse.getVersion())
            .setXSeqNo(getResponse.getSeqNo())
            .setXPrimaryTerm(getResponse.getPrimaryTerm());

        // Set source if available and document exists
        if (getResponse.isExists() && getResponse.getSource() != null && !getResponse.getSource().isEmpty()) {
            // Convert source map to JSON bytes using the x_source field
            try {
                String sourceJson = org.opensearch.common.xcontent.XContentHelper.convertToJson(
                    getResponse.getSourceAsBytesRef(),
                    false,
                    org.opensearch.common.xcontent.XContentType.JSON
                );
                responseBodyBuilder.setXSource(com.google.protobuf.ByteString.copyFromUtf8(sourceJson));
            } catch (Exception e) {
                // If source conversion fails, continue without it
            }
        }

        return GetDocumentResponse.newBuilder().setGetDocumentResponseBody(responseBodyBuilder.build()).build();
    }

    /**
     * Creates an error response from an exception.
     *
     * @param exception The exception that occurred
     * @param statusCode The HTTP status code
     * @return The protobuf GetDocumentResponse with error details
     */
    public static GetDocumentResponse toErrorProto(Exception exception, RestStatus statusCode) {
        throw new UnsupportedOperationException("Use GrpcErrorHandler.convertToGrpcError() instead");
    }
}
