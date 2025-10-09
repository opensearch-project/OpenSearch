/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document;

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.protobufs.DeleteDocumentRequest;

/**
 * Utility class for converting protobuf DeleteDocumentRequest to OpenSearch DeleteRequest.
 * <p>
 * This class provides functionality similar to the REST layer's delete document request processing.
 * The parameter mapping and processing logic should be kept consistent with the corresponding
 * REST implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.rest.action.document.RestDeleteAction#prepareRequest(RestRequest, NodeClient) REST equivalent for parameter processing
 * @see org.opensearch.action.delete.DeleteRequest OpenSearch internal delete request representation
 * @see org.opensearch.protobufs.DeleteDocumentRequest Protobuf definition for gRPC delete requests
 */
public class DeleteDocumentRequestProtoUtils {

    private DeleteDocumentRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a protobuf DeleteDocumentRequest to an OpenSearch DeleteRequest.
     * <p>
     * This method processes delete document request parameters similar to how
     * {@link org.opensearch.rest.action.document.RestDeleteAction#prepareRequest(RestRequest, NodeClient)}
     * processes REST requests. Parameter mapping includes index name, document ID, routing,
     * timeout, refresh policy, versioning, and wait for active shards.
     *
     * @param protoRequest The protobuf DeleteDocumentRequest to convert
     * @return The corresponding OpenSearch DeleteRequest
     * @throws IllegalArgumentException if required fields are missing or invalid
     * @see org.opensearch.rest.action.document.RestDeleteAction#prepareRequest(RestRequest, NodeClient) REST equivalent
     */
    public static DeleteRequest fromProto(DeleteDocumentRequest protoRequest) {
        if (protoRequest.getIndex().isEmpty()) {
            throw new IllegalArgumentException("Index name is required");
        }

        if (protoRequest.getId().isEmpty()) {
            throw new IllegalArgumentException("Document ID is required");
        }

        DeleteRequest deleteRequest = new DeleteRequest(protoRequest.getIndex(), protoRequest.getId());

        // Set routing if provided
        if (protoRequest.hasRouting() && !protoRequest.getRouting().isEmpty()) {
            deleteRequest.routing(protoRequest.getRouting());
        }

        // Set refresh policy if provided
        if (protoRequest.hasRefresh()) {
            deleteRequest.setRefreshPolicy(convertRefresh(protoRequest.getRefresh()));
        }

        // Set version constraints if provided
        if (protoRequest.hasIfSeqNo()) {
            deleteRequest.setIfSeqNo(protoRequest.getIfSeqNo());
        }

        if (protoRequest.hasIfPrimaryTerm()) {
            deleteRequest.setIfPrimaryTerm(protoRequest.getIfPrimaryTerm());
        }

        // Set timeout if provided
        if (protoRequest.hasTimeout() && !protoRequest.getTimeout().isEmpty()) {
            deleteRequest.timeout(protoRequest.getTimeout());
        }

        return deleteRequest;
    }

    /**
     * Convert protobuf Refresh to WriteRequest.RefreshPolicy.
     */
    private static WriteRequest.RefreshPolicy convertRefresh(org.opensearch.protobufs.Refresh refresh) {
        switch (refresh) {
            case REFRESH_TRUE:
                return WriteRequest.RefreshPolicy.IMMEDIATE;
            case REFRESH_WAIT_FOR:
                return WriteRequest.RefreshPolicy.WAIT_UNTIL;
            case REFRESH_FALSE:
            default:
                return WriteRequest.RefreshPolicy.NONE;
        }
    }
}
