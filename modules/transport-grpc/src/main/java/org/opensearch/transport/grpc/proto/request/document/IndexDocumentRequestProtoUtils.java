/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.protobufs.IndexDocumentRequest;
import org.opensearch.transport.grpc.proto.request.common.OpTypeProtoUtils;

/**
 * Utility class for converting protobuf IndexDocumentRequest to OpenSearch IndexRequest.
 * <p>
 * This class provides functionality similar to the REST layer's index document request processing.
 * The parameter mapping and processing logic should be kept consistent with the corresponding
 * REST implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.rest.action.document.RestIndexAction#prepareRequest(RestRequest, NodeClient) REST equivalent for parameter processing
 * @see org.opensearch.action.index.IndexRequest OpenSearch internal index request representation
 * @see org.opensearch.protobufs.IndexDocumentRequest Protobuf definition for gRPC index requests
 */
public class IndexDocumentRequestProtoUtils {

    private IndexDocumentRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a protobuf IndexDocumentRequest to an OpenSearch IndexRequest.
     * <p>
     * This method processes index document request parameters similar to how
     * {@link org.opensearch.rest.action.document.RestIndexAction#prepareRequest(RestRequest, NodeClient)}
     * processes REST requests. Parameter mapping includes index name, document ID, routing,
     * pipeline, source content, timeout, refresh policy, versioning, and operation type.
     *
     * @param protoRequest The protobuf IndexDocumentRequest to convert
     * @return The corresponding OpenSearch IndexRequest
     * @throws IllegalArgumentException if required fields are missing or invalid
     * @see org.opensearch.rest.action.document.RestIndexAction#prepareRequest(RestRequest, NodeClient) REST equivalent
     */
    public static IndexRequest fromProto(IndexDocumentRequest protoRequest) {
        if (!protoRequest.hasIndex() || protoRequest.getIndex().isEmpty()) {
            throw new IllegalArgumentException("Index name is required");
        }

        IndexRequest indexRequest = new IndexRequest(protoRequest.getIndex());

        // Set document ID if provided
        if (protoRequest.hasId() && !protoRequest.getId().isEmpty()) {
            indexRequest.id(protoRequest.getId());
        }

        // Set document content based on the oneof field
        if (protoRequest.hasBytesRequestBody()) {
            BytesReference documentBytes = new BytesArray(protoRequest.getBytesRequestBody().toByteArray());
            indexRequest.source(documentBytes, XContentType.JSON);
        } else if (protoRequest.hasRequestBody()) {
            // For now, we don't support ObjectMap - would need ObjectMapProtoUtils
            throw new UnsupportedOperationException("ObjectMap request body not yet supported, use bytes_request_body");
        } else {
            throw new IllegalArgumentException("Document content is required (use bytes_request_body field)");
        }

        // Set operation type if provided
        if (protoRequest.hasOpType()) {
            indexRequest.opType(OpTypeProtoUtils.fromProto(protoRequest.getOpType()));
        }

        // Set routing if provided
        if (protoRequest.hasRouting() && !protoRequest.getRouting().isEmpty()) {
            indexRequest.routing(protoRequest.getRouting());
        }

        // Set refresh policy if provided
        if (protoRequest.hasRefresh()) {
            indexRequest.setRefreshPolicy(convertRefresh(protoRequest.getRefresh()));
        }

        // Set version constraints if provided
        if (protoRequest.hasIfSeqNo()) {
            indexRequest.setIfSeqNo(protoRequest.getIfSeqNo());
        }

        if (protoRequest.hasIfPrimaryTerm()) {
            indexRequest.setIfPrimaryTerm(protoRequest.getIfPrimaryTerm());
        }

        // Set pipeline if provided
        if (protoRequest.hasPipeline() && !protoRequest.getPipeline().isEmpty()) {
            indexRequest.setPipeline(protoRequest.getPipeline());
        }

        // Set timeout if provided
        if (protoRequest.hasTimeout() && !protoRequest.getTimeout().isEmpty()) {
            indexRequest.timeout(protoRequest.getTimeout());
        }

        return indexRequest;
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
