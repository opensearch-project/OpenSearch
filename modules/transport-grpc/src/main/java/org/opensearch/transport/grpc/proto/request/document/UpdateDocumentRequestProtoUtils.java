/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.protobufs.UpdateDocumentRequest;
import org.opensearch.protobufs.UpdateDocumentRequestBody;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

/**
 * Utility class for converting protobuf UpdateDocumentRequest to OpenSearch UpdateRequest.
 * <p>
 * This class provides functionality similar to the REST layer's update document request processing.
 * The parameter mapping and processing logic should be kept consistent with the corresponding
 * REST implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.rest.action.document.RestUpdateAction#prepareRequest(RestRequest, NodeClient) REST equivalent for parameter processing
 * @see org.opensearch.action.update.UpdateRequest OpenSearch internal update request representation
 * @see org.opensearch.protobufs.UpdateDocumentRequest Protobuf definition for gRPC update requests
 */
public class UpdateDocumentRequestProtoUtils {

    private UpdateDocumentRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a protobuf UpdateDocumentRequest to an OpenSearch UpdateRequest.
     * <p>
     * This method processes update document request parameters similar to how
     * {@link org.opensearch.rest.action.document.RestUpdateAction#prepareRequest(RestRequest, NodeClient)}
     * processes REST requests. Parameter mapping includes index name, document ID, routing,
     * refresh policy, versioning, retry on conflict, timeout, source filtering, and request body
     * (including doc updates, upserts, and scripted updates).
     *
     * @param protoRequest The protobuf UpdateDocumentRequest to convert
     * @return The corresponding OpenSearch UpdateRequest
     * @throws IllegalArgumentException if required fields are missing or invalid
     * @see org.opensearch.rest.action.document.RestUpdateAction#prepareRequest(RestRequest, NodeClient) REST equivalent
     */
    public static UpdateRequest fromProto(UpdateDocumentRequest protoRequest) {
        if (!protoRequest.hasIndex() || protoRequest.getIndex().isEmpty()) {
            throw new IllegalArgumentException("Index name is required");
        }

        if (!protoRequest.hasId() || protoRequest.getId().isEmpty()) {
            throw new IllegalArgumentException("Document ID is required");
        }

        UpdateRequest updateRequest = new UpdateRequest(protoRequest.getIndex(), protoRequest.getId());

        // Set routing if provided
        if (protoRequest.hasRouting() && !protoRequest.getRouting().isEmpty()) {
            updateRequest.routing(protoRequest.getRouting());
        }

        // Set refresh policy if provided
        if (protoRequest.hasRefresh()) {
            updateRequest.setRefreshPolicy(convertRefresh(protoRequest.getRefresh()));
        }

        // Set version constraints if provided
        if (protoRequest.hasIfSeqNo()) {
            updateRequest.setIfSeqNo(protoRequest.getIfSeqNo());
        }

        if (protoRequest.hasIfPrimaryTerm()) {
            updateRequest.setIfPrimaryTerm(protoRequest.getIfPrimaryTerm());
        }

        // Set retry on conflict if provided
        if (protoRequest.hasRetryOnConflict()) {
            updateRequest.retryOnConflict(protoRequest.getRetryOnConflict());
        }

        // Set timeout if provided
        if (protoRequest.hasTimeout() && !protoRequest.getTimeout().isEmpty()) {
            updateRequest.timeout(protoRequest.getTimeout());
        }

        // Set source configuration if provided
        if (!protoRequest.getXSourceExcludesList().isEmpty()) {
            String[] excludes = protoRequest.getXSourceExcludesList().toArray(new String[0]);
            updateRequest.fetchSource(new FetchSourceContext(true, null, excludes));
        }

        if (!protoRequest.getXSourceIncludesList().isEmpty()) {
            String[] includes = protoRequest.getXSourceIncludesList().toArray(new String[0]);
            updateRequest.fetchSource(new FetchSourceContext(true, includes, null));
        }

        // Process request body if provided
        if (protoRequest.hasRequestBody()) {
            processRequestBody(updateRequest, protoRequest.getRequestBody());
        }

        return updateRequest;
    }

    /**
     * Processes the update request body and applies it to the UpdateRequest.
     *
     * @param updateRequest The OpenSearch UpdateRequest to modify
     * @param requestBody The protobuf UpdateDocumentRequestBody
     */
    private static void processRequestBody(UpdateRequest updateRequest, UpdateDocumentRequestBody requestBody) {
        // Set detect noop if provided
        if (requestBody.hasDetectNoop()) {
            updateRequest.detectNoop(requestBody.getDetectNoop());
        }

        // Set partial document update if provided
        if (requestBody.hasBytesDoc()) {
            BytesReference docBytes = new BytesArray(requestBody.getBytesDoc().toByteArray());
            updateRequest.doc(docBytes, XContentType.JSON);
        } else if (requestBody.hasDoc()) {
            // For now, we don't support ObjectMap - would need ObjectMapProtoUtils
            throw new UnsupportedOperationException("ObjectMap doc updates not yet supported, use bytes_doc");
        }

        // Set doc_as_upsert if provided
        if (requestBody.hasDocAsUpsert()) {
            updateRequest.docAsUpsert(requestBody.getDocAsUpsert());
        }

        // Set scripted_upsert if provided
        if (requestBody.hasScriptedUpsert()) {
            updateRequest.scriptedUpsert(requestBody.getScriptedUpsert());
        }

        // Set upsert document if provided
        if (requestBody.hasUpsert()) {
            BytesReference upsertBytes = new BytesArray(requestBody.getUpsert().toByteArray());
            updateRequest.upsert(upsertBytes, XContentType.JSON);
        }

        // Note: Script support would require ScriptProtoUtils implementation
        if (requestBody.hasScript()) {
            throw new UnsupportedOperationException("Script updates not yet supported");
        }
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
