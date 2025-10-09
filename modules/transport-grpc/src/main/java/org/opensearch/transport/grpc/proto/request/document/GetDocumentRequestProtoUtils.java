/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document;

import org.opensearch.action.get.GetRequest;
import org.opensearch.protobufs.GetDocumentRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

/**
 * Utility class for converting protobuf GetDocumentRequest to OpenSearch GetRequest.
 * <p>
 * This class provides functionality similar to the REST layer's get document request processing.
 * The parameter mapping and processing logic should be kept consistent with the corresponding
 * REST implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.rest.action.document.RestGetAction#prepareRequest(RestRequest, NodeClient) REST equivalent for parameter processing
 * @see org.opensearch.action.get.GetRequest OpenSearch internal get request representation
 * @see org.opensearch.protobufs.GetDocumentRequest Protobuf definition for gRPC get requests
 */
public class GetDocumentRequestProtoUtils {

    private GetDocumentRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a protobuf GetDocumentRequest to an OpenSearch GetRequest.
     * <p>
     * This method processes get document request parameters similar to how
     * {@link org.opensearch.rest.action.document.RestGetAction#prepareRequest(RestRequest, NodeClient)}
     * processes REST requests. Parameter mapping includes index name, document ID, routing,
     * preference, realtime, refresh, stored fields, versioning, and source filtering.
     *
     * @param protoRequest The protobuf GetDocumentRequest to convert
     * @return The corresponding OpenSearch GetRequest
     * @throws IllegalArgumentException if required fields are missing or invalid
     * @see org.opensearch.rest.action.document.RestGetAction#prepareRequest(RestRequest, NodeClient) REST equivalent
     */
    public static GetRequest fromProto(GetDocumentRequest protoRequest) {
        if (protoRequest.getIndex().isEmpty()) {
            throw new IllegalArgumentException("Index name is required");
        }

        if (protoRequest.getId().isEmpty()) {
            throw new IllegalArgumentException("Document ID is required");
        }

        GetRequest getRequest = new GetRequest(protoRequest.getIndex(), protoRequest.getId());

        // Set routing if provided
        if (protoRequest.hasRouting() && !protoRequest.getRouting().isEmpty()) {
            getRequest.routing(protoRequest.getRouting());
        }

        // Set preference if provided
        if (protoRequest.hasPreference() && !protoRequest.getPreference().isEmpty()) {
            getRequest.preference(protoRequest.getPreference());
        }

        // Set realtime if provided
        if (protoRequest.hasRealtime()) {
            getRequest.realtime(protoRequest.getRealtime());
        }

        // Set refresh if provided
        if (protoRequest.hasRefresh()) {
            getRequest.refresh(protoRequest.getRefresh());
        }

        // Set stored fields if provided
        if (!protoRequest.getStoredFieldsList().isEmpty()) {
            String[] storedFields = protoRequest.getStoredFieldsList().toArray(new String[0]);
            getRequest.storedFields(storedFields);
        }

        // Set version if provided
        if (protoRequest.hasVersion()) {
            getRequest.version(protoRequest.getVersion());
        }

        // Set source configuration if provided
        if (!protoRequest.getXSourceExcludesList().isEmpty() || !protoRequest.getXSourceIncludesList().isEmpty()) {
            String[] includes = protoRequest.getXSourceIncludesList().isEmpty()
                ? null
                : protoRequest.getXSourceIncludesList().toArray(new String[0]);
            String[] excludes = protoRequest.getXSourceExcludesList().isEmpty()
                ? null
                : protoRequest.getXSourceExcludesList().toArray(new String[0]);
            getRequest.fetchSourceContext(new FetchSourceContext(true, includes, excludes));
        }

        return getRequest;
    }
}
