/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.rest.RestRequest;
import org.opensearch.search.fetch.StoredFieldsContext;

import java.io.IOException;
import java.util.List;

/**
 * Utility class for converting StoredFieldsContext between OpenSearch and Protocol Buffers formats.
 * This class provides methods to create StoredFieldsContext objects from Protocol Buffer requests
 * to ensure proper handling of stored fields in search operations.
 */
public class StoredFieldsContextProtoUtils {

    private StoredFieldsContextProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Create a StoredFieldsContext from a Protocol Buffer list of field names
     *
     * @param storedFields the list of field names
     * @return a StoredFieldsContext
     * @throws IOException if an I/O exception occurred
     */
    protected static StoredFieldsContext fromProto(List<String> storedFields) throws IOException {
        if (storedFields == null || storedFields.isEmpty()) {
            return null;
        }
        return StoredFieldsContext.fromList(storedFields);
    }

    /**
     * Create a StoredFieldsContext from a Protocol Buffer SearchRequest
     * Similar to {@link StoredFieldsContext#fromRestRequest(String, RestRequest)}
     *
     * @param request the Protocol Buffer SearchRequest
     * @return a StoredFieldsContext
     */
    protected static StoredFieldsContext fromProtoRequest(org.opensearch.protobufs.SearchRequestBody request) {
        if (request.getStoredFieldsCount() > 0) {
            return StoredFieldsContext.fromList(request.getStoredFieldsList());
        }
        return null;
    }
}
