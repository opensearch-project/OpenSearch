/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.action.search.DeletePitRequest;

/**
 * Utility class for converting PIT deletion protobuf requests to OpenSearch requests.
 */
public class DeletePitRequestProtoUtils {

    private DeletePitRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a protobuf delete PIT request to an OpenSearch delete PIT request.
     *
     * @param request the protobuf request
     * @return the OpenSearch request
     */
    public static DeletePitRequest prepareRequest(org.opensearch.protobufs.DeletePitRequest request) {
        return new DeletePitRequest(request.getPitIdList());
    }
}
