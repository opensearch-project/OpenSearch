/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestCreatePitAction;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Utility class for converting PIT creation protobuf requests to OpenSearch requests.
 */
public class CreatePitRequestProtoUtils {
    private static final String KEEP_ALIVE = "keep_alive";

    private CreatePitRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a protobuf PIT request to an OpenSearch create PIT request.
     * Similar to {@link RestCreatePitAction#prepareRequest(RestRequest, NodeClient)}.
     *
     * @param request the protobuf request
     * @return the OpenSearch request
     */
    public static CreatePitRequest prepareRequest(org.opensearch.protobufs.CreatePitRequest request) {
        if (request.hasGlobalParams()) {
            throw new UnsupportedOperationException("global_params param is not supported yet");
        }

        TimeValue keepAlive = request.getKeepAlive().isEmpty() ? null : TimeValue.parseTimeValue(request.getKeepAlive(), null, KEEP_ALIVE);
        boolean allowPartialPitCreation = request.hasAllowPartialPitCreation() ? request.getAllowPartialPitCreation() : true;

        CreatePitRequest createPitRequest = new CreatePitRequest(
            keepAlive,
            allowPartialPitCreation,
            request.getIndexList().toArray(new String[0])
        );
        createPitRequest.setIndicesOptions(IndicesOptionsProtoUtils.fromRequest(request, createPitRequest.indicesOptions()));
        createPitRequest.setPreference(request.hasPreference() ? request.getPreference() : null);
        createPitRequest.setRouting(request.getRoutingCount() > 0 ? String.join(",", request.getRoutingList()) : null);
        return createPitRequest;
    }
}
