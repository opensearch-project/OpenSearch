/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.action.support.WriteRequest;

/**
 * Utility class for converting Refresh Protocol Buffers to OpenSearch WriteRequest.RefreshPolicy values.
 * This class handles the conversion of Protocol Buffer refresh policy representations to their
 * corresponding OpenSearch refresh policy string values.
 */
public class RefreshProtoUtils {

    private RefreshProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer Refresh enum to its corresponding OpenSearch refresh policy string value.
     * This method maps the gRPC protocol buffer refresh policy values to the internal
     * OpenSearch WriteRequest.RefreshPolicy string values.
     *
     * @param refresh The Protocol Buffer Refresh enum to convert
     * @return The corresponding OpenSearch refresh policy string value
     */
    public static String getRefreshPolicy(org.opensearch.protobufs.Refresh refresh) {
        switch (refresh) {
            case REFRESH_TRUE:
                return WriteRequest.RefreshPolicy.IMMEDIATE.getValue();
            case REFRESH_WAIT_FOR:
                return WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue();
            case REFRESH_FALSE:
            case REFRESH_UNSPECIFIED:
            default:
                return WriteRequest.RefreshPolicy.NONE.getValue();
        }
    }
}
