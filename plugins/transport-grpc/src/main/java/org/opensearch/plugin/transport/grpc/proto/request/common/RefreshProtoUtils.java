/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.common;

import org.opensearch.action.support.WriteRequest;

/**
 * Utility class for converting SourceConfig Protocol Buffers to FetchSourceContext objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding OpenSearch objects.
 */
public class RefreshProtoUtils {

    private RefreshProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Extracts the refresh policy from the bulk request.
     *
     * @param refresh The bulk request containing the refresh policy
     * @return The refresh policy as a string, or null if not specified
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
