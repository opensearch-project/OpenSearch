/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document.common;

import org.opensearch.index.VersionType;

/**
 * Utility class for converting VersionType Protocol Buffers to OpenSearch VersionType objects.
 * This class handles the conversion of Protocol Buffer version type representations to their
 * corresponding OpenSearch version type enumerations.
 */
public class VersionTypeProtoUtils {

    private VersionTypeProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer VersionType to its corresponding OpenSearch VersionType.
     * Similar to {@link VersionType#fromString(String)}.
     *
     * @param versionType The Protocol Buffer VersionType to convert
     * @return The corresponding OpenSearch VersionType
     */
    public static VersionType fromProto(org.opensearch.protobufs.VersionType versionType) {
        switch (versionType) {
            case VERSION_TYPE_EXTERNAL:
                return VersionType.EXTERNAL;
            case VERSION_TYPE_EXTERNAL_GTE:
                return VersionType.EXTERNAL_GTE;
            case VERSION_TYPE_INTERNAL:
                return VersionType.INTERNAL;
            case VERSION_TYPE_UNSPECIFIED:
            default:
                return VersionType.INTERNAL;
        }
    }
}
