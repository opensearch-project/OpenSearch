/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.response.document.common;

import org.opensearch.index.VersionType;

/**
 * Utility class for converting REST-like actions between OpenSearch and Protocol Buffers formats.
 * This class provides methods to transform response components such as shard statistics and
 * broadcast headers to ensure proper communication between the OpenSearch server and gRPC clients.
 */
public class VersionTypeProtoUtils {

    private VersionTypeProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link VersionType#fromString(String)}
     *
     * @param versionType
     */
    public static VersionType fromProto(org.opensearch.protobufs.VersionType versionType) {
        switch (versionType) {
            case VERSION_TYPE_EXTERNAL:
                return VersionType.EXTERNAL;
            case VERSION_TYPE_EXTERNAL_GTE:
                return VersionType.EXTERNAL_GTE;
            default:
                return VersionType.INTERNAL;
        }
    }
}
