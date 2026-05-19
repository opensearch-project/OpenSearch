/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ObjectMap;

/**
 * Utility class for converting ShardOperationFailedException objects to Protocol Buffers.
 * This class specifically handles the conversion of ShardOperationFailedException instances
 * to their Protocol Buffer representation, which represent failures that occur during
 * operations on specific shards in an OpenSearch cluster.
 */
public class ShardOperationFailedExceptionProtoUtils {

    private ShardOperationFailedExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a ShardOperationFailedException to a Protocol Buffer Value.
     * This method is similar to {@link ShardOperationFailedException#toXContent(XContentBuilder, ToXContent.Params)}
     * TODO why is ShardOperationFailedException#toXContent() empty?
     *
     * @param exception The ShardOperationFailedException to convert
     * @return A Protocol Buffer Value representing the exception (currently empty)
     */
    public static ObjectMap.Value toProto(ShardOperationFailedException exception) {
        return ObjectMap.Value.newBuilder().build();
    }
}
