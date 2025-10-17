/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.protobufs.OpType;

/**
 * Utility class for converting OpType Protocol Buffers to OpenSearch DocWriteRequest.OpType objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding OpenSearch operation type enumerations.
 */
public class OpTypeProtoUtils {

    private OpTypeProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer OpType to its corresponding OpenSearch DocWriteRequest.OpType.
     * Similar to {@link DocWriteRequest.OpType}.
     *
     * @param opType The Protocol Buffer OpType to convert
     * @return The corresponding OpenSearch DocWriteRequest.OpType
     * @throws UnsupportedOperationException if the operation type is not supported
     */
    public static DocWriteRequest.OpType fromProto(OpType opType) {

        switch (opType) {
            case OP_TYPE_CREATE:
                return DocWriteRequest.OpType.CREATE;
            case OP_TYPE_INDEX:
                return DocWriteRequest.OpType.INDEX;
            case OP_TYPE_UNSPECIFIED:
            default:
                throw new UnsupportedOperationException("Invalid optype: " + opType);
        }
    }
}
