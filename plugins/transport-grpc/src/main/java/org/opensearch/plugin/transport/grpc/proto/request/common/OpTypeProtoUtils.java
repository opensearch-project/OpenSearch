/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.common;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.protobufs.OpType;

/**
 * Utility class for converting SourceConfig Protocol Buffers to FetchSourceContext objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding OpenSearch objects.
 */
public class OpTypeProtoUtils {

    private OpTypeProtoUtils() {
        // Utility class, no instances
    }

    /**
     *
     * Similar to {@link DocWriteRequest.OpType}
     *
     * @param opType
     * @return
     */
    public static DocWriteRequest.OpType fromProto(OpType opType) {

        switch (opType) {
            case OP_TYPE_CREATE:
                return DocWriteRequest.OpType.CREATE;
            case OP_TYPE_INDEX:
                return DocWriteRequest.OpType.INDEX;
            default:
                throw new UnsupportedOperationException("Invalid optype: " + opType);
        }
    }
}
