/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.common;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.protobufs.Result;

/**
 * Utility class for converting DocWriteResponse fields to protobuf.
 */
public class DocWriteResponseProtoUtils {

    private DocWriteResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a DocWriteResponse.Result to protobuf Result.
     *
     * @param result The DocWriteResponse.Result
     * @return The corresponding protobuf Result
     */
    public static Result resultToProto(DocWriteResponse.Result result) {
        switch (result) {
            case CREATED:
                return Result.RESULT_CREATED;
            case UPDATED:
                return Result.RESULT_UPDATED;
            case DELETED:
                return Result.RESULT_DELETED;
            case NOT_FOUND:
                return Result.RESULT_NOT_FOUND;
            case NOOP:
                return Result.RESULT_NOOP;
            default:
                return Result.RESULT_UNSPECIFIED;
        }
    }
}
