/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.action.search.DeletePitInfo;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.protobufs.DeletePITResponse;
import org.opensearch.protobufs.DeletedPit;

/**
 * Utility class for converting PIT deletion responses to protobuf.
 */
public class DeletePitResponseProtoUtils {
    private DeletePitResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an OpenSearch delete PIT response to protobuf.
     *
     * @param response the OpenSearch response
     * @return the protobuf response
     */
    public static DeletePITResponse toProto(DeletePitResponse response) {
        DeletePITResponse.Builder builder = DeletePITResponse.newBuilder();
        for (DeletePitInfo pitInfo : response.getDeletePitResults()) {
            builder.addPits(DeletedPit.newBuilder().setSuccessful(pitInfo.isSuccessful()).setPitId(pitInfo.getPitId()).build());
        }
        return builder.build();
    }
}
