/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class CreatePitResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProto() throws IOException {
        CreatePitResponse response = new CreatePitResponse("pit-123", 12345L, 5, 4, 1, 0, ShardSearchFailure.EMPTY_ARRAY);

        org.opensearch.protobufs.CreatePITResponse protoResponse = CreatePitResponseProtoUtils.toProto(response);

        assertNotNull(protoResponse);
        assertEquals("pit-123", protoResponse.getPitId());
        assertEquals(12345L, protoResponse.getCreationTime());
        assertEquals(5, protoResponse.getXShards().getTotal());
        assertEquals(4, protoResponse.getXShards().getSuccessful());
        assertEquals(1, protoResponse.getXShards().getSkipped());
        assertEquals(0, protoResponse.getXShards().getFailed());
    }
}
