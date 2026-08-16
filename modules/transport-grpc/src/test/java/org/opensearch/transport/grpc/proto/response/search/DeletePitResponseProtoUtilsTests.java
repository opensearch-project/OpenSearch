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
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class DeletePitResponseProtoUtilsTests extends OpenSearchTestCase {

    public void testToProto() {
        DeletePitResponse response = new DeletePitResponse(List.of(new DeletePitInfo(true, "pit-1"), new DeletePitInfo(false, "pit-2")));

        org.opensearch.protobufs.DeletePITResponse protoResponse = DeletePitResponseProtoUtils.toProto(response);

        assertNotNull(protoResponse);
        assertEquals(2, protoResponse.getPitsCount());
        assertTrue(protoResponse.getPits(0).getSuccessful());
        assertEquals("pit-1", protoResponse.getPits(0).getPitId());
        assertFalse(protoResponse.getPits(1).getSuccessful());
        assertEquals("pit-2", protoResponse.getPits(1).getPitId());
    }
}
