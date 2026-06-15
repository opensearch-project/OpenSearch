/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.test.OpenSearchTestCase;

public class DeletePitRequestProtoUtilsTests extends OpenSearchTestCase {

    public void testPrepareRequestWithMultiplePitIds() {
        org.opensearch.protobufs.DeletePitRequest protoRequest = org.opensearch.protobufs.DeletePitRequest.newBuilder()
            .addPitId("pit-1")
            .addPitId("pit-2")
            .build();

        DeletePitRequest request = DeletePitRequestProtoUtils.prepareRequest(protoRequest);

        assertEquals(2, request.getPitIds().size());
        assertEquals("pit-1", request.getPitIds().get(0));
        assertEquals("pit-2", request.getPitIds().get(1));
    }

    public void testPrepareRequestWithAllPitSelector() {
        org.opensearch.protobufs.DeletePitRequest protoRequest = org.opensearch.protobufs.DeletePitRequest.newBuilder()
            .addPitId("_all")
            .build();

        DeletePitRequest request = DeletePitRequestProtoUtils.prepareRequest(protoRequest);

        assertEquals(1, request.getPitIds().size());
        assertEquals("_all", request.getPitIds().get(0));
    }
}
