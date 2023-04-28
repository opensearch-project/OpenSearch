/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.filecache;

import org.opensearch.action.admin.cluster.node.filecache.clear.ClearNodesFileCacheRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ClearNodesFileCacheRequestTests extends OpenSearchTestCase {

    public void testRequest() {
        ClearNodesFileCacheRequest clearNodesFileCacheRequest = new ClearNodesFileCacheRequest();
        assertNull(clearNodesFileCacheRequest.nodesIds());
        assertNull(clearNodesFileCacheRequest.concreteNodes());
        assertNull(clearNodesFileCacheRequest.timeout());
    }

    public void testSerialization() throws Exception {
        ClearNodesFileCacheRequest clearNodesFileCacheRequest = new ClearNodesFileCacheRequest();
        assertNull(clearNodesFileCacheRequest.nodesIds());
        ClearNodesFileCacheRequest deserializedRequest = roundTripRequest(clearNodesFileCacheRequest);
        assertEquals(0, deserializedRequest.nodesIds().length);
        assertNull(deserializedRequest.concreteNodes());
        assertNull(deserializedRequest.timeout());
    }

    /**
     * Serialize and deserialize a request.
     * @param request A request to serialize.
     * @return The deserialized, "round-tripped" request.
     */
    private static ClearNodesFileCacheRequest roundTripRequest(ClearNodesFileCacheRequest request) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new ClearNodesFileCacheRequest(in);
            }
        }
    }
}
