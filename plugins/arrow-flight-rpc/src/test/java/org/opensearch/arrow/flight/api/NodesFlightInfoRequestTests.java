/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class NodesFlightInfoRequestTests extends OpenSearchTestCase {

    public void testNodesFlightInfoRequestSerialization() throws Exception {
        NodesFlightInfoRequest originalRequest = new NodesFlightInfoRequest("node1", "node2");

        BytesStreamOutput output = new BytesStreamOutput();
        originalRequest.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        NodesFlightInfoRequest deserializedRequest = new NodesFlightInfoRequest(input);

        assertArrayEquals(originalRequest.nodesIds(), deserializedRequest.nodesIds());
    }

    public void testNodesFlightInfoRequestConcreteNodes() {
        String[] nodeIds = new String[] { "node1", "node2" };
        NodesFlightInfoRequest request = new NodesFlightInfoRequest(nodeIds);
        assertArrayEquals(nodeIds, request.nodesIds());
    }

    public void testNodesFlightInfoRequestAllNodes() {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest();
        assertEquals(0, request.nodesIds().length);
    }
}
