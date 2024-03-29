/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StoreCorrelationRequestTests extends OpenSearchTestCase {

    public void testStoreCorrelationRequest() throws IOException {
        StoreCorrelationRequest request = new StoreCorrelationRequest("index1", "event1", 100000L, Map.of(), List.of());
        Assert.assertNotNull(request);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        StoreCorrelationRequest newRequest = new StoreCorrelationRequest(sin);

        Assert.assertEquals("index1", newRequest.getIndex());
        Assert.assertEquals("event1", newRequest.getEvent());
        Assert.assertEquals(100000L, (long) newRequest.getTimestamp());
        Assert.assertEquals(Map.of(), newRequest.getEventsAdjacencyList());
        Assert.assertEquals(List.of(), newRequest.getTags());
    }
}
