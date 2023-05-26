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

public class SearchCorrelatedEventsRequestTests extends OpenSearchTestCase {

    public void testSearchCorrelatedEventsRequest() throws IOException {
        SearchCorrelatedEventsRequest request = new SearchCorrelatedEventsRequest("index1", "event1", "timestamp1", 100000L, 5);
        Assert.assertNotNull(request);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        SearchCorrelatedEventsRequest newRequest = new SearchCorrelatedEventsRequest(sin);

        Assert.assertEquals("index1", newRequest.getIndex());
        Assert.assertEquals("event1", newRequest.getEvent());
        Assert.assertEquals("timestamp1", newRequest.getTimestampField());
        Assert.assertEquals(100000L, (long) newRequest.getTimeWindow());
        Assert.assertEquals(5, (int) newRequest.getNearbyEvents());
    }
}
