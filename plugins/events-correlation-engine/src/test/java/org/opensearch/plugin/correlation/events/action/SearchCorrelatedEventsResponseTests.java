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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.correlation.events.model.EventWithScore;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;

public class SearchCorrelatedEventsResponseTests extends OpenSearchTestCase {

    public void testSearchCorrelatedEventsResponse() throws IOException {
        EventWithScore event = new EventWithScore("index1", "event1", 0.0, List.of());
        SearchCorrelatedEventsResponse response = new SearchCorrelatedEventsResponse(List.of(event), RestStatus.OK);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        SearchCorrelatedEventsResponse newResponse = new SearchCorrelatedEventsResponse(sin);

        Assert.assertNotNull(newResponse.getEvents());
        Assert.assertEquals(1, newResponse.getEvents().size());
        Assert.assertEquals(event, newResponse.getEvents().get(0));
    }
}
