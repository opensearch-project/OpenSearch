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

public class IndexCorrelationRequestTests extends OpenSearchTestCase {

    public void testIndexCorrelationRequest() throws IOException {
        IndexCorrelationRequest request = new IndexCorrelationRequest("index1", "event1", true);
        Assert.assertNotNull(request);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        IndexCorrelationRequest newRequest = new IndexCorrelationRequest(sin);

        Assert.assertEquals("index1", newRequest.getIndex());
        Assert.assertEquals("event1", newRequest.getEvent());
        Assert.assertEquals(true, newRequest.getStore());
    }
}
