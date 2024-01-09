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
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.Map;

public class IndexCorrelationResponseTests extends OpenSearchTestCase {

    public void testIndexCorrelationResponse() throws IOException {
        IndexCorrelationResponse response = new IndexCorrelationResponse(true, Map.of(), RestStatus.CREATED);
        Assert.assertNotNull(response);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);

        IndexCorrelationResponse newResponse = new IndexCorrelationResponse(sin);
        Assert.assertEquals(true, newResponse.getOrphan());
        Assert.assertEquals(Map.of(), newResponse.getNeighborEvents());
        Assert.assertEquals(RestStatus.CREATED, newResponse.getStatus());
    }
}
