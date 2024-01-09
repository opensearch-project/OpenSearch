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

public class StoreCorrelationResponseTests extends OpenSearchTestCase {

    public void testStoreCorrelationResponse() throws IOException {
        StoreCorrelationResponse response = new StoreCorrelationResponse(RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        StoreCorrelationResponse newResponse = new StoreCorrelationResponse(sin);

        Assert.assertEquals(RestStatus.OK, newResponse.getStatus());
    }
}
