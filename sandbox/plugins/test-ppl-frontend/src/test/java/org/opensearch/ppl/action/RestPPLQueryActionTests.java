/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

public class RestPPLQueryActionTests extends OpenSearchTestCase {

    private final RestPPLQueryAction action = new RestPPLQueryAction();

    public void testName() {
        assertEquals("analytics_ppl_query", action.getName());
    }

    public void testRoutes() {
        assertEquals(1, action.routes().size());
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
        assertEquals("/_analytics/ppl", action.routes().get(0).getPath());
    }

    public void testPrepareRequestMissingQuery() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_analytics/ppl")
            .withContent(new BytesArray("{\"other\":\"value\"}"), XContentType.JSON)
            .build();

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(ex.getMessage().contains("query"));
    }
}
