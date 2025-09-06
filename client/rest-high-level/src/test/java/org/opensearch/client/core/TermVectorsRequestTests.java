/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.core;

import org.opensearch.client.AbstractRequestTestCase;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

public class TermVectorsRequestTests extends AbstractRequestTestCase<
    TermVectorsRequest,
    org.opensearch.action.termvectors.TermVectorsRequest> {
    @Override
    protected TermVectorsRequest createClientTestInstance() {
        TermVectorsRequest clientRequest = new TermVectorsRequest(randomAlphaOfLength(5), randomAlphaOfLength(5));
        clientRequest.setRouting("some-routing");
        return clientRequest;
    }

    @Override
    protected org.opensearch.action.termvectors.TermVectorsRequest doParseToServerInstance(XContentParser parser) throws IOException {
        org.opensearch.action.termvectors.TermVectorsRequest serverRequest = new org.opensearch.action.termvectors.TermVectorsRequest();
        org.opensearch.action.termvectors.TermVectorsRequest.parseRequest(serverRequest, parser);
        return serverRequest;
    }

    @Override
    protected void assertInstances(
        org.opensearch.action.termvectors.TermVectorsRequest serverInstance,
        TermVectorsRequest clientTestInstance
    ) {
        assertEquals(serverInstance.routing(), clientTestInstance.getRouting());
    }
}
