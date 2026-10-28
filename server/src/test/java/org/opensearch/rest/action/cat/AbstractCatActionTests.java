/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Table;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class AbstractCatActionTests extends OpenSearchTestCase {

    private static class TestCatAction extends AbstractCatAction {
        final AtomicInteger doCatRequestCalls = new AtomicInteger();

        @Override
        public List<RestHandler.Route> routes() {
            return Collections.emptyList();
        }

        @Override
        public String getName() {
            return "test_cat_action";
        }

        @Override
        protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
            doCatRequestCalls.incrementAndGet();
            return channel -> {};
        }

        @Override
        protected void documentation(StringBuilder sb) {}

        @Override
        protected Table getTableWithHeader(RestRequest request) {
            return new Table();
        }
    }

    public void testInvalidLimitIsRejectedBeforeDoCatRequest() {
        for (String invalid : new String[] { "0", "-1" }) {
            TestCatAction action = new TestCatAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Collections.singletonMap("limit", invalid))
                .build();
            expectThrows(OpenSearchParseException.class, () -> action.prepareRequest(request, null));
            assertEquals("doCatRequest must not run for an invalid limit", 0, action.doCatRequestCalls.get());
        }

        TestCatAction action = new TestCatAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Collections.singletonMap("limit", "abc")).build();
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertEquals(0, action.doCatRequestCalls.get());
    }

    public void testValidOrMissingLimitReachesDoCatRequest() throws Exception {
        TestCatAction action = new TestCatAction();
        action.prepareRequest(
            new FakeRestRequest.Builder(xContentRegistry()).withParams(Collections.singletonMap("limit", "5")).build(),
            null
        );
        action.prepareRequest(new FakeRestRequest.Builder(xContentRegistry()).build(), null);
        assertEquals(2, action.doCatRequestCalls.get());
    }
}
