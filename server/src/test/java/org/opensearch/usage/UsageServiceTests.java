/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.usage;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.support.AggregationUsageService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Locale;
import java.util.Map;

import static org.opensearch.search.aggregations.support.AggregationUsageService.OTHER_SUBTYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class UsageServiceTests extends OpenSearchTestCase {

    /**
     * Test that we can not add a null reference to a {@link org.opensearch.rest.RestHandler} to the {@link UsageService}.
     */
    public void testHandlerCanNotBeNull() {
        final UsageService service = new UsageService();
        expectThrows(NullPointerException.class, () -> service.addRestHandler(null));
    }

    /**
     * Test that we can not add an instance of a {@link org.opensearch.rest.RestHandler} with no name to the {@link UsageService}.
     */
    public void testAHandlerWithNoName() {
        final UsageService service = new UsageService();
        final BaseRestHandler horse = new MockRestHandler(null);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.addRestHandler(horse));
        assertThat(
            e.getMessage(),
            equalTo("handler of type [org.opensearch.usage.UsageServiceTests$MockRestHandler] does not have a name")
        );
    }

    /**
     * Test that we can add the same instance of a {@link org.opensearch.rest.RestHandler} to the {@link UsageService} multiple times.
     */
    public void testHandlerWithConflictingNamesButSameInstance() {
        final UsageService service = new UsageService();
        final String name = randomAlphaOfLength(8);
        final BaseRestHandler first = new MockRestHandler(name);
        service.addRestHandler(first);
        // nothing bad ever happens to me
        service.addRestHandler(first);
    }

    /**
     * Test that we can not add different instances of {@link org.opensearch.rest.RestHandler} with the same name to the
     * {@link UsageService}.
     */
    public void testHandlersWithConflictingNamesButDifferentInstances() {
        final UsageService service = new UsageService();
        final String name = randomAlphaOfLength(8);
        final BaseRestHandler first = new MockRestHandler(name);
        final BaseRestHandler second = new MockRestHandler(name);
        service.addRestHandler(first);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.addRestHandler(second));
        final String expected = String.format(
            Locale.ROOT,
            "handler of type [%s] conflicts with handler of type [%1$s] as they both have the same name [%s]",
            "org.opensearch.usage.UsageServiceTests$MockRestHandler",
            name
        );
        assertThat(e.getMessage(), equalTo(expected));
    }

    public void testRestUsage() throws Exception {
        RestRequest restRequest = new FakeRestRequest();
        BaseRestHandler handlerA = new MockRestHandler("a");
        BaseRestHandler handlerB = new MockRestHandler("b");
        BaseRestHandler handlerC = new MockRestHandler("c");
        BaseRestHandler handlerD = new MockRestHandler("d");
        BaseRestHandler handlerE = new MockRestHandler("e");
        BaseRestHandler handlerF = new MockRestHandler("f");
        UsageService usageService = new UsageService();
        usageService.addRestHandler(handlerA);
        usageService.addRestHandler(handlerB);
        usageService.addRestHandler(handlerC);
        usageService.addRestHandler(handlerD);
        usageService.addRestHandler(handlerE);
        usageService.addRestHandler(handlerF);
        try (NodeClient client = new NoOpNodeClient(this.getClass().getSimpleName() + "TestClient")) {
            handlerA.handleRequest(restRequest, null, client);
            handlerB.handleRequest(restRequest, null, client);
            handlerA.handleRequest(restRequest, null, client);
            handlerA.handleRequest(restRequest, null, client);
            handlerB.handleRequest(restRequest, null, client);
            handlerC.handleRequest(restRequest, null, client);
            handlerC.handleRequest(restRequest, null, client);
            handlerD.handleRequest(restRequest, null, client);
            handlerA.handleRequest(restRequest, null, client);
            handlerB.handleRequest(restRequest, null, client);
            handlerE.handleRequest(restRequest, null, client);
            handlerF.handleRequest(restRequest, null, client);
            handlerC.handleRequest(restRequest, null, client);
            handlerD.handleRequest(restRequest, null, client);
        }
        Map<String, Long> restUsage = usageService.getRestUsageStats();
        assertThat(restUsage, notNullValue());
        assertThat(restUsage.size(), equalTo(6));
        assertThat(restUsage.get("a"), equalTo(4L));
        assertThat(restUsage.get("b"), equalTo(3L));
        assertThat(restUsage.get("c"), equalTo(3L));
        assertThat(restUsage.get("d"), equalTo(2L));
        assertThat(restUsage.get("e"), equalTo(1L));
        assertThat(restUsage.get("f"), equalTo(1L));
    }

    @SuppressWarnings("unchecked")
    public void testAggsUsage() throws Exception {
        AggregationUsageService.Builder builder = new AggregationUsageService.Builder();

        builder.registerAggregationUsage("a", "x");
        builder.registerAggregationUsage("a", "y");
        builder.registerAggregationUsage("b", "x");
        builder.registerAggregationUsage("c");
        builder.registerAggregationUsage("b", "y");
        builder.registerAggregationUsage("a", "z");

        AggregationUsageService usageService = builder.build();

        usageService.incAggregationUsage("a", "x");
        for (int i = 0; i < 2; i++) {
            usageService.incAggregationUsage("a", "y");
        }
        for (int i = 0; i < 3; i++) {
            usageService.incAggregationUsage("a", "z");
        }
        for (int i = 0; i < 4; i++) {
            usageService.incAggregationUsage("b", "x");
        }
        for (int i = 0; i < 5; i++) {
            usageService.incAggregationUsage("b", "y");
        }
        for (int i = 0; i < 6; i++) {
            usageService.incAggregationUsage("c", OTHER_SUBTYPE);
        }

        Map<String, Object> aggsUsage = usageService.getUsageStats();
        assertThat(aggsUsage, notNullValue());
        assertThat(aggsUsage.size(), equalTo(3));
        assertThat(((Map<String, Object>) aggsUsage.get("a")).get("x"), equalTo(1L));
        assertThat(((Map<String, Object>) aggsUsage.get("a")).get("y"), equalTo(2L));
        assertThat(((Map<String, Object>) aggsUsage.get("a")).get("z"), equalTo(3L));
        assertThat(((Map<String, Object>) aggsUsage.get("b")).get("x"), equalTo(4L));
        assertThat(((Map<String, Object>) aggsUsage.get("b")).get("y"), equalTo(5L));
        assertThat(((Map<String, Object>) aggsUsage.get("c")).get(OTHER_SUBTYPE), equalTo(6L));
    }

    private class MockRestHandler extends BaseRestHandler {

        private String name;

        protected MockRestHandler(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            return channel -> {};
        }

    }

}
