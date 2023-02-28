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

package org.opensearch.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Table;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.rest.action.cat.AbstractCatAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestHandler.ReplacedRoute;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.object.HasToString.hasToString;

public class BaseRestHandlerTests extends OpenSearchTestCase {
    private NodeClient mockClient;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(this.getClass().getSimpleName() + "ThreadPool");
        mockClient = new NodeClient(Settings.EMPTY, threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        mockClient.close();
    }

    public void testOneUnconsumedParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        BaseRestHandler handler = new BaseRestHandler() {
            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                request.param("consumed");
                return channel -> executed.set(true);
            }

            @Override
            public String getName() {
                return "test_one_unconsumed_response_action";
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("consumed", randomAlphaOfLength(8));
        params.put("unconsumed", randomAlphaOfLength(8));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> handler.handleRequest(request, channel, mockClient)
        );
        assertThat(e, hasToString(containsString("request [/] contains unrecognized parameter: [unconsumed]")));
        assertFalse(executed.get());
    }

    public void testMultipleUnconsumedParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        BaseRestHandler handler = new BaseRestHandler() {
            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                request.param("consumed");
                return channel -> executed.set(true);
            }

            @Override
            public String getName() {
                return "test_multiple_unconsumed_response_action";
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("consumed", randomAlphaOfLength(8));
        params.put("unconsumed-first", randomAlphaOfLength(8));
        params.put("unconsumed-second", randomAlphaOfLength(8));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> handler.handleRequest(request, channel, mockClient)
        );
        assertThat(e, hasToString(containsString("request [/] contains unrecognized parameters: [unconsumed-first], [unconsumed-second]")));
        assertFalse(executed.get());
    }

    public void testUnconsumedParametersDidYouMean() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        BaseRestHandler handler = new BaseRestHandler() {
            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                request.param("consumed");
                request.param("field");
                request.param("tokenizer");
                request.param("very_close_to_parameter_1");
                request.param("very_close_to_parameter_2");
                return channel -> executed.set(true);
            }

            @Override
            protected Set<String> responseParams() {
                return Collections.singleton("response_param");
            }

            @Override
            public String getName() {
                return "test_unconsumed_did_you_mean_response_action";
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("consumed", randomAlphaOfLength(8));
        params.put("flied", randomAlphaOfLength(8));
        params.put("respones_param", randomAlphaOfLength(8));
        params.put("tokenzier", randomAlphaOfLength(8));
        params.put("very_close_to_parametre", randomAlphaOfLength(8));
        params.put("very_far_from_every_consumed_parameter", randomAlphaOfLength(8));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> handler.handleRequest(request, channel, mockClient)
        );
        assertThat(
            e,
            hasToString(
                containsString(
                    "request [/] contains unrecognized parameters: "
                        + "[flied] -> did you mean [field]?, "
                        + "[respones_param] -> did you mean [response_param]?, "
                        + "[tokenzier] -> did you mean [tokenizer]?, "
                        + "[very_close_to_parametre] -> did you mean any of [very_close_to_parameter_1, very_close_to_parameter_2]?, "
                        + "[very_far_from_every_consumed_parameter]"
                )
            )
        );
        assertFalse(executed.get());
    }

    public void testUnconsumedResponseParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        BaseRestHandler handler = new BaseRestHandler() {
            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                request.param("consumed");
                return channel -> executed.set(true);
            }

            @Override
            protected Set<String> responseParams() {
                return Collections.singleton("response_param");
            }

            @Override
            public String getName() {
                return "test_unconsumed_response_action";
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("consumed", randomAlphaOfLength(8));
        params.put("response_param", randomAlphaOfLength(8));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        handler.handleRequest(request, channel, mockClient);
        assertTrue(executed.get());
    }

    public void testDefaultResponseParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        BaseRestHandler handler = new BaseRestHandler() {
            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                return channel -> executed.set(true);
            }

            @Override
            public String getName() {
                return "test_default_response_action";
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("format", randomAlphaOfLength(8));
        params.put("filter_path", randomAlphaOfLength(8));
        params.put("pretty", randomFrom("true", "false", "", null));
        params.put("human", null);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        handler.handleRequest(request, channel, mockClient);
        assertTrue(executed.get());
    }

    public void testCatResponseParameters() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        AbstractCatAction handler = new AbstractCatAction() {
            @Override
            protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
                return channel -> executed.set(true);
            }

            @Override
            protected void documentation(StringBuilder sb) {

            }

            @Override
            protected Table getTableWithHeader(RestRequest request) {
                return null;
            }

            @Override
            public String getName() {
                return "test_cat_response_action";
            }
        };

        final HashMap<String, String> params = new HashMap<>();
        params.put("format", randomAlphaOfLength(8));
        params.put("h", randomAlphaOfLength(8));
        params.put("v", randomAlphaOfLength(8));
        params.put("ts", randomAlphaOfLength(8));
        params.put("pri", randomAlphaOfLength(8));
        params.put("bytes", randomAlphaOfLength(8));
        params.put("size", randomAlphaOfLength(8));
        params.put("time", randomAlphaOfLength(8));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        handler.handleRequest(request, channel, mockClient);
        assertTrue(executed.get());
    }

    public void testConsumedBody() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        final BaseRestHandler handler = new BaseRestHandler() {
            @Override
            protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
                request.content();
                return channel -> executed.set(true);
            }

            @Override
            public String getName() {
                return "test_consumed_body";
            }
        };

        try (XContentBuilder builder = JsonXContent.contentBuilder().startObject().endObject()) {
            final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
                new BytesArray(builder.toString()),
                XContentType.JSON
            ).build();
            final RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
            handler.handleRequest(request, channel, mockClient);
            assertTrue(executed.get());
        }
    }

    public void testUnconsumedNoBody() throws Exception {
        final AtomicBoolean executed = new AtomicBoolean();
        final BaseRestHandler handler = new BaseRestHandler() {
            @Override
            protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
                return channel -> executed.set(true);
            }

            @Override
            public String getName() {
                return "test_unconsumed_body";
            }
        };

        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
        final RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
        handler.handleRequest(request, channel, mockClient);
        assertTrue(executed.get());
    }

    public void testUnconsumedBody() throws IOException {
        final AtomicBoolean executed = new AtomicBoolean();
        final BaseRestHandler handler = new BaseRestHandler() {
            @Override
            protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
                return channel -> executed.set(true);
            }

            @Override
            public String getName() {
                return "test_unconsumed_body";
            }
        };

        try (XContentBuilder builder = JsonXContent.contentBuilder().startObject().endObject()) {
            final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
                new BytesArray(builder.toString()),
                XContentType.JSON
            ).build();
            final RestChannel channel = new FakeRestChannel(request, randomBoolean(), 1);
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> handler.handleRequest(request, channel, mockClient)
            );
            assertThat(e, hasToString(containsString("request [GET /] does not support having a body")));
            assertFalse(executed.get());
        }
    }

    public void testReplaceRoutesMethod() throws Exception {
        List<Route> routes = Arrays.asList(new Route(Method.GET, "/path/test"), new Route(Method.PUT, "/path2/test"));
        List<ReplacedRoute> replacedRoutes = RestHandler.replaceRoutes(routes, "/prefix", "/deprecatedPrefix");

        for (int i = 0; i < routes.size(); i++) {
            assertEquals("/prefix" + routes.get(i).getPath(), replacedRoutes.get(i).getPath());
            assertEquals(routes.get(i).getMethod(), replacedRoutes.get(i).getMethod());
            assertEquals("/deprecatedPrefix" + routes.get(i).getPath(), replacedRoutes.get(i).getDeprecatedPath());
        }
    }

}
