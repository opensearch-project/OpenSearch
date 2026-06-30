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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class RestGetAliasesActionTests extends OpenSearchTestCase {

    // # Assumes the following setup
    // curl -X PUT "localhost:9200/index" -H "Content-Type: application/json" -d'
    // {
    // "aliases": {
    // "foo": {},
    // "foobar": {}
    // }
    // }'

    public void testBareRequest() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final AliasMetadata foobarAliasMetadata = AliasMetadata.builder("foobar").build();
        final AliasMetadata fooAliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(fooAliasMetadata, foobarAliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(false, new String[0], openMapBuilder, xContentBuilder);
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foo\":{},\"foobar\":{}}}}"));
    }

    public void testSimpleAliasWildcardMatchingNothing() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "baz*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    public void testMultipleAliasWildcardsSomeMatching() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foobar").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "baz*", "foobar*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foobar\":{}}}}"));
    }

    public void testAliasWildcardsIncludeAndExcludeAll() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foob*", "-foo*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    public void testAliasWildcardsIncludeAndExcludeSome() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foo*", "-foob*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{\"index\":{\"aliases\":{\"foo\":{}}}}"));
    }

    public void testAliasWildcardsIncludeAndExcludeSomeAndExplicitMissing() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final AliasMetadata aliasMetadata = AliasMetadata.builder("foo").build();
        openMapBuilder.put("index", Arrays.asList(aliasMetadata));
        final String[] aliasPattern;
        if (randomBoolean()) {
            aliasPattern = new String[] { "missing", "foo*", "-foob*" };
        } else {
            aliasPattern = new String[] { "foo*", "-foob*", "missing" };
        }

        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(true, aliasPattern, openMapBuilder, xContentBuilder);
        assertThat(restResponse.status(), equalTo(NOT_FOUND));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(
            restResponse.content().utf8ToString(),
            equalTo("{\"error\":\"alias [missing] missing\",\"status\":404,\"index\":{\"aliases\":{\"foo\":{}}}}")
        );
    }

    public void testAliasWildcardsExcludeExplicitMissing() throws Exception {
        final XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        final Map<String, List<AliasMetadata>> openMapBuilder = new HashMap<>();
        final RestResponse restResponse = RestGetAliasesAction.buildRestResponse(
            true,
            new String[] { "foo", "foofoo", "-foo*" },
            openMapBuilder,
            xContentBuilder
        );
        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content().utf8ToString(), equalTo("{}"));
    }

    // NodeClient#admin() is final, so it cannot be mocked directly: https://github.com/mockito/mockito/issues/146
    // Instead we use a real NoOpNodeClient (overriding doExecute) and a real TestThreadPool whose
    // MANAGEMENT executor is wrapped to record the thread that actually runs the dispatched work.
    public void testProcessResponseForksToManagementThreadPool() throws Exception {
        final AtomicReference<String> executingThreadName = new AtomicReference<>();
        final TestThreadPool threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ExecutorService executor(String name) {
                final ExecutorService delegate = super.executor(name);
                if (ThreadPool.Names.MANAGEMENT.equals(name) == false) {
                    return delegate;
                }
                return new AbstractExecutorService() {
                    @Override
                    public void execute(Runnable command) {
                        delegate.execute(() -> {
                            executingThreadName.set(Thread.currentThread().getName());
                            command.run();
                        });
                    }

                    @Override
                    public void shutdown() {
                        delegate.shutdown();
                    }

                    @Override
                    public List<Runnable> shutdownNow() {
                        return delegate.shutdownNow();
                    }

                    @Override
                    public boolean isShutdown() {
                        return delegate.isShutdown();
                    }

                    @Override
                    public boolean isTerminated() {
                        return delegate.isTerminated();
                    }

                    @Override
                    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                        return delegate.awaitTermination(timeout, unit);
                    }
                };
            }
        };
        try {
            final NodeClient client = new NoOpNodeClient(threadPool) {
                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    listener.onResponse((Response) new GetAliasesResponse(Collections.emptyMap()));
                }
            };

            final RestGetAliasesAction action = new RestGetAliasesAction(threadPool);
            final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath("/_aliases")
                .build();
            final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
            final String callingThreadName = Thread.currentThread().getName();

            action.handleRequest(request, channel, client);

            // The key assertion: response work ran on the MANAGEMENT pool, not on the calling (transport) thread.
            assertBusy(() -> assertThat(channel.responses().get(), equalTo(1)));
            assertThat(channel.errors().get(), equalTo(0));
            assertThat(executingThreadName.get(), notNullValue());
            assertThat(executingThreadName.get(), not(equalTo(callingThreadName)));
            assertThat(executingThreadName.get(), containsString("[" + ThreadPool.Names.MANAGEMENT + "]"));
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }
}
