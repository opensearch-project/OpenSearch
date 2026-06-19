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

import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;

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

    @SuppressWarnings("unchecked")
    public void testProcessResponseForksToManagementThreadPool() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ExecutorService managementExecutor = mock(ExecutorService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(threadPool.executor(ThreadPool.Names.MANAGEMENT)).thenReturn(managementExecutor);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);

        // Wire up a NodeClient mock so we can capture the ActionListener
        org.opensearch.transport.client.AdminClient adminClient = mock(org.opensearch.transport.client.AdminClient.class);
        org.opensearch.transport.client.IndicesAdminClient indicesClient = mock(org.opensearch.transport.client.IndicesAdminClient.class);
        org.opensearch.transport.client.node.NodeClient nodeClient = mock(org.opensearch.transport.client.node.NodeClient.class);
        when(nodeClient.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);
        when(nodeClient.threadPool()).thenReturn(threadPool);

        RestGetAliasesAction action = new RestGetAliasesAction(threadPool);
        org.opensearch.rest.RestRequest request = new org.opensearch.test.rest.FakeRestRequest.Builder(xContentRegistry())
            .withMethod(org.opensearch.rest.RestRequest.Method.GET)
            .withPath("/_aliases")
            .build();
        org.opensearch.rest.RestChannel channel = mock(org.opensearch.rest.RestChannel.class);
        when(channel.newBuilder()).thenReturn(MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON));

        // prepareRequest registers the listener with indicesClient.getAliases(...)
        action.handleRequest(request, channel, nodeClient);

        // Capture the listener and fire onResponse to trigger processResponse
        org.mockito.ArgumentCaptor<org.opensearch.core.action.ActionListener> captor =
            org.mockito.ArgumentCaptor.forClass(org.opensearch.core.action.ActionListener.class);
        verify(indicesClient).getAliases(any(), captor.capture());
        captor.getValue().onResponse(new GetAliasesResponse(Collections.emptyMap()));

        // The key assertion: response work was forked to MANAGEMENT, not run on the transport thread
        verify(threadPool).executor(eq(ThreadPool.Names.MANAGEMENT));
        verify(managementExecutor).execute(any(Runnable.class));
    }
}
