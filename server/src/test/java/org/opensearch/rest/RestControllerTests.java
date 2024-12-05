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
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.yaml.YamlXContent;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpInfo;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpStats;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.rest.action.admin.indices.RestCreateIndexAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.usage.UsageService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestControllerTests extends OpenSearchTestCase {

    private static final ByteSizeValue BREAKER_LIMIT = new ByteSizeValue(20);
    private CircuitBreaker inFlightRequestsBreaker;
    private RestController restController;
    private HierarchyCircuitBreakerService circuitBreakerService;
    private UsageService usageService;
    private NodeClient client;

    @Before
    public void setup() {
        circuitBreakerService = new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), BREAKER_LIMIT)
                // We want to have reproducible results in this test, hence we disable real memory usage accounting
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        usageService = new UsageService();
        // we can do this here only because we know that we don't adjust breaker settings dynamically in the test
        inFlightRequestsBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        HttpServerTransport httpServerTransport = new TestHttpServerTransport();
        client = new NoOpNodeClient(this.getTestName());
        restController = new RestController(Collections.emptySet(), null, client, circuitBreakerService, usageService);
        restController.registerHandler(
            RestRequest.Method.GET,
            "/",
            (request, channel, client) -> channel.sendResponse(
                new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY)
            )
        );
        restController.registerHandler(RestRequest.Method.GET, "/error", (request, channel, client) -> {
            throw new IllegalArgumentException("test error");
        });

        httpServerTransport.start();
    }

    @After
    public void teardown() throws IOException {
        IOUtils.close(client);
    }

    public void testDefaultRestControllerGetAllHandlersContainsFavicon() {
        final RestController restController = new RestController(null, null, null, circuitBreakerService, usageService);
        Iterator<MethodHandlers> handlers = restController.getAllHandlers();
        assertTrue(handlers.hasNext());
        MethodHandlers faviconHandler = handlers.next();
        assertEquals(faviconHandler.getPath(), "/favicon.ico");
        assertEquals(faviconHandler.getValidMethods(), Set.of(RestRequest.Method.GET));
        assertFalse(handlers.hasNext());
    }

    public void testRestControllerGetAllHandlers() {
        final RestController restController = new RestController(null, null, null, circuitBreakerService, usageService);

        restController.registerHandler(RestRequest.Method.PATCH, "/foo", mock(RestHandler.class));
        restController.registerHandler(RestRequest.Method.GET, "/foo", mock(RestHandler.class));

        Iterator<MethodHandlers> handlers = restController.getAllHandlers();

        assertTrue(handlers.hasNext());
        MethodHandlers rootHandler = handlers.next();
        assertEquals(rootHandler.getPath(), "/foo");
        assertEquals(rootHandler.getValidMethods(), Set.of(RestRequest.Method.GET, RestRequest.Method.PATCH));

        assertTrue(handlers.hasNext());
        MethodHandlers faviconHandler = handlers.next();
        assertEquals(faviconHandler.getPath(), "/favicon.ico");
        assertEquals(faviconHandler.getValidMethods(), Set.of(RestRequest.Method.GET));

        assertFalse(handlers.hasNext());
    }

    public void testApplyRelevantHeaders() throws Exception {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        Set<RestHeaderDefinition> headers = new HashSet<>(
            Arrays.asList(new RestHeaderDefinition("header.1", true), new RestHeaderDefinition("header.2", true))
        );
        final RestController restController = new RestController(headers, null, null, circuitBreakerService, usageService);
        Map<String, List<String>> restHeaders = new HashMap<>();
        restHeaders.put("header.1", Collections.singletonList("true"));
        restHeaders.put("header.2", Collections.singletonList("true"));
        restHeaders.put("header.3", Collections.singletonList("false"));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(restHeaders).build();
        final RestController spyRestController = spy(restController);
        when(spyRestController.getAllRestMethodHandlers(null, fakeRequest.rawPath())).thenReturn(new Iterator<RestMethodHandlers>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public RestMethodHandlers next() {
                return new RestMethodHandlers("/", (RestRequest request, RestChannel channel, NodeClient client) -> {
                    assertEquals("true", threadContext.getHeader("header.1"));
                    assertEquals("true", threadContext.getHeader("header.2"));
                    assertNull(threadContext.getHeader("header.3"));
                }, RestRequest.Method.GET);
            }
        });
        AssertingChannel channel = new AssertingChannel(fakeRequest, false, RestStatus.BAD_REQUEST);
        restController.dispatchRequest(fakeRequest, channel, threadContext);
        // the rest controller relies on the caller to stash the context, so we should expect these values here as we didn't stash the
        // context in this test
        assertEquals("true", threadContext.getHeader("header.1"));
        assertEquals("true", threadContext.getHeader("header.2"));
        assertNull(threadContext.getHeader("header.3"));
    }

    public void testRequestWithDisallowedMultiValuedHeader() {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        Set<RestHeaderDefinition> headers = new HashSet<>(
            Arrays.asList(new RestHeaderDefinition("header.1", true), new RestHeaderDefinition("header.2", false))
        );
        final RestController restController = new RestController(headers, null, null, circuitBreakerService, usageService);
        Map<String, List<String>> restHeaders = new HashMap<>();
        restHeaders.put("header.1", Collections.singletonList("boo"));
        restHeaders.put("header.2", Arrays.asList("foo", "bar"));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(restHeaders).build();
        AssertingChannel channel = new AssertingChannel(fakeRequest, false, RestStatus.BAD_REQUEST);
        restController.dispatchRequest(fakeRequest, channel, threadContext);
        assertTrue(channel.getSendResponseCalled());
    }

    public void testRequestWithDisallowedMultiValuedHeaderButSameValues() {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        Set<RestHeaderDefinition> headers = new HashSet<>(
            Arrays.asList(new RestHeaderDefinition("header.1", true), new RestHeaderDefinition("header.2", false))
        );
        final RestController restController = new RestController(headers, null, client, circuitBreakerService, usageService);
        Map<String, List<String>> restHeaders = new HashMap<>();
        restHeaders.put("header.1", Collections.singletonList("boo"));
        restHeaders.put("header.2", Arrays.asList("foo", "foo"));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(restHeaders).withPath("/bar").build();
        restController.registerHandler(RestRequest.Method.GET, "/bar", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }
        });
        AssertingChannel channel = new AssertingChannel(fakeRequest, false, RestStatus.OK);
        restController.dispatchRequest(fakeRequest, channel, threadContext);
        assertTrue(channel.getSendResponseCalled());
    }

    public void testRegisterAsDeprecatedHandler() {
        RestController controller = mock(RestController.class);

        RestRequest.Method method = randomFrom(RestRequest.Method.values());
        String path = "/_" + randomAlphaOfLengthBetween(1, 6);
        RestHandler handler = mock(RestHandler.class);
        String deprecationMessage = randomAlphaOfLengthBetween(1, 10);

        // don't want to test everything -- just that it actually wraps the handler
        doCallRealMethod().when(controller).registerAsDeprecatedHandler(method, path, handler, deprecationMessage);

        controller.registerAsDeprecatedHandler(method, path, handler, deprecationMessage);

        verify(controller).registerHandler(eq(method), eq(path), any(DeprecationRestHandler.class));
    }

    public void testRegisterWithDeprecatedHandler() {
        final RestController controller = mock(RestController.class);

        final RestRequest.Method method = randomFrom(RestRequest.Method.values());
        final String path = "/_" + randomAlphaOfLengthBetween(1, 6);
        final RestHandler handler = mock(RestHandler.class);
        final RestRequest.Method deprecatedMethod = randomFrom(RestRequest.Method.values());
        final String deprecatedPath = "/_" + randomAlphaOfLengthBetween(1, 6);

        final String deprecationMessage = "["
            + deprecatedMethod.name()
            + " "
            + deprecatedPath
            + "] is deprecated! Use ["
            + method.name()
            + " "
            + path
            + "] instead.";

        // don't want to test everything -- just that it actually wraps the handlers
        doCallRealMethod().when(controller).registerWithDeprecatedHandler(method, path, handler, deprecatedMethod, deprecatedPath);

        controller.registerWithDeprecatedHandler(method, path, handler, deprecatedMethod, deprecatedPath);

        verify(controller).registerHandler(method, path, handler);
        verify(controller).registerAsDeprecatedHandler(deprecatedMethod, deprecatedPath, handler, deprecationMessage);
    }

    public void testRegisterSecondMethodWithDifferentNamedWildcard() {
        final RestController restController = new RestController(null, null, null, circuitBreakerService, usageService);

        RestRequest.Method firstMethod = randomFrom(RestRequest.Method.values());
        RestRequest.Method secondMethod = randomFrom(
            Arrays.stream(RestRequest.Method.values()).filter(m -> m != firstMethod).collect(Collectors.toList())
        );

        final String path = "/_" + randomAlphaOfLengthBetween(1, 6);

        RestHandler handler = mock(RestHandler.class);
        restController.registerHandler(firstMethod, path + "/{wildcard1}", handler);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> restController.registerHandler(secondMethod, path + "/{wildcard2}", handler)
        );

        assertThat(exception.getMessage(), equalTo("Trying to use conflicting wildcard names for same path: wildcard1 and wildcard2"));
    }

    public void testRestHandlerWrapper() throws Exception {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        AtomicBoolean wrapperCalled = new AtomicBoolean(false);
        final RestHandler handler = (RestRequest request, RestChannel channel, NodeClient client) -> handlerCalled.set(true);
        final HttpServerTransport httpServerTransport = new TestHttpServerTransport();
        final RestController restController = new RestController(Collections.emptySet(), h -> {
            assertSame(handler, h);
            return (RestRequest request, RestChannel channel, NodeClient client) -> wrapperCalled.set(true);
        }, client, circuitBreakerService, usageService);
        restController.registerHandler(RestRequest.Method.GET, "/wrapped", handler);
        RestRequest request = testRestRequest("/wrapped", "{}", MediaTypeRegistry.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.BAD_REQUEST);
        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());
        httpServerTransport.start();
        assertTrue(wrapperCalled.get());
        assertFalse(handlerCalled.get());
    }

    public void testDispatchRequestAddsAndFreesBytesOnSuccess() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        RestRequest request = testRestRequest("/", content, MediaTypeRegistry.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.OK);

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnError() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        RestRequest request = testRestRequest("/error", content, MediaTypeRegistry.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.BAD_REQUEST);

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnlyOnceOnError() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        // we will produce an error in the rest handler and one more when sending the error response
        RestRequest request = testRestRequest("/error", content, MediaTypeRegistry.JSON);
        ExceptionThrowingChannel channel = new ExceptionThrowingChannel(request, true);

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestLimitsBytes() {
        int contentLength = BREAKER_LIMIT.bytesAsInt() + 1;
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        RestRequest request = testRestRequest("/", content, MediaTypeRegistry.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.TOO_MANY_REQUESTS);

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(1, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequiresContentTypeForRequestsWithContent() {
        String content = randomAlphaOfLength((int) Math.round(BREAKER_LIMIT.getBytes() / inFlightRequestsBreaker.getOverhead()));
        RestRequest request = testRestRequest("/", content, null);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.NOT_ACCEPTABLE);
        restController = new RestController(Collections.emptySet(), null, null, circuitBreakerService, usageService);
        restController.registerHandler(
            RestRequest.Method.GET,
            "/",
            (r, c, client) -> c.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY))
        );

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchDoesNotRequireContentTypeForRequestsWithoutContent() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchFailsWithPlainText() {
        String content = randomAlphaOfLength((int) Math.round(BREAKER_LIMIT.getBytes() / inFlightRequestsBreaker.getOverhead()));
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(content),
            null
        ).withPath("/foo").withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("text/plain"))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }
        });

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchUnsupportedContentType() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray("{}"), null)
            .withPath("/")
            .withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("application/x-www-form-urlencoded")))
            .build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWorksWithNewlineDelimitedJson() {
        final String mimeType = "application/x-ndjson";
        String content = randomAlphaOfLength((int) Math.round(BREAKER_LIMIT.getBytes() / inFlightRequestsBreaker.getOverhead()));
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(content),
            null
        ).withPath("/foo").withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList(mimeType))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWithContentStream() {
        final String mimeType = randomFrom("application/json", "application/smile");
        String content = randomAlphaOfLength((int) Math.round(BREAKER_LIMIT.getBytes() / inFlightRequestsBreaker.getOverhead()));
        final List<String> contentTypeHeader = Collections.singletonList(mimeType);
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(content),
            RestRequest.parseContentType(contentTypeHeader)
        ).withPath("/foo").withHeaders(Collections.singletonMap("Content-Type", contentTypeHeader)).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWithContentStreamNoContentType() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray("{}"), null)
            .withPath("/foo")
            .build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testNonStreamingXContentCausesErrorResponse() throws IOException {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            BytesReference.bytes(YamlXContent.contentBuilder().startObject().endObject()),
            XContentType.YAML
        ).withPath("/foo").build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });
        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testUnknownContentWithContentStream() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray("aaaabbbbb"),
            null
        ).withPath("/foo").withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("foo/bar"))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });
        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchBadRequest() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        restController.dispatchBadRequest(
            channel,
            client.threadPool().getThreadContext(),
            randomBoolean() ? new IllegalStateException("bad request") : new Throwable("bad request")
        );
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("bad request"));
    }

    public void testDispatchBadRequestUnknownCause() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        restController.dispatchBadRequest(channel, client.threadPool().getThreadContext(), null);
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("unknown cause"));
    }

    public void testFavicon() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/favicon.ico")
            .build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, false, RestStatus.OK);
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().contentType(), containsString("image/x-icon"));
    }

    public void testFaviconWithWrongHttpMethod() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(
            randomValueOtherThanMany(
                m -> m == RestRequest.Method.GET || m == RestRequest.Method.OPTIONS,
                () -> randomFrom(RestRequest.Method.values())
            )
        ).withPath("/favicon.ico").build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.METHOD_NOT_ALLOWED);
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().getHeaders().containsKey("Allow"), equalTo(true));
        assertThat(channel.getRestResponse().getHeaders().get("Allow"), hasItem(equalTo(RestRequest.Method.GET.toString())));
    }

    public void testHandleBadRequestWithHtmlSpecialCharsInUri() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath(
            "/<script>alert('xss');alert(\"&#x6A&#x61&#x76&#x61\");</script>"
        ).build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("invalid uri has been requested"));
    }

    public void testHandleBadInputWithCreateIndex() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withPath("/foo")
            .withMethod(RestRequest.Method.PUT)
            .withContent(new BytesArray("ddd"), MediaTypeRegistry.JSON)
            .build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        restController.registerHandler(RestRequest.Method.PUT, "/foo", new RestCreateIndexAction());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertEquals(
            channel.getRestResponse().content().utf8ToString(),
            "{\"error\":{\"root_cause\":[{\"type\":\"not_x_content_exception\",\"reason\":\"Compressor detection can only be called on some xcontent bytes or compressed xcontent bytes\"}],\"type\":\"not_x_content_exception\",\"reason\":\"Compressor detection can only be called on some xcontent bytes or compressed xcontent bytes\"},\"status\":400}"
        );
    }

    public void testDispatchUnsupportedHttpMethod() {
        final boolean hasContent = randomBoolean();
        final RestRequest request = RestRequest.request(xContentRegistry(), new HttpRequest() {
            @Override
            public RestRequest.Method method() {
                throw new IllegalArgumentException("test");
            }

            @Override
            public String uri() {
                return "/";
            }

            @Override
            public BytesReference content() {
                if (hasContent) {
                    return new BytesArray("test");
                }
                return BytesArray.EMPTY;
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                Map<String, List<String>> headers = new HashMap<>();
                if (hasContent) {
                    headers.put("Content-Type", Collections.singletonList("text/plain"));
                }
                return headers;
            }

            @Override
            public List<String> strictCookies() {
                return null;
            }

            @Override
            public HttpVersion protocolVersion() {
                return randomFrom(HttpVersion.values());
            }

            @Override
            public HttpRequest removeHeader(String header) {
                return this;
            }

            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                return null;
            }

            @Override
            public void release() {}

            @Override
            public HttpRequest releaseAndCopy() {
                return this;
            }

            @Override
            public Exception getInboundException() {
                return null;
            }
        }, null);

        final AssertingChannel channel = new AssertingChannel(request, true, RestStatus.METHOD_NOT_ALLOWED);
        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().getHeaders().containsKey("Allow"), equalTo(true));
        assertThat(channel.getRestResponse().getHeaders().get("Allow"), hasItem(equalTo(RestRequest.Method.GET.toString())));
        assertThat(
            channel.getRestResponse().content().utf8ToString(),
            equalTo("{\"error\":\"Unexpected HTTP method, allowed: [GET]\",\"status\":405}")
        );
    }

    private static final class TestHttpServerTransport extends AbstractLifecycleComponent implements HttpServerTransport {

        TestHttpServerTransport() {}

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() {}

        @Override
        public BoundTransportAddress boundAddress() {
            TransportAddress transportAddress = buildNewFakeTransportAddress();
            return new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress);
        }

        @Override
        public HttpInfo info() {
            return null;
        }

        @Override
        public HttpStats stats() {
            return null;
        }
    }

    public static final class AssertingChannel extends AbstractRestChannel {

        private final RestStatus expectedStatus;
        private final AtomicReference<RestResponse> responseReference = new AtomicReference<>();

        public AssertingChannel(RestRequest request, boolean detailedErrorsEnabled, RestStatus expectedStatus) {
            super(request, detailedErrorsEnabled);
            this.expectedStatus = expectedStatus;
        }

        @Override
        public void sendResponse(RestResponse response) {
            assertEquals(expectedStatus, response.status());
            responseReference.set(response);
        }

        RestResponse getRestResponse() {
            return responseReference.get();
        }

        boolean getSendResponseCalled() {
            return getRestResponse() != null;
        }

    }

    private static final class ExceptionThrowingChannel extends AbstractRestChannel {

        protected ExceptionThrowingChannel(RestRequest request, boolean detailedErrorsEnabled) {
            super(request, detailedErrorsEnabled);
        }

        @Override
        public void sendResponse(RestResponse response) {
            throw new IllegalStateException("always throwing an exception for testing");
        }
    }

    private static RestRequest testRestRequest(String path, String content, MediaType mediaType) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        builder.withPath(path);
        builder.withContent(new BytesArray(content), mediaType);
        return builder.build();
    }
}
