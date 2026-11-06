/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.document;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.http.HttpChunk;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.mockito.ArgumentCaptor;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RestBulkStreamingAction}.
 */
public class RestBulkStreamingActionTests extends OpenSearchTestCase {
    public void testBulkStreamingPipelineUpsert() throws Exception {
        SetOnce<Boolean> bulkCalled = new SetOnce<>();
        try (NodeClient verifyingClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
                bulkCalled.set(true);
            }
        }) {
            final Map<String, String> params = new HashMap<>();
            params.put("pipeline", "timestamps");

            final FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("my_index/_bulk/streaming")
                .withParams(params)
                .withContent(
                    new BytesArray(
                        "{\"index\":{\"_id\":\"1\"}}\n"
                            + "{\"field1\":\"val1\"}\n"
                            + "{\"update\":{\"_id\":\"2\"}}\n"
                            + "{\"script\":{\"source\":\"ctx._source.counter++;\"},\"upsert\":{\"field1\":\"upserted_val\"}}\n"
                    ),
                    MediaTypeRegistry.JSON
                )
                .withMethod(RestRequest.Method.POST)
                .build();
            request.param("error_trace", "false");
            request.param("rest.exception.stacktrace.skip", "false");

            final RestChannel channel = mock(RestChannel.class);
            when(channel.request()).thenReturn(request);
            when(channel.newErrorBuilder()).thenReturn(XContentType.YAML.contentBuilder());
            when(channel.detailedErrorsEnabled()).thenReturn(true);

            new RestBulkStreamingAction(settings(Version.CURRENT).build()).handleRequest(request, channel, verifyingClient);

            final ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.captor();
            verify(channel).sendResponse(responseCaptor.capture());

            // We do not expect `bulk` action to be called since the default HTTP transport (netty4) does not support streaming
            assertThat(bulkCalled.get(), equalTo(null));
            assertThat(responseCaptor.getValue().status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(
                responseCaptor.getValue().content().utf8ToString(),
                containsString("Unable to initiate request / response streaming")
            );
        }
    }

    public void testMissingIndexErrorHasChunkDelimiter() throws Exception {
        final TestStreamingChannel channel = streamingChannel(Map.of(), "{\"index\":{}}\n{\"field\":\"value\"}\n");
        execute(channel, (request, listener) -> listener.onFailure(request.validate()));
        assertEquals(1, channel.responses.size());
        final RestResponse response = channel.responses.get(0);
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertThat(response.content().utf8ToString(), containsString("index is missing"));
        assertTrue(response.content().utf8ToString().endsWith("\r\n"));
        assertTrue(channel.chunks.isEmpty());
    }

    public void testErrorAfterSuccessfulChunkHasChunkDelimiter() throws Exception {
        final TestStreamingChannel channel = streamingChannel(
            Map.of(),
            "{\"index\":{\"_index\":\"test\"}}\n{\"field\":\"value\"}\n",
            "{\"index\":{}}\n{\"field\":\"value\"}\n"
        );
        execute(channel, (request, listener) -> {
            final ActionRequestValidationException error = request.validate();
            if (error == null) {
                listener.onResponse(new BulkResponse(new BulkItemResponse[0], 1));
            } else {
                assertEquals(1, channel.chunks.size());
                listener.onFailure(error);
            }
        });
        assertEquals(List.of("{\"took\":1,\"errors\":false,\"items\":[]}\r\n"), channel.chunks);
        assertEquals(1, channel.responses.size());
        assertEquals(RestStatus.BAD_REQUEST, channel.responses.get(0).status());
        assertTrue(channel.responses.get(0).content().utf8ToString().endsWith("\r\n"));
        assertEquals(0, channel.lastChunks);
    }

    public void testMalformedBulkErrorHasChunkDelimiter() throws Exception {
        final TestStreamingChannel channel = streamingChannel(Map.of(), "{\"invalid\":{}}\n");
        execute(channel, (request, listener) -> fail("Malformed metadata must not reach bulk execution"));
        assertEquals(1, channel.responses.size());
        assertEquals(RestStatus.BAD_REQUEST, channel.responses.get(0).status());
        assertThat(channel.responses.get(0).content().utf8ToString(), containsString("illegal_argument_exception"));
        assertTrue(channel.responses.get(0).content().utf8ToString().endsWith("\r\n"));
    }

    public void testStreamingErrorPreservesResponseMetadata() throws Exception {
        for (Map<String, String> params : List.of(
            Map.<String, String>of(),
            Map.of("pretty", "true"),
            Map.of("error_trace", "true"),
            Map.of("format", "yaml")
        )) {
            final OpenSearchException error = new OpenSearchException("streaming failure");
            error.addHeader("test-header", "first", "second");
            final TestStreamingChannel channel = streamingChannel(params, "{\"index\":{\"_index\":\"test\"}}\n{}\n");
            final BytesRestResponse expected = new BytesRestResponse(new FakeRestChannel(channel.request(), true, 1), error);
            execute(channel, (request, listener) -> listener.onFailure(error));
            assertEquals(1, channel.responses.size());
            final RestResponse response = channel.responses.get(0);
            assertEquals(expected.status(), response.status());
            assertEquals(expected.contentType(), response.contentType());
            assertEquals(expected.getHeaders(), response.getHeaders());
            assertEquals(expected.content().utf8ToString() + "\r\n", response.content().utf8ToString());
        }
    }

    public void testSuccessfulStreamHasChunkDelimiterAndLastChunk() throws Exception {
        final TestStreamingChannel channel = streamingChannel(Map.of(), "{\"index\":{\"_index\":\"test\"}}\n{}\n");
        execute(channel, (request, listener) -> listener.onResponse(new BulkResponse(new BulkItemResponse[0], 1)));
        assertEquals(List.of("{\"took\":1,\"errors\":false,\"items\":[]}\r\n"), channel.chunks);
        assertEquals(1, channel.lastChunks);
        assertTrue(channel.responses.isEmpty());
    }

    public void testNonStreamingErrorIsUnchanged() throws Exception {
        final RestRequest request = streamingChannel(Map.of()).request();
        final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        final ActionRequestValidationException error = new ActionRequestValidationException();
        error.addValidationError("Unable to initiate request / response streaming over non-streaming channel");
        final BytesRestResponse expected = new BytesRestResponse(new FakeRestChannel(request, true, 1), error);
        try (NodeClient client = new NoOpNodeClient(getTestName())) {
            new RestBulkStreamingAction(Settings.EMPTY).handleRequest(request, channel, client);
        }
        assertEquals(expected.status(), channel.capturedResponse().status());
        assertEquals(expected.content(), channel.capturedResponse().content());
    }

    private TestStreamingChannel streamingChannel(Map<String, String> params, String... bodies) {
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_bulk/stream")
            .withMethod(RestRequest.Method.POST)
            .withParams(new HashMap<>(params))
            .withContent(BytesArray.EMPTY, MediaTypeRegistry.JSON)
            .build();
        final List<HttpChunk> chunks = new ArrayList<>();
        for (int i = 0; i < bodies.length; i++) {
            final HttpChunk chunk = mock(HttpChunk.class);
            when(chunk.content()).thenReturn(new BytesArray(bodies[i]));
            when(chunk.isLast()).thenReturn(i == bodies.length - 1);
            chunks.add(chunk);
        }
        return new TestStreamingChannel(request, Flux.fromIterable(chunks));
    }

    private void execute(TestStreamingChannel channel, BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk) throws Exception {
        try (NodeClient client = new NoOpNodeClient(getTestName()) {
            @Override
            public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
                bulk.accept(request, listener);
            }
        }) {
            new RestBulkStreamingAction(Settings.EMPTY).handleRequest(channel.request(), channel, client);
        }
    }

    private static class TestStreamingChannel extends AbstractRestChannel implements StreamingRestChannel {
        private final Flux<HttpChunk> input;
        private final List<String> chunks = new ArrayList<>();
        private final List<RestResponse> responses = new ArrayList<>();
        private int lastChunks;

        TestStreamingChannel(RestRequest request, Flux<HttpChunk> input) {
            super(request, true);
            this.input = input;
        }

        @Override
        public void subscribe(Subscriber<? super HttpChunk> subscriber) {
            input.subscribe(subscriber);
        }

        @Override
        public void sendChunk(HttpChunk chunk) {
            try (chunk) {
                if (chunk.isLast()) {
                    lastChunks++;
                } else {
                    chunks.add(chunk.content().utf8ToString());
                }
            }
        }

        @Override
        public void sendResponse(RestResponse response) {
            final BytesRestResponse copy = new BytesRestResponse(
                response.status(),
                response.contentType(),
                BytesReference.toBytes(response.content())
            );
            response.getHeaders().forEach((name, values) -> values.forEach(value -> copy.addHeader(name, value)));
            responses.add(copy);
        }

        @Override
        public void prepareResponse(RestStatus status, Map<String, List<String>> headers) {}

        @Override
        public boolean isReadable() {
            return true;
        }

        @Override
        public boolean isWritable() {
            return true;
        }
    }

}
