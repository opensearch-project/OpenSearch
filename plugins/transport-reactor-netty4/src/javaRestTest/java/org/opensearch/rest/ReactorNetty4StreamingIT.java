/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.message.BasicHttpRequest;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.reactive.ReactiveEntityProducer;
import org.apache.hc.core5.reactive.ReactiveResponseConsumer;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.StreamingRequest;
import org.opensearch.client.StreamingResponse;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;
import org.junit.After;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class ReactorNetty4StreamingIT extends OpenSearchRestTestCase {
    @After
    @Override
    public void tearDown() throws Exception {
        final Request request = new Request("DELETE", "/test-streaming");
        request.addParameter("ignore_unavailable", "true");

        final Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        super.tearDown();
    }

    public void testStreamingRequestNoBatching() throws IOException {
        final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create(true);

        final Stream<String> stream = IntStream.range(1, 6).mapToObj(id -> String.format(Locale.ENGLISH, """
            { "index": { "_index": "test-streaming", "_id": "%d" } }
            { "name": "josh" }
            """, id));

        final Duration delay = Duration.ofMillis(1);
        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).delayElements(delay, scheduler).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "true");

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);
        scheduler.advanceTimeBy(delay); /* emit first element */

        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"1\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"2\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"3\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"4\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"5\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());

        final Request request = new Request("GET", "/test-streaming/_count");
        final Response response = client().performRequest(request);
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Integer count = objectPath.evaluate("count");
        assertThat(count, equalTo(5));
    }

    public void testStreamingRequestOpaqueId() throws IOException {
        final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create(true);

        final Stream<String> stream = IntStream.range(1, 6).mapToObj(id -> String.format(Locale.ENGLISH, """
            { "index": { "_index": "test-streaming", "_id": "%d" } }
            { "name": "josh" }
            """, id));

        final RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("X-Opaque-Id", "1").build();

        final Duration delay = Duration.ofMillis(1);
        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).delayElements(delay, scheduler).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "true");
        streamingRequest.setOptions(options);

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);
        scheduler.advanceTimeBy(delay); /* emit first element */

        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"1\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"2\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"3\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"4\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"5\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getHeader("X-Opaque-Id"), equalTo("1"));
        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());

        final Request request = new Request("GET", "/test-streaming/_count");
        final Response response = client().performRequest(request);
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Integer count = objectPath.evaluate("count");
        assertThat(count, equalTo(5));
    }

    public void testStreamingRequestOpaqueIdTwice() throws IOException {
        final Stream<String> stream = IntStream.range(1, 6).mapToObj(id -> String.format(Locale.ENGLISH, """
            { "index": { "_index": "test-streaming", "_id": "%d" } }
            { "name": "josh" }
            """, id));

        final RequestOptions options = RequestOptions.DEFAULT.toBuilder()
            .addHeader("X-Opaque-Id", "1")
            .addHeader("X-Opaque-Id", "2")
            .build();

        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "true");
        streamingRequest.setOptions(options);

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);

        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectError(ResponseException.class)
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(400));
        assertThat(streamingResponse.getWarnings(), empty());
    }

    public void testStreamingRequestOneBatchBySize() throws IOException, InterruptedException {
        final Stream<String> stream = IntStream.range(1, 6).mapToObj(id -> String.format(Locale.ENGLISH, """
            { "index": { "_index": "test-streaming", "_id": "%d" } }
            { "name": "josh" }
            """, id));

        final Duration delay = Duration.ofMillis(1);
        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).delayElements(delay).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "true");
        streamingRequest.addParameter("batch_size", "5");

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);

        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(
                s -> s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"1\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"2\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"3\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"4\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"5\"")
            )
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());

        final Request request = new Request("GET", "/test-streaming/_count");
        final Response response = client().performRequest(request);
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Integer count = objectPath.evaluate("count");
        assertThat(count, equalTo(5));
    }

    public void testStreamingRequestManyBatchesBySize() throws IOException {
        final Stream<String> stream = IntStream.range(1, 6).mapToObj(id -> String.format(Locale.ENGLISH, """
            { "index": { "_index": "test-streaming", "_id": "%d" } }
            { "name": "josh" }
            """, id));

        final Duration delay = Duration.ofMillis(1);
        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).delayElements(delay).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "true");
        streamingRequest.addParameter("batch_size", "3");

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);

        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(
                s -> s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"1\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"2\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"3\"")
            )
            .expectNextMatches(
                s -> s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"4\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"5\"")
            )
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());

        final Request request = new Request("GET", "/test-streaming/_count");
        final Response response = client().performRequest(request);
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Integer count = objectPath.evaluate("count");
        assertThat(count, equalTo(5));
    }

    public void testStreamingRequestManyBatchesByInterval() throws IOException {
        final Stream<String> stream = IntStream.range(1, 6).mapToObj(id -> String.format(Locale.ENGLISH, """
            { "index": { "_index": "test-streaming", "_id": "%d" } }
            { "name": "josh" }
            """, id));

        final Duration delay = Duration.ofMillis(500);
        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).delayElements(delay).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "true");
        streamingRequest.addParameter("batch_interval", "5s");

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);

        // We don't check for a other documents here since those may appear in any of the chunks (it is very
        // difficult to get the timing right). But at the end, the total number of the documents is being checked.
        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(
                s -> s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"1\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"2\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"3\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"4\"")
                    && s.contains("\"result\":\"created\"")
                    && s.contains("\"_id\":\"5\"")
            )
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());

        final Request request = new Request("GET", "/test-streaming/_count");
        final Response response = client().performRequest(request);
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Integer count = objectPath.evaluate("count");
        assertThat(count, equalTo(5));
    }

    public void testStreamingRequestManyBatchesByIntervalAndSize() throws IOException {
        final Stream<String> stream = IntStream.range(1, 6).mapToObj(id -> String.format(Locale.ENGLISH, """
            { "index": { "_index": "test-streaming", "_id": "%d" } }
            { "name": "josh" }
            """, id));

        final Duration delay = Duration.ofSeconds(1);
        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).delayElements(delay).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "true");
        streamingRequest.addParameter("batch_interval", "3s");
        streamingRequest.addParameter("batch_size", "5");

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);

        // We don't check for a other documents here since those may appear in any of the chunks (it is very
        // difficult to get the timing right). But at the end, the total number of the documents is being checked.
        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"1\""))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"5\""))
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());

        final Request request = new Request("GET", "/test-streaming/_count");
        final Response response = client().performRequest(request);
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Integer count = objectPath.evaluate("count");
        assertThat(count, equalTo(5));
    }

    public void testStreamingBadRequest() throws IOException {
        final Stream<String> stream = Stream.of("""
            { "index": { "_index": "test-streaming", "_id": "1" } }
            { "name": "josh" }
            """);

        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "not-supported-policy");

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);
        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectErrorMatches(
                ex -> ex instanceof ResponseException && ((ResponseException) ex).getResponse().getStatusLine().getStatusCode() == 400
            )
            .verify(Duration.ofSeconds(10));
        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(400));
        assertThat(streamingResponse.getWarnings(), empty());
    }

    public void testStreamingBadStream() throws IOException {
        final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create(true);

        final Stream<String> stream = Stream.of("""
            { "index": { "_index": "test-streaming", "_id": "1" } }
            { "name": "josh" }
            """, """
            { "name": "josh" }
            """);

        final Duration delay = Duration.ofMillis(1);
        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).delayElements(delay, scheduler).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);
        scheduler.advanceTimeBy(delay); /* emit first element */

        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"1\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectNextMatches(s -> s.contains("\"type\":\"illegal_argument_exception\""))
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());
    }

    public void testStreamingErrorAfterSuccessHasDelimiter() throws Exception {
        assertTerminalError(true);
    }

    public void testStreamingFirstErrorIsCompleteResponse() throws Exception {
        assertTerminalError(false);
    }

    private void assertTerminalError(boolean afterSuccess) throws Exception {
        final Sinks.One<String> firstRequest = Sinks.one();
        final Sinks.One<String> invalidRequest = Sinks.one();
        final String invalid = "{\"index\":{}}\n{}\n";
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        final AtomicBoolean errorSent = new AtomicBoolean();
        try (var client = HttpAsyncClients.createDefault()) {
            client.start();
            final ReactiveResponseConsumer consumer = new ReactiveResponseConsumer();
            final var input = afterSuccess ? Flux.concat(firstRequest.asMono(), invalidRequest.asMono()) : Flux.from(firstRequest.asMono());
            final BasicHttpRequest request = new BasicHttpRequest("POST", getClusterHosts().get(0), "/_bulk/stream");
            final BasicRequestProducer producer = new BasicRequestProducer(
                request,
                new ReactiveEntityProducer(input.map(StandardCharsets.UTF_8::encode), -1, ContentType.APPLICATION_JSON, null)
            );
            final var exchange = client.execute(producer, consumer, null);
            final var response = consumer.getResponseFuture().get(30, TimeUnit.SECONDS);
            // Headers can be committed before any request data or successful response chunk.
            assertEquals(200, response.getHead().getCode());
            assertEquals("application/json", ContentType.parse(response.getHead().getFirstHeader("Content-Type").getValue()).getMimeType());
            assertEquals(
                Sinks.EmitResult.OK,
                firstRequest.tryEmitValue(afterSuccess ? "{\"index\":{\"_index\":\"test-streaming\"}}\n{}\n" : invalid)
            );
            StepVerifier.create(Flux.from(response.getBody()).doOnNext(buffer -> {
                final byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                body.writeBytes(bytes);
                if (afterSuccess && body.toString(StandardCharsets.UTF_8).contains("\r\n") && errorSent.compareAndSet(false, true)) {
                    assertEquals(Sinks.EmitResult.OK, invalidRequest.tryEmitValue(invalid));
                }
            }).then()).expectComplete().verify(Duration.ofSeconds(30));
            exchange.get(30, TimeUnit.SECONDS);
        }
        final String content = body.toString(StandardCharsets.UTF_8);
        assertThat(content, containsString("index is missing"));
        if (afterSuccess) {
            final String[] records = content.split("\r\n", -1);
            assertEquals(content, 3, records.length);
            assertThat(records[0], containsString("created"));
            assertThat(records[1], containsString("index is missing"));
            assertEquals("", records[2]);
        } else {
            assertFalse(content, content.endsWith("\n"));
        }
    }

    public void testStreamingLargeDocument() throws IOException {
        final Stream<String> stream = Stream.of(String.format(Locale.getDefault(), """
            { "index": { "_index": "test-streaming", "_id": "1" } }
            { "name": "%s" }
            """, randomAlphaOfLength(7000)));

        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);

        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(s -> s.contains("\"result\":\"created\"") && s.contains("\"_id\":\"1\""))
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());
    }

    public void testStreamingLargeDocumentThatExceedsChunkSize() throws IOException {
        final Stream<String> stream = Stream.of(
            String.format(
                Locale.getDefault(),
                """
                    { "index": { "_index": "test-streaming", "_id": "1" } }
                    { "name": "%s" }
                    """,
                randomAlphaOfLength(9000) /* the default chunk size limit is set 8k */
            )
        );

        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);

        StepVerifier.create(Flux.from(streamingResponse.getBody()).map(b -> new String(b.array(), StandardCharsets.UTF_8)))
            .expectNextMatches(s -> s.contains("\"type\":\"illegal_argument_exception\""))
            .expectComplete()
            .verify();

        assertThat(streamingResponse.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(streamingResponse.getWarnings(), empty());
    }
}
