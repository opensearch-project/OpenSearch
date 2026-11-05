/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.http.ContentType;
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
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.hamcrest.CoreMatchers.equalTo;
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

    public void testMissingIndexErrorDelimiterAndCompletion() throws Exception {
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        assertStreamingResponse(
            Mono.just("{\"index\":{}}\n{\"hello\":\"world\"}\n"),
            body::writeBytes,
            status -> assertTrue(status == 200 || status == 400)
        );
        final String content = body.toString(StandardCharsets.UTF_8);
        assertTrue(content, content.endsWith("\r\n"));
        assertTrue(content, content.contains("index is missing"));
        assertTrue(content, content.contains("\"status\":400"));
    }

    public void testErrorAfterSuccessDelimiterAndCompletion() throws Exception {
        final Sinks.One<String> nextRequest = Sinks.one();
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        final AtomicBoolean sentError = new AtomicBoolean();
        assertStreamingResponse(
            Flux.concat(Mono.just("{\"index\":{\"_index\":\"test-streaming\"}}\n{\"hello\":\"world\"}\n"), nextRequest.asMono()),
            bytes -> {
                body.writeBytes(bytes);
                if (body.toString(StandardCharsets.UTF_8).contains("\r\n") && sentError.compareAndSet(false, true)) {
                    assertEquals(Sinks.EmitResult.OK, nextRequest.tryEmitValue("{\"index\":{}}\n{\"hello\":\"world\"}\n"));
                }
            },
            status -> assertEquals(200, (int) status)
        );
        final String content = body.toString(StandardCharsets.UTF_8);
        final String[] records = content.split("\r\n", -1);
        assertEquals(content, 3, records.length);
        assertTrue(records[0], records[0].contains("\"result\":\"created\""));
        assertTrue(records[1], records[1].contains("index is missing"));
        assertTrue(records[1], records[1].contains("\"status\":400"));
        assertEquals("", records[2]);
    }

    private void assertStreamingResponse(Publisher<String> input, Consumer<byte[]> consume, Consumer<Integer> checkStatus)
        throws Exception {
        try (CloseableHttpAsyncClient client = HttpAsyncClients.createDefault()) {
            client.start();
            final ReactiveResponseConsumer consumer = new ReactiveResponseConsumer();
            final BasicRequestProducer producer = new BasicRequestProducer(
                "POST",
                getClusterHosts().get(0),
                "/_bulk/stream",
                new ReactiveEntityProducer(Flux.from(input).map(StandardCharsets.UTF_8::encode), -1, ContentType.APPLICATION_JSON, null)
            );
            final var exchange = client.execute(producer, consumer, null);
            final var response = consumer.getResponseFuture().get(30, TimeUnit.SECONDS);
            checkStatus.accept(response.getHead().getCode());
            StepVerifier.create(Flux.from(response.getBody()).doOnNext(buffer -> {
                final byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                consume.accept(bytes);
            }).then()).expectComplete().verify(Duration.ofSeconds(30));
            exchange.get(30, TimeUnit.SECONDS);
        }
    }

    public void testNonStreamingBulkErrorHasNoChunkDelimiter() throws IOException {
        final Request request = new Request("POST", "/_bulk");
        request.setJsonEntity("{\"index\":{}}\n{\"hello\":\"world\"}\n");
        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        try (var input = exception.getResponse().getEntity().getContent()) {
            final String content = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(content, content.contains("index is missing"));
            assertFalse(content, content.endsWith("\n"));
        }
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
