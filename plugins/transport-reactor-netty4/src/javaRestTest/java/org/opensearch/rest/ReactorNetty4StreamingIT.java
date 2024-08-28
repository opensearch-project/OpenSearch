/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.StreamingRequest;
import org.opensearch.client.StreamingResponse;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;
import org.junit.After;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;
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

    public void testStreamingRequestNoBatching() throws IOException {
        final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create(true);

        final Stream<String> stream = IntStream.range(1, 6)
            .mapToObj(id -> "{ \"index\": { \"_index\": \"test-streaming\", \"_id\": \"" + id + "\" } }\n" + "{ \"name\": \"josh\" }\n");

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

    public void testStreamingRequestOneBatchBySize() throws IOException, InterruptedException {
        final Stream<String> stream = IntStream.range(1, 6)
            .mapToObj(id -> "{ \"index\": { \"_index\": \"test-streaming\", \"_id\": \"" + id + "\" } }\n" + "{ \"name\": \"josh\" }\n");

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
        final Stream<String> stream = IntStream.range(1, 6)
            .mapToObj(id -> "{ \"index\": { \"_index\": \"test-streaming\", \"_id\": \"" + id + "\" } }\n" + "{ \"name\": \"josh\" }\n");

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
        final Stream<String> stream = IntStream.range(1, 6)
            .mapToObj(id -> "{ \"index\": { \"_index\": \"test-streaming\", \"_id\": \"" + id + "\" } }\n" + "{ \"name\": \"josh\" }\n");

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
        final Stream<String> stream = IntStream.range(1, 6)
            .mapToObj(id -> "{ \"index\": { \"_index\": \"test-streaming\", \"_id\": \"" + id + "\" } }\n" + "{ \"name\": \"josh\" }\n");

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
        final Stream<String> stream = Stream.of(
            "{ \"index\": { \"_index\": \"test-streaming\", \"_id\": \"1\" } }\n" + "{ \"name\": \"josh\"  }\n"
        );

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

        final Stream<String> stream = Stream.of(
            "{ \"index\": { \"_index\": \"test-streaming\", \"_id\": \"1\" } }\n" + "{ \"name\": \"josh\"  }\n",
            "{ \"name\": \"josh\"  }\n"
        );

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
}
