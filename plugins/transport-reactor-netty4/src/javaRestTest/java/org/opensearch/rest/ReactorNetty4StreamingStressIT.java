/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.apache.hc.core5.http.ConnectionClosedException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.StreamingRequest;
import org.opensearch.client.StreamingResponse;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.After;

import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;
import reactor.test.subscriber.TestSubscriber;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class ReactorNetty4StreamingStressIT extends OpenSearchRestTestCase {
    @After
    @Override
    public void tearDown() throws Exception {
        final Request request = new Request("DELETE", "/test-stress-streaming");
        request.addParameter("ignore_unavailable", "true");

        final Response response = adminClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        super.tearDown();
    }

    public void testCloseClientStreamingRequest() throws Exception {
        final AtomicInteger id = new AtomicInteger(0);
        final Stream<String> stream = Stream.generate(
            () -> "{ \"index\": { \"_index\": \"test-stress-streaming\", \"_id\": \""
                + id.incrementAndGet()
                + "\" } }\n"
                + "{ \"name\": \"josh\"  }\n"
        );

        final StreamingRequest<ByteBuffer> streamingRequest = new StreamingRequest<>(
            "POST",
            "/_bulk/stream",
            Flux.fromStream(stream).delayElements(Duration.ofMillis(500)).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        );
        streamingRequest.addParameter("refresh", "true");

        final StreamingResponse<ByteBuffer> streamingResponse = client().streamRequest(streamingRequest);
        TestSubscriber<ByteBuffer> subscriber = TestSubscriber.create();
        streamingResponse.getBody().subscribe(subscriber);

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            // Await for subscriber to receive at least one chunk
            assertBusy(() -> assertThat(subscriber.getReceivedOnNext(), not(empty())));

            // Close client forceably
            executor.schedule(() -> {
                client().close();
                return null;
            }, 2, TimeUnit.SECONDS);

            // Await for subscriber to terminate
            subscriber.block(Duration.ofSeconds(10));
            assertThat(
                subscriber.expectTerminalError(),
                anyOf(instanceOf(InterruptedIOException.class), instanceOf(ConnectionClosedException.class))
            );
        } finally {
            executor.shutdown();
            if (executor.awaitTermination(1, TimeUnit.SECONDS) == false) {
                executor.shutdownNow();
            }
        }
    }
}
