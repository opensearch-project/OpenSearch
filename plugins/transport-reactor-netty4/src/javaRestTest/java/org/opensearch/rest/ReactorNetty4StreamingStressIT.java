/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.apache.http.ConnectionClosedException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.StreamingRequest;
import org.opensearch.client.StreamingResponse;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.After;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.hamcrest.CoreMatchers.equalTo;

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
        final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create(true);

        final AtomicInteger id = new AtomicInteger(0);
        final Stream<String> stream = Stream.generate(
            () -> "{ \"index\": { \"_index\": \"test-stress-streaming\", \"_id\": \""
                + id.incrementAndGet()
                + "\" } }\n"
                + "{ \"name\": \"josh\"  }\n"
        );

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
            .then(() -> {
                try {
                    client().close();
                } catch (final IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            })
            .then(() -> scheduler.advanceTimeBy(delay))
            .expectErrorMatches(t -> t instanceof InterruptedIOException || t instanceof ConnectionClosedException)
            .verify();
    }
}
