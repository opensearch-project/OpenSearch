/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.OSFlightClient;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streams more data than gRPC's 1 MiB initial window through a client built with a fixed
 * {@code arrow.flight.client.flow_control_window}, so the stream depends on the configured window
 * being granted and replenished.
 */
public class FlightClientFlowControlWindowTests extends FlightTransportTestBase {

    private static final int BATCHES = 64;
    private static final int BATCH_CHARS = 64 * 1024;

    @Override
    protected Settings additionalSettings() {
        return Settings.builder()
            .put(ServerConfig.FLIGHT_CLIENT_FLOW_CONTROL_WINDOW.getKey(), new ByteSizeValue(16, ByteSizeUnit.MB))
            .build();
    }

    public void testStreamWithFixedFlowControlWindow() throws Exception {
        String action = "internal:test/flow-control-window";
        String payload = "x".repeat(BATCH_CHARS);
        streamTransportService.registerRequestHandler(action, ThreadPool.Names.SAME, TestRequest::new, (request, channel, task) -> {
            for (int i = 0; i < BATCHES; i++) {
                channel.sendResponseBatch(new TestResponse(payload));
            }
            channel.completeStream();
        });

        CountDownLatch done = new CountDownLatch(1);
        AtomicInteger received = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        streamTransportService.sendRequest(
            remoteNode,
            action,
            new TestRequest(),
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new StreamTransportResponseHandler<TestResponse>() {
                @Override
                public void handleStreamResponse(StreamTransportResponse<TestResponse> streamResponse) {
                    try {
                        TestResponse response;
                        while ((response = streamResponse.nextResponse()) != null) {
                            assertEquals(BATCH_CHARS, response.getData().length());
                            received.incrementAndGet();
                        }
                    } catch (Exception e) {
                        failure.set(e);
                    } finally {
                        try {
                            streamResponse.close();
                        } catch (Exception ignored) {}
                        done.countDown();
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    failure.set(exp);
                    done.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public TestResponse read(StreamInput in) throws IOException {
                    return new TestResponse(in);
                }
            }
        );

        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(BATCHES, received.get());
    }

    public void testBuilderRejectsNegativeWindow() {
        expectThrows(IllegalArgumentException.class, () -> OSFlightClient.builder().flowControlWindow(-1));
    }
}
