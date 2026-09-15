/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCounted;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Checks that {@code RestController} does not leak {@code in_flight_requests} circuit breaker reservations for
 * requests it cannot dispatch.
 *
 * <p>{@code RestBulkStreamingAction} is registered on every node, but streaming only works on a transport that
 * provides a {@code StreamingRestChannel} (the {@code transport-reactor-netty4} plugin). On the default netty4
 * transport, {@code RestController} used to reserve the request's {@code contentLength} on the breaker and only
 * then discover that the channel cannot stream, failing over the raw channel which owns no reservation. Every
 * request to {@code /_bulk/stream} therefore leaked its full content length until the node was restarted,
 * eventually rejecting all traffic with a 429 {@code circuit_breaking_exception}.
 *
 * <p>A single node "cluster" is used because {@code in_flight_requests} is shared with the transport layer.
 */
@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numClientNodes = 0, numDataNodes = 1)
public class Netty4HttpRequestBreakerIT extends OpenSearchNetty4IntegTestCase {

    private static final int REQUEST_COUNT = 5;

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testBreakerIsNotLeakedWhenStreamingIsUnsupported() throws Exception {
        ensureGreen();

        final CircuitBreaker breaker = internalCluster().getInstance(CircuitBreakerService.class)
            .getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        // The breaker is shared with the transport layer, so let it quiesce before measuring anything.
        assertBusy(() -> assertThat("breaker did not quiesce before the test", breaker.getUsed(), equalTo(0L)));

        final StringBuilder body = new StringBuilder();
        body.append("{\"index\":{\"_index\":\"test\"}}").append(System.lineSeparator());
        body.append("{\"field\":\"").append("x".repeat(8192)).append("\"}").append(System.lineSeparator());

        final List<Tuple<String, CharSequence>> requests = new ArrayList<>();
        for (int i = 0; i < REQUEST_COUNT; i++) {
            requests.add(Tuple.tuple("/_bulk/stream", body));
        }

        final HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        final TransportAddress transportAddress = randomFrom(httpServerTransport.boundAddress().boundAddresses());

        try (Netty4HttpClient nettyHttpClient = Netty4HttpClient.http()) {
            final Collection<FullHttpResponse> responses = nettyHttpClient.post(transportAddress.address(), requests);
            try {
                assertThat(responses, hasSize(REQUEST_COUNT));
                for (FullHttpResponse response : responses) {
                    assertThat(
                        "streaming is unsupported on the netty4 transport, so every request must fail",
                        response.status(),
                        equalTo(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    );
                }
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }

        // Every reservation must have been returned. Before the fix the breaker stayed at REQUEST_COUNT * body length.
        assertBusy(() -> assertThat("in_flight_requests reservation leaked", breaker.getUsed(), equalTo(0L)));
    }
}
