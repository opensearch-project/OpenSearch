/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCounted;
import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4Http2PostIT extends OpenSearchNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testThatNettyHttpServerSupportsHttp2() throws Exception {
        final List<Tuple<String, CharSequence>> requests = List.of(Tuple.tuple("/_search", "{\"query\":{ \"match_all\":{}}}"));

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        try (Netty4HttpClient nettyHttpClient = Netty4HttpClient.http2()) {
            Collection<FullHttpResponse> responses = nettyHttpClient.post(transportAddress.address(), requests);
            try {
                assertThat(responses, hasSize(1));

                for (FullHttpResponse response : responses) {
                    assertThat(response.getStatus(), equalTo(HttpResponseStatus.OK));
                }

                Collection<String> opaqueIds = Netty4HttpClient.returnOpaqueIds(responses);
                assertOpaqueIdsInAnyOrder(opaqueIds);
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

    private void assertOpaqueIdsInAnyOrder(Collection<String> opaqueIds) {
        // check if opaque ids are present in any order, since for HTTP/2 we use streaming (no head of line blocking)
        // and responses may come back at any order
        int i = 0;
        String msg = String.format(Locale.ROOT, "Expected list of opaque ids to be in any order, got [%s]", opaqueIds);
        assertThat(msg, opaqueIds, containsInAnyOrder(IntStream.range(0, 1).mapToObj(Integer::toString).toArray()));
    }

}
