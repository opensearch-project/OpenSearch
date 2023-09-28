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
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCounted;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import io.netty.handler.codec.http2.HttpConversionUtil;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4Http2IT extends OpenSearchNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testThatNettyHttpServerSupportsHttp2GetUpgrades() throws Exception {
        String[] requests = new String[] { "/", "/_nodes/stats", "/", "/_cluster/state", "/" };

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        try (Netty4HttpClient nettyHttpClient = Netty4HttpClient.http2()) {
            Collection<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), requests);
            try {
                assertThat(responses, hasSize(5));

                Collection<String> opaqueIds = Netty4HttpClient.returnOpaqueIds(responses);
                assertOpaqueIdsInAnyOrder(5, opaqueIds);
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }


    public void testThatNettyHttpServerRequestBlockedWithHeaderVerifier() throws Exception {
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        final FullHttpRequest blockedRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        blockedRequest.headers().add("blockme", "Not Allowed");
        blockedRequest.headers().add(HOST, "localhost");
        blockedRequest.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), "http");

        
        final List<FullHttpResponse> responses = new ArrayList<>();
        try (Netty4HttpClient nettyHttpClient = Netty4HttpClient.http2()    ) {
            try {
                FullHttpResponse blockedResponse = nettyHttpClient.send(transportAddress.address(), blockedRequest);
                responses.add(blockedResponse);
                assertThat(blockedResponse.status().code(), equalTo(401));
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

    public void testThatNettyHttpServerSupportsHttp2PostUpgrades() throws Exception {
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
                String msg = String.format(Locale.ROOT, "Expected opaque id [0], got [%s]", opaqueIds);
                assertOpaqueIdsInAnyOrder(1, opaqueIds);
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

    private void assertOpaqueIdsInAnyOrder(int expected, Collection<String> opaqueIds) {
        // check if opaque ids are present in any order, since for HTTP/2 we use streaming (no head of line blocking)
        // and responses may come back at any order
        assertThat(opaqueIds, containsInAnyOrder(IntStream.range(0, expected).mapToObj(Integer::toString).toArray()));
    }

}
