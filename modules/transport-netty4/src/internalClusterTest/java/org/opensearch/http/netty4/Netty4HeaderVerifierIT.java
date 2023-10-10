/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.Netty4BlockingPlugin;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCounted;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4HeaderVerifierIT extends OpenSearchNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(Netty4BlockingPlugin.class);
    }

    public void testThatNettyHttpServerRequestBlockedWithHeaderVerifier() throws Exception {
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        final FullHttpRequest blockedRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        blockedRequest.headers().add("blockme", "Not Allowed");
        blockedRequest.headers().add(HOST, "localhost");

        final List<FullHttpResponse> responses = new ArrayList<>();
        try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
            try {
                FullHttpResponse blockedResponse = nettyHttpClient.send(transportAddress.address(), blockedRequest);
                responses.add(blockedResponse);
                String blockedResponseContent = new String(ByteBufUtil.getBytes(blockedResponse.content()), StandardCharsets.UTF_8);
                assertThat(blockedResponseContent, containsString("Hit header_verifier"));
                assertThat(blockedResponse.status().code(), equalTo(401));
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

}
