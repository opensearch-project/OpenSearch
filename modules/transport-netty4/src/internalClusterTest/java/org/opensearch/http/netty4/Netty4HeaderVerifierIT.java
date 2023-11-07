/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.Netty4MarkedMessagePlugin;

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
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4HeaderVerifierIT extends OpenSearchNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(Netty4MarkedMessagePlugin.class);
    }

    public void testThatNettyHttpServerRequestMarksMessageWithHeaderVerifier() throws Exception {
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        final FullHttpRequest markedRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        final String expectedMarkedHeaderValue = "Mark with" + randomAlphaOfLength(10);
        markedRequest.headers().add("marked-message", expectedMarkedHeaderValue);

        final List<FullHttpResponse> responses = new ArrayList<>();
        try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
            try {
                final FullHttpResponse markedResponse = nettyHttpClient.send(transportAddress.address(), markedRequest);
                responses.add(markedResponse);
                final String rootResponseContent = new String(ByteBufUtil.getBytes(markedResponse.content()), StandardCharsets.UTF_8);
                assertThat(rootResponseContent, containsString("opensearch"));
                assertThat(markedResponse.status().code(), equalTo(200));

                assertThat(Netty4MarkedMessagePlugin.MESSAGE.get().headers().get("marked-message"), equalTo(expectedMarkedHeaderValue));
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }

    }

}
