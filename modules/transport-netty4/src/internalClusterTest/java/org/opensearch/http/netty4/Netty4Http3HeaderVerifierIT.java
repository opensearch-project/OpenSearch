/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.http.netty4.http3.Http3Utils;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.Netty4BlockingPlugin;
import org.opensearch.transport.Netty4ModulePlugin;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.ReferenceCounted;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static io.netty.handler.codec.http.HttpHeaderNames.HOST;
import static org.junit.Assume.assumeThat;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4Http3HeaderVerifierIT extends OpenSearchNetty4IntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Netty4BlockingPlugin.class, Netty4Http3IT.SecureSettingsPlugin.class);
    }

    public void testThatNettyHttpServerRequestBlockedWithHeaderVerifier() throws Exception {
        assumeThat("HTTP/3 is not available on this arch/platform", Http3Utils.isHttp3Available(), is(true));

        ensureGreen();
        ensureFullyConnectedCluster();

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        final FullHttpRequest blockedRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        blockedRequest.headers().add("blockme", "Not Allowed");
        blockedRequest.headers().add(HOST, "localhost");
        blockedRequest.headers().add(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), "http");

        final List<FullHttpResponse> responses = new ArrayList<>();
        try (Netty4HttpClient nettyHttpClient = Netty4HttpClient.http3().withLogger(logger)) {
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

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HttpTransportSettings.SETTING_HTTP_HTTP3_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange())
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_SECURE_HTTP_TRANSPORT_NAME)
            .build();
    }
}
