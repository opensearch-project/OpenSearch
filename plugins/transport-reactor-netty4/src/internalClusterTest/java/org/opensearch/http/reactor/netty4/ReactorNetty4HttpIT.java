/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.OpenSearchReactorNetty4IntegTestCase;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.http.netty4.http3.Http3Utils;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.AbstractSecureSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.NettyAllocator;
import org.opensearch.transport.reactor.ReactorNetty4Plugin;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCounted;
import reactor.netty.http.HttpProtocol;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class ReactorNetty4HttpIT extends OpenSearchReactorNetty4IntegTestCase {
    public static final class SecureSettingsPlugin extends AbstractSecureSettingsPlugin {
        public SecureSettingsPlugin() {
            super(InsecureTrustManagerFactory.INSTANCE, Http2SecurityUtil.CIPHERS);
        }

        @Override
        protected SSLEngine newSSLEngine(KeyManagerFactory keyManagerFactory) throws SSLException {
            return SslContextBuilder.forServer(keyManagerFactory)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build()
                .newEngine(NettyAllocator.getAllocator());
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testThatNettyHttpServerSupportsHttp2OrHttp3Get() throws Exception {
        String[] requests = new String[] { "/", "/_nodes/stats", "/", "/_cluster/state", "/" };
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        try (ReactorHttpClient client = ReactorHttpClient.https(settings())) {
            Collection<FullHttpResponse> responses = client.get(transportAddress.address(), randomFrom(requests));
            try {
                assertThat(responses, hasSize(1));

                for (HttpResponse response : responses) {
                    if (client.protocol() == HttpProtocol.HTTP3) {
                        assertThat(response.headers().get("Alt-Svc"), containsString("h2="));
                    } else if (client.protocol() == HttpProtocol.H2 && Http3Utils.isHttp3Available() == true) {
                        assertThat(response.headers().get("Alt-Svc"), containsString("h3="));
                    }
                }

                Collection<String> opaqueIds = ReactorHttpClient.returnOpaqueIds(responses);
                assertOpaqueIdsInAnyOrder(1, opaqueIds);
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

    public void testThatNettyHttpServerSupportsHttp2OrHttp3Post() throws Exception {
        final List<Tuple<String, CharSequence>> requests = List.of(Tuple.tuple("/_search", "{\"query\":{ \"match_all\":{}}}"));
        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        try (ReactorHttpClient client = ReactorHttpClient.https(settings())) {
            Collection<FullHttpResponse> responses = client.post(transportAddress.address(), requests);
            try {
                assertThat(responses, hasSize(1));

                for (FullHttpResponse response : responses) {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    if (client.protocol() == HttpProtocol.HTTP3) {
                        assertThat(response.headers().get("Alt-Svc"), containsString("h2="));
                    } else if (client.protocol() == HttpProtocol.H2 && Http3Utils.isHttp3Available() == true) {
                        assertThat(response.headers().get("Alt-Svc"), containsString("h3="));
                    }
                }

                Collection<String> opaqueIds = ReactorHttpClient.returnOpaqueIds(responses);
                assertOpaqueIdsInAnyOrder(1, opaqueIds);
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(settings())
            .put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange())
            .put(NetworkModule.HTTP_TYPE_KEY, ReactorNetty4Plugin.REACTOR_NETTY_SECURE_HTTP_TRANSPORT_NAME)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(SecureSettingsPlugin.class)).toList();
    }

    private Settings settings() {
        return Settings.builder().put(HttpTransportSettings.SETTING_HTTP_HTTP3_ENABLED.getKey(), true).build();
    }

    private void assertOpaqueIdsInAnyOrder(int expected, Collection<String> opaqueIds) {
        // check if opaque ids are present in any order, since for HTTP/2 we use streaming (no head of line blocking)
        // and responses may come back at any order
        assertThat(opaqueIds, containsInAnyOrder(IntStream.range(0, expected).mapToObj(Integer::toString).toArray()));
    }
}
