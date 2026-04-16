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

package org.opensearch.http.netty4;

import org.opensearch.common.settings.Settings;
import org.opensearch.http.HttpRequest.HttpVersion;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.netty4.http3.Http3Utils;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_HTTP3_ENABLED;

/**
 * Default implementations for {@link HttpResponseHeadersFactory} for all supported HTTP
 * protocol versions.
 */
public final class HttpResponseHeadersFactories {
    private static final String ALT_SVC_HEADER = "Alt-Svc";

    private static class Http2AwareResponseHeadersFactory implements HttpResponseHeadersFactory {
        private final HttpServerTransport transport;

        public Http2AwareResponseHeadersFactory(final HttpServerTransport transport) {
            this.transport = Objects.requireNonNull(transport);
        }

        @Override
        public Map<String, String> headers(HttpVersion version) {
            // See please https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Alt-Svc
            return Map.of(
                ALT_SVC_HEADER,
                String.join(
                    ", ",
                    Arrays.stream(transport.boundAddress().boundAddresses())
                        // Use 1h as expiration policy, reconsider in future
                        .map(address -> String.format(Locale.ENGLISH, "h2=\":%d\"; ma=3600", address.getPort()))
                        .distinct()
                        .toList()
                )
            );
        }
    }

    private static class Http3AwareResponseHeadersFactory implements HttpResponseHeadersFactory {
        private final HttpServerTransport transport;

        public Http3AwareResponseHeadersFactory(final HttpServerTransport transport) {
            this.transport = Objects.requireNonNull(transport);
        }

        @Override
        public Map<String, String> headers(HttpVersion version) {
            // See please https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Alt-Svc
            return Map.of(
                ALT_SVC_HEADER,
                String.join(
                    ", ",
                    Arrays.stream(transport.boundAddress().boundAddresses())
                        // Use 1h as expiration policy, reconsider in future
                        .map(address -> String.format(Locale.ENGLISH, "h3=\":%d\"; ma=3600", address.getPort()))
                        .distinct()
                        .toList()
                )
            );
        }
    }

    /**
     * Creates {@link HttpResponseHeadersFactory} instance that is aware of HTTP/2 protocol handler
     * @param settings settings
     * @param transport HTTP server transport
     * @return instance that is aware of HTTP/2 protocol handler
     */
    public static HttpResponseHeadersFactory newHttp2Aware(Settings settings, HttpServerTransport transport) {
        return new Http2AwareResponseHeadersFactory(transport);
    }

    /**
     * Creates {@link HttpResponseHeadersFactory} instance that is aware of HTTP/3 protocol handler
     * @param settings settings
     * @param transport HTTP server transport
     * @return instance that is aware of HTTP/3 protocol handler
     */
    public static HttpResponseHeadersFactory newHttp3Aware(Settings settings, HttpServerTransport transport) {
        if (Http3Utils.isHttp3Available() == true && SETTING_HTTP_HTTP3_ENABLED.get(settings).booleanValue() == true) {
            return new Http3AwareResponseHeadersFactory(transport);
        } else {
            return newDefault();
        }
    }

    /**
     * Creates default {@link HttpResponseHeadersFactory} instance
     * @return default (noop) instance
     */
    public static HttpResponseHeadersFactory newDefault() {
        return new HttpResponseHeadersFactory() {
            @Override
            public Map<String, String> headers(HttpVersion version) {
                return Map.of();
            }
        };
    }
}
