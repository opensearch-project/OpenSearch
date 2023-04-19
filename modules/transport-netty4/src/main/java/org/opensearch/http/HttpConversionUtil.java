/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import io.netty.handler.codec.http.HttpMethod;

import org.opensearch.rest.RestRequest;

public final class HttpConversionUtil {
    private HttpConversionUtil() {}

    /**
     * Converts {@link HttpMethod} to {@link RestRequest.Method}
     * @param method {@link HttpMethod} method
     * @return corresponding {@link RestRequest.Method}
     * @throws IllegalArgumentException if HTTP method is not supported
     */
    public static RestRequest.Method convertMethod(HttpMethod method) {
        if (method == HttpMethod.GET) return RestRequest.Method.GET;

        if (method == HttpMethod.POST) return RestRequest.Method.POST;

        if (method == HttpMethod.PUT) return RestRequest.Method.PUT;

        if (method == HttpMethod.DELETE) return RestRequest.Method.DELETE;

        if (method == HttpMethod.HEAD) {
            return RestRequest.Method.HEAD;
        }

        if (method == HttpMethod.OPTIONS) {
            return RestRequest.Method.OPTIONS;
        }

        if (method == HttpMethod.PATCH) {
            return RestRequest.Method.PATCH;
        }

        if (method == HttpMethod.TRACE) {
            return RestRequest.Method.TRACE;
        }

        if (method == HttpMethod.CONNECT) {
            return RestRequest.Method.CONNECT;
        }

        throw new IllegalArgumentException("Unexpected http method: " + method);
    }

}
