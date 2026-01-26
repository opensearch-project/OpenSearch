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

import org.opensearch.http.HttpRequest.HttpVersion;

import java.util.Map;

/**
 * The HTTP response headers extension point that allows to add default response headers
 * based on the HTTP protocol version of the HTTP request (for example, "Alt-Svc" header).
 */
public interface HttpResponseHeadersFactory {
    /**
     * Produces the response headers to be included into the HTTP response
     * @param version HTTP protocol version of the request
     * @return the response headers to be included into the HTTP response
     */
    Map<String, String> headers(HttpVersion version);
}
