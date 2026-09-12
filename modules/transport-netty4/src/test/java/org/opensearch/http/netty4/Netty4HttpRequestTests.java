/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class Netty4HttpRequestTests extends OpenSearchTestCase {

    /**
     * QUERY (RFC 10008) is not one of Netty's cached {@link HttpMethod} constants, so the mapping must compare by name
     * rather than identity. This guards that regression.
     */
    public void testQueryMethodIsMapped() {
        assertEquals(RestRequest.Method.QUERY, requestForMethod(HttpMethod.valueOf("QUERY")).method());
    }

    public void testStandardMethodsStillMap() {
        assertEquals(RestRequest.Method.GET, requestForMethod(HttpMethod.GET).method());
        assertEquals(RestRequest.Method.POST, requestForMethod(HttpMethod.POST).method());
        assertEquals(RestRequest.Method.DELETE, requestForMethod(HttpMethod.DELETE).method());
    }

    public void testUnknownMethodIsRejected() {
        Netty4HttpRequest request = requestForMethod(HttpMethod.valueOf("FROB"));
        expectThrows(IllegalArgumentException.class, request::method);
    }

    private static Netty4HttpRequest requestForMethod(HttpMethod method) {
        return new Netty4HttpRequest(new DefaultFullHttpRequest(HTTP_1_1, method, "/_search"), HttpResponseHeadersFactories.newDefault());
    }
}
