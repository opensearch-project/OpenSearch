/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

import io.netty.handler.codec.http.HttpMethod;

public class HttpConversionUtilTests extends OpenSearchTestCase {

    /**
     * QUERY (RFC 10008) is not one of Netty's cached {@link HttpMethod} constants, so the mapping must compare by name
     * rather than identity. This guards that regression.
     */
    public void testQueryMethodIsMapped() {
        assertEquals(RestRequest.Method.QUERY, HttpConversionUtil.convertMethod(HttpMethod.valueOf("QUERY")));
    }

    public void testStandardMethodsStillMap() {
        assertEquals(RestRequest.Method.GET, HttpConversionUtil.convertMethod(HttpMethod.GET));
        assertEquals(RestRequest.Method.POST, HttpConversionUtil.convertMethod(HttpMethod.POST));
        assertEquals(RestRequest.Method.DELETE, HttpConversionUtil.convertMethod(HttpMethod.DELETE));
    }

    public void testUnknownMethodIsRejected() {
        expectThrows(IllegalArgumentException.class, () -> HttpConversionUtil.convertMethod(HttpMethod.valueOf("FROB")));
    }
}
