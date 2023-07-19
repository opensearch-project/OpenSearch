/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.rest.BytesRestResponse.TEXT_CONTENT_TYPE;
import static org.opensearch.core.rest.RestStatus.ACCEPTED;
import static org.opensearch.core.rest.RestStatus.OK;

public class ExtensionRestResponseTests extends OpenSearchTestCase {

    private static final String OCTET_CONTENT_TYPE = "application/octet-stream";
    private static final String JSON_CONTENT_TYPE = "application/json; charset=UTF-8";

    private String testText;
    private byte[] testBytes;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        testText = "plain text";
        testBytes = new byte[] { 1, 2 };
    }

    private RestRequest generateTestRequest() {
        RestRequest request = RestRequest.request(null, new HttpRequest() {

            @Override
            public Method method() {
                return Method.GET;
            }

            @Override
            public String uri() {
                return "/foo";
            }

            @Override
            public BytesReference content() {
                return new BytesArray("Text Content");
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return Collections.emptyMap();
            }

            @Override
            public List<String> strictCookies() {
                return Collections.emptyList();
            }

            @Override
            public HttpVersion protocolVersion() {
                return null;
            }

            @Override
            public HttpRequest removeHeader(String header) {
                // we don't use
                return null;
            }

            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                return null;
            }

            @Override
            public Exception getInboundException() {
                return null;
            }

            @Override
            public void release() {}

            @Override
            public HttpRequest releaseAndCopy() {
                return null;
            }
        }, null);
        // consume params "foo" and "bar"
        request.param("foo");
        request.param("bar");
        // consume content
        request.content();
        return request;
    }

    public void testConstructorWithBuilder() throws IOException {
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        builder.field("status", ACCEPTED);
        builder.endObject();
        RestRequest request = generateTestRequest();
        ExtensionRestResponse response = new ExtensionRestResponse(request, OK, builder);

        assertEquals(OK, response.status());
        assertEquals(JSON_CONTENT_TYPE, response.contentType());
        assertEquals("{\"status\":\"ACCEPTED\"}", response.content().utf8ToString());
        for (String param : response.getConsumedParams()) {
            assertTrue(request.consumedParams().contains(param));
        }
        assertTrue(request.isContentConsumed());
    }

    public void testConstructorWithPlainText() {
        RestRequest request = generateTestRequest();
        ExtensionRestResponse response = new ExtensionRestResponse(request, OK, testText);

        assertEquals(OK, response.status());
        assertEquals(TEXT_CONTENT_TYPE, response.contentType());
        assertEquals(testText, response.content().utf8ToString());
        for (String param : response.getConsumedParams()) {
            assertTrue(request.consumedParams().contains(param));
        }
        assertTrue(request.isContentConsumed());
    }

    public void testConstructorWithText() {
        RestRequest request = generateTestRequest();
        ExtensionRestResponse response = new ExtensionRestResponse(request, OK, TEXT_CONTENT_TYPE, testText);

        assertEquals(OK, response.status());
        assertEquals(TEXT_CONTENT_TYPE, response.contentType());
        assertEquals(testText, response.content().utf8ToString());

        for (String param : response.getConsumedParams()) {
            assertTrue(request.consumedParams().contains(param));
        }
        assertTrue(request.isContentConsumed());
    }

    public void testConstructorWithByteArray() {
        RestRequest request = generateTestRequest();
        ExtensionRestResponse response = new ExtensionRestResponse(request, OK, OCTET_CONTENT_TYPE, testBytes);

        assertEquals(OK, response.status());
        assertEquals(OCTET_CONTENT_TYPE, response.contentType());
        assertArrayEquals(testBytes, BytesReference.toBytes(response.content()));
        for (String param : response.getConsumedParams()) {
            assertTrue(request.consumedParams().contains(param));
        }
        assertTrue(request.isContentConsumed());
    }

    public void testConstructorWithBytesReference() {
        RestRequest request = generateTestRequest();
        ExtensionRestResponse response = new ExtensionRestResponse(
            request,
            OK,
            OCTET_CONTENT_TYPE,
            BytesReference.fromByteBuffer(ByteBuffer.wrap(testBytes, 0, 2))
        );

        assertEquals(OK, response.status());
        assertEquals(OCTET_CONTENT_TYPE, response.contentType());
        assertArrayEquals(testBytes, BytesReference.toBytes(response.content()));
        for (String param : response.getConsumedParams()) {
            assertTrue(request.consumedParams().contains(param));
        }
        assertTrue(request.isContentConsumed());
    }
}
