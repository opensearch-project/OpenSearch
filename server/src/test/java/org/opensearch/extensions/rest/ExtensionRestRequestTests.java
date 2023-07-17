/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.OpenSearchParseException;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.http.HttpRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import static java.util.Map.entry;

public class ExtensionRestRequestTests extends OpenSearchTestCase {

    private Method expectedMethod;
    private String expectedPath;
    private String expectedUri;
    Map<String, String> expectedParams;
    Map<String, List<String>> expectedHeaders;
    XContentType expectedContentType;
    BytesReference expectedContent;
    String extensionUniqueId1;
    Principal userPrincipal;
    HttpRequest.HttpVersion expectedHttpVersion;
    // Will be replaced with ExtensionTokenProcessor and PrincipalIdentifierToken classes from feature/identity
    String extensionTokenProcessor;
    String expectedRequestIssuerIdentity;
    NamedWriteableRegistry registry;

    public void setUp() throws Exception {
        super.setUp();
        expectedMethod = Method.GET;
        expectedPath = "/test/uri";
        expectedUri = "foobar?foo=bar&baz=42";
        expectedParams = Map.ofEntries(entry("foo", "bar"), entry("baz", "42"));
        expectedHeaders = Map.ofEntries(
            entry("Content-Type", Arrays.asList("application/json")),
            entry("foo", Arrays.asList("hello", "world"))
        );
        expectedContentType = XContentType.JSON;
        expectedContent = new BytesArray("{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8));
        extensionUniqueId1 = "ext_1";
        userPrincipal = () -> "user1";
        expectedHttpVersion = HttpRequest.HttpVersion.HTTP_1_1;
        extensionTokenProcessor = "placeholder_extension_token_processor";
        expectedRequestIssuerIdentity = "placeholder_request_issuer_identity";
    }

    public void testExtensionRestRequest() throws Exception {
        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedUri,
            expectedPath,
            expectedParams,
            expectedHeaders,
            expectedContentType,
            expectedContent,
            expectedRequestIssuerIdentity,
            expectedHttpVersion
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedUri, request.uri());
        assertEquals(expectedPath, request.path());

        assertEquals(expectedParams, request.params());
        assertEquals(expectedHttpVersion, request.protocolVersion());

        assertEquals(Collections.emptyList(), request.consumedParams());
        assertTrue(request.hasParam("foo"));
        assertFalse(request.hasParam("bar"));
        assertEquals("bar", request.param("foo"));
        assertEquals("baz", request.param("bar", "baz"));
        assertEquals(42L, request.paramAsLong("baz", 0L));
        assertEquals(0L, request.paramAsLong("bar", 0L));
        assertTrue(request.consumedParams().contains("foo"));
        assertTrue(request.consumedParams().contains("baz"));

        assertEquals(expectedContentType, request.getXContentType());
        assertTrue(request.hasContent());
        assertFalse(request.isContentConsumed());
        assertEquals(expectedContent, request.content());
        assertTrue(request.isContentConsumed());

        XContentParser parser = request.contentParser(NamedXContentRegistry.EMPTY);
        Map<String, String> contentMap = parser.mapStrings();
        assertEquals("value", contentMap.get("key"));

        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedUri, request.uri());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertEquals(expectedHeaders, request.headers());
                    assertEquals(expectedContent, request.content());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                    assertEquals(expectedHttpVersion, request.protocolVersion());
                }
            }
        }
    }

    public void testExtensionRestRequestWithNoContent() throws Exception {
        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedUri,
            expectedPath,
            expectedParams,
            expectedHeaders,
            null,
            new BytesArray(new byte[0]),
            expectedRequestIssuerIdentity,
            expectedHttpVersion
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedPath, request.path());
        assertEquals(expectedParams, request.params());
        assertEquals(expectedHeaders, request.headers());
        assertNull(request.getXContentType());
        assertEquals(0, request.content().length());
        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
        assertEquals(expectedHttpVersion, request.protocolVersion());

        final ExtensionRestRequest requestWithNoContent = request;
        assertThrows(OpenSearchParseException.class, () -> requestWithNoContent.contentParser(NamedXContentRegistry.EMPTY));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedUri, request.uri());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertNull(request.getXContentType());
                    assertEquals(0, request.content().length());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                    assertEquals(expectedHttpVersion, request.protocolVersion());

                    final ExtensionRestRequest requestWithNoContentType = request;
                    assertThrows(OpenSearchParseException.class, () -> requestWithNoContentType.contentParser(NamedXContentRegistry.EMPTY));
                }
            }
        }
    }

    public void testExtensionRestRequestWithPlainTextContent() throws Exception {
        BytesReference expectedText = new BytesArray("Plain text");

        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedUri,
            expectedPath,
            expectedParams,
            expectedHeaders,
            null,
            expectedText,
            expectedRequestIssuerIdentity,
            expectedHttpVersion
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedUri, request.uri());
        assertEquals(expectedPath, request.path());
        assertEquals(expectedParams, request.params());
        assertNull(request.getXContentType());
        assertEquals(expectedText, request.content());
        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
        assertEquals(expectedHttpVersion, request.protocolVersion());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedUri, request.uri());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertNull(request.getXContentType());
                    assertEquals(expectedText, request.content());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                    assertEquals(expectedHttpVersion, request.protocolVersion());
                }
            }
        }
    }

    public void testRestExecuteOnExtensionResponse() throws Exception {
        RestStatus expectedStatus = RestStatus.OK;
        String expectedContentType = BytesRestResponse.TEXT_CONTENT_TYPE;
        String expectedResponse = "Test response";
        byte[] expectedResponseBytes = expectedResponse.getBytes(StandardCharsets.UTF_8);

        RestExecuteOnExtensionResponse response = new RestExecuteOnExtensionResponse(
            expectedStatus,
            expectedContentType,
            expectedResponseBytes,
            Collections.emptyMap(),
            Collections.emptyList(),
            false
        );

        assertEquals(expectedStatus, response.getStatus());
        assertEquals(expectedContentType, response.getContentType());
        assertArrayEquals(expectedResponseBytes, response.getContent());
        assertEquals(0, response.getHeaders().size());
        assertEquals(0, response.getConsumedParams().size());
        assertFalse(response.isContentConsumed());

        String headerKey = "foo";
        List<String> headerValueList = List.of("bar", "baz");
        Map<String, List<String>> expectedHeaders = Map.of(headerKey, headerValueList);
        List<String> expectedConsumedParams = List.of("foo", "bar");

        response = new RestExecuteOnExtensionResponse(
            expectedStatus,
            expectedContentType,
            expectedResponseBytes,
            expectedHeaders,
            expectedConsumedParams,
            true
        );

        assertEquals(expectedStatus, response.getStatus());
        assertEquals(expectedContentType, response.getContentType());
        assertArrayEquals(expectedResponseBytes, response.getContent());

        assertEquals(1, response.getHeaders().keySet().size());
        assertTrue(response.getHeaders().containsKey(headerKey));

        List<String> fooList = response.getHeaders().get(headerKey);
        assertEquals(2, fooList.size());
        assertTrue(fooList.containsAll(headerValueList));

        assertEquals(2, response.getConsumedParams().size());
        assertTrue(response.getConsumedParams().containsAll(expectedConsumedParams));
        assertTrue(response.isContentConsumed());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                response = new RestExecuteOnExtensionResponse(in);

                assertEquals(expectedStatus, response.getStatus());
                assertEquals(expectedContentType, response.getContentType());
                assertArrayEquals(expectedResponseBytes, response.getContent());

                assertEquals(1, response.getHeaders().keySet().size());
                assertTrue(response.getHeaders().containsKey(headerKey));

                fooList = response.getHeaders().get(headerKey);
                assertEquals(2, fooList.size());
                assertTrue(fooList.containsAll(headerValueList));

                assertEquals(2, response.getConsumedParams().size());
                assertTrue(response.getConsumedParams().containsAll(expectedConsumedParams));
                assertTrue(response.isContentConsumed());
            }
        }
    }
}
