/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.identity.ExtensionTokenProcessor;
import org.opensearch.identity.PrincipalIdentifierToken;
import org.opensearch.rest.RestStatus;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import static java.util.Map.entry;

public class ExtensionRestRequestTests extends OpenSearchTestCase {

    private Method expectedMethod;
    private String expectedPath;
    Map<String, String> expectedParams;
    XContentType expectedContentType;
    BytesReference expectedContent;
    String extensionUniqueId1;
    Principal userPrincipal;
    ExtensionTokenProcessor extensionTokenProcessor;
    PrincipalIdentifierToken expectedRequestIssuerIdentity;
    NamedWriteableRegistry registry;

    public void setUp() throws Exception {
        super.setUp();
        expectedMethod = Method.GET;
        expectedPath = "/test/uri";
        expectedParams = Map.ofEntries(entry("foo", "bar"), entry("baz", "42"));
        expectedContentType = XContentType.JSON;
        expectedContent = new BytesArray("content".getBytes(StandardCharsets.UTF_8));
        extensionUniqueId1 = "ext_1";
        userPrincipal = () -> "user1";
        extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId1);
        expectedRequestIssuerIdentity = extensionTokenProcessor.generateToken(userPrincipal);
        registry = new NamedWriteableRegistry(
            org.opensearch.common.collect.List.of(
                new NamedWriteableRegistry.Entry(
                    PrincipalIdentifierToken.class,
                    PrincipalIdentifierToken.NAME,
                    PrincipalIdentifierToken::new
                )
            )
        );
    }

    public void testExtensionRestRequest() throws Exception {
        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedPath,
            expectedParams,
            expectedContentType,
            expectedContent,
            expectedRequestIssuerIdentity
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedPath, request.path());

        assertEquals(expectedParams, request.params());
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

        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertEquals(expectedContent, request.content());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                }
            }
        }
    }

    public void testExtensionRestRequestWithNoContent() throws Exception {
        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedPath,
            expectedParams,
            null,
            new BytesArray(new byte[0]),
            expectedRequestIssuerIdentity
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedPath, request.path());
        assertEquals(expectedParams, request.params());
        assertNull(request.getXContentType());
        assertEquals(0, request.content().length());
        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertNull(request.getXContentType());
                    assertEquals(0, request.content().length());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                }
            }
        }
    }

    public void testExtensionRestRequestWithPlainTextContent() throws Exception {
        BytesReference expectedText = new BytesArray("Plain text");

        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedPath,
            expectedParams,
            null,
            expectedText,
            expectedRequestIssuerIdentity
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedPath, request.path());
        assertEquals(expectedParams, request.params());
        assertNull(request.getXContentType());
        assertEquals(expectedText, request.content());
        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertNull(request.getXContentType());
                    assertEquals(expectedText, request.content());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                }
            }
        }
    }

    public void testRestExecuteOnExtensionResponse() throws Exception {
        RestStatus expectedStatus = RestStatus.OK;
        String expectedContentType = BytesRestResponse.TEXT_CONTENT_TYPE;
        String expectedResponse = "Test response";
        byte[] expectedResponseBytes = expectedResponse.getBytes(StandardCharsets.UTF_8);

        RestExecuteOnExtensionResponse response = new RestExecuteOnExtensionResponse(expectedStatus, expectedResponse);

        assertEquals(expectedStatus, response.getStatus());
        assertEquals(expectedContentType, response.getContentType());
        assertArrayEquals(expectedResponseBytes, response.getContent());
        assertEquals(0, response.getHeaders().size());

        String headerKey = "foo";
        List<String> headerValueList = List.of("bar", "baz");
        Map<String, List<String>> expectedHeaders = Map.of(headerKey, headerValueList);

        response = new RestExecuteOnExtensionResponse(expectedStatus, expectedContentType, expectedResponseBytes, expectedHeaders);

        assertEquals(expectedStatus, response.getStatus());
        assertEquals(expectedContentType, response.getContentType());
        assertArrayEquals(expectedResponseBytes, response.getContent());

        assertEquals(1, expectedHeaders.keySet().size());
        assertTrue(expectedHeaders.containsKey(headerKey));

        List<String> fooList = expectedHeaders.get(headerKey);
        assertEquals(2, fooList.size());
        assertTrue(fooList.containsAll(headerValueList));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                response = new RestExecuteOnExtensionResponse(in);

                assertEquals(expectedStatus, response.getStatus());
                assertEquals(expectedContentType, response.getContentType());
                assertArrayEquals(expectedResponseBytes, response.getContent());

                assertEquals(1, expectedHeaders.keySet().size());
                assertTrue(expectedHeaders.containsKey(headerKey));

                fooList = expectedHeaders.get(headerKey);
                assertEquals(2, fooList.size());
                assertTrue(fooList.containsAll(headerValueList));
            }
        }
    }
}
