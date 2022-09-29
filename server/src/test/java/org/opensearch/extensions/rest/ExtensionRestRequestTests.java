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
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import static java.util.Map.entry;

public class ExtensionRestRequestTests extends OpenSearchTestCase {

    public void testExecuteRestRequest() throws Exception {
        Method expectedMethod = Method.GET;
        String expectedUri = "/test/uri";
        Map<String, String> expectedParams = Map.ofEntries(entry("foo", "bar"), entry("baz", "qux"));
        String extensionUniqueId1 = "ext_1";
        Principal userPrincipal = () -> "user1";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId1);
        PrincipalIdentifierToken expectedRequestIssuerIdentity = extensionTokenProcessor.generateToken(userPrincipal);
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            org.opensearch.common.collect.List.of(
                new NamedWriteableRegistry.Entry(
                    PrincipalIdentifierToken.class,
                    PrincipalIdentifierToken.NAME,
                    PrincipalIdentifierToken::new
                )
            )
        );

        ExtensionRestRequest request = new ExtensionRestRequest(expectedMethod, expectedUri, expectedParams, expectedRequestIssuerIdentity);

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedUri, request.uri());
        assertEquals(expectedParams, request.params());
        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                }

                assertEquals(expectedMethod, request.method());
                assertEquals(expectedUri, request.uri());
                assertEquals(expectedParams, request.params());
                assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
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
