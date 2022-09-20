/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.test.OpenSearchTestCase;

import java.security.Principal;

public class ExtensionTokenProcessorTests extends OpenSearchTestCase {

    private static final Principal userPrincipal = () -> "user1";

    public void testGenerateToken() {
        String extensionUniqueId = "ext_1";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        String expectedToken = userPrincipal.getName() + ":" + extensionUniqueId;
        PrincipalIdentifierToken generatedIdentifier = extensionTokenProcessor.generateToken(userPrincipal);

        assertNotEquals(null, generatedIdentifier);
        assertEquals(expectedToken, generatedIdentifier.getToken());
    }

    public void testExtractPrincipal() {
        String extensionUniqueId = "ext_1";
        String token = userPrincipal.getName() + ":" + extensionUniqueId;
        PrincipalIdentifierToken principalIdentifierToken = new PrincipalIdentifierToken(token);

        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        Principal principal = extensionTokenProcessor.extractPrincipal(principalIdentifierToken);

        assertNotEquals(null, principal);
        assertEquals(userPrincipal.getName(), principal.getName());
    }

    public void testExtractPrincipalMalformedToken() {
        String extensionUniqueId = "ext_1";
        String token = "garbage";
        PrincipalIdentifierToken principalIdentifierToken = new PrincipalIdentifierToken(token);
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> extensionTokenProcessor.extractPrincipal(principalIdentifierToken)
        );

        assertFalse(exception.getMessage().isEmpty());
        assertEquals(ExtensionTokenProcessor.INVALID_TOKEN_MESSAGE, exception.getMessage());
    }

    public void testExtractPrincipalWithNullToken() {
        String extensionUniqueId1 = "ext_1";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId1);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> extensionTokenProcessor.extractPrincipal(null));

        assertFalse(exception.getMessage().isEmpty());
        assertEquals(ExtensionTokenProcessor.INVALID_TOKEN_MESSAGE, exception.getMessage());
    }

    public void testExtractPrincipalWithTokenInvalidExtension() {
        String extensionUniqueId1 = "ext_1";
        String extensionUniqueId2 = "ext_2";
        String token = userPrincipal.getName() + ":" + extensionUniqueId1;
        PrincipalIdentifierToken principalIdentifierToken = new PrincipalIdentifierToken(token);
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId2);

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> extensionTokenProcessor.extractPrincipal(principalIdentifierToken)
        );

        assertFalse(exception.getMessage().isEmpty());
        assertEquals(ExtensionTokenProcessor.INVALID_EXTENSION_MESSAGE, exception.getMessage());
    }
}
