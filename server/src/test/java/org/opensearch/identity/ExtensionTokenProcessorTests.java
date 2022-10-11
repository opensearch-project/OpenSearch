/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

public class ExtensionTokenProcessorTests extends OpenSearchTestCase {

    private static final Principal userPrincipal = () -> "user1";

    public void testGenerateToken() {
        String extensionUniqueId = "ext_1";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        String expectedToken = userPrincipal.getName() + ":" + extensionUniqueId;
        PrincipalIdentifierToken generatedIdentifier;
        try {
            generatedIdentifier = extensionTokenProcessor.generateToken(userPrincipal);
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
                | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException
                | IOException e) {
       
            e.printStackTrace();
            throw new Error(e);
        }

        assertNotEquals(null, generatedIdentifier);
        assertEquals(expectedToken, generatedIdentifier.getToken());
    }

    public void testExtractPrincipal() {
        String extensionUniqueId = "ext_1";
        String token = userPrincipal.getName() + ":" + extensionUniqueId;
        PrincipalIdentifierToken principalIdentifierToken = new PrincipalIdentifierToken(token);

        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        String principalName;
        try {
            principalName = extensionTokenProcessor.extractPrincipal(principalIdentifierToken);
        } catch (InvalidKeyException | IllegalArgumentException | InvalidAlgorithmParameterException
                | IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException
                | NoSuchPaddingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new Error(e);
        }

        //assertNotEquals(null, principal);
        assertEquals(userPrincipal.getName(), principalName);
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
