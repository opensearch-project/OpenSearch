/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.test.OpenSearchTestCase;

import java.security.Principal;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.AEADBadTagException;
import javax.crypto.SecretKey;
public class ExtensionTokenProcessorTests extends OpenSearchTestCase {

    private static final String userName = "user1";
    private static final Principal userPrincipal = () -> userName;

    public void testGenerateToken() {

        System.out.println("Start of the extension token tests");
        String extensionUniqueId = "ext_1";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        PrincipalIdentifierToken generatedIdentifier;

        try {
            generatedIdentifier = extensionTokenProcessor.generateToken(userPrincipal);
        } catch (InvalidKeyException | NoSuchAlgorithmException | InvalidAlgorithmParameterException | IOException | NoSuchPaddingException
            | IllegalBlockSizeException | BadPaddingException e) {

            throw new Error(e);
        }

        assertNotEquals(null, generatedIdentifier);
        System.out.println(generatedIdentifier.getToken());
    }

    public void testExtractPrincipal() {
        String extensionUniqueId = "ext_2";

        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        PrincipalIdentifierToken generatedIdentifier;

        String principalName;

        SecretKey secretKey;

        try {
            generatedIdentifier = extensionTokenProcessor.generateToken(userPrincipal);
            secretKey = extensionTokenProcessor.getSecretKey();
            principalName = extensionTokenProcessor.extractPrincipal(generatedIdentifier, secretKey);
        } catch (InvalidKeyException | IllegalArgumentException | InvalidAlgorithmParameterException | IllegalBlockSizeException
            | BadPaddingException | NoSuchAlgorithmException | NoSuchPaddingException | IOException e) {

            throw new Error(e);
        }

        assertEquals(userName, principalName);
    }

    public void testBadAEADTag() {

        String extensionUniqueId = "ext_2";

        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        PrincipalIdentifierToken generatedIdentifier;

        // Create a new key that does not match
        SecretKey secretKey;
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(128, SecureRandom.getInstanceStrong());
            secretKey = keyGen.generateKey();
            generatedIdentifier = extensionTokenProcessor.generateToken(userPrincipal);
        } catch (InvalidKeyException | IllegalArgumentException | InvalidAlgorithmParameterException | IllegalBlockSizeException
            | BadPaddingException | NoSuchAlgorithmException | NoSuchPaddingException | IOException ex) {

            throw new Error(ex);
        }

        Exception exception = assertThrows(
            AEADBadTagException.class,
            () -> extensionTokenProcessor.extractPrincipal(generatedIdentifier, secretKey)
        );

        assertFalse(exception.getMessage().isEmpty());
        assertEquals(ExtensionTokenProcessor.INVALID_TAG_MESSAGE, exception.getMessage());
    }

    public void testBadKey() {

        String extensionUniqueId = "ext_2";

        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId);

        PrincipalIdentifierToken generatedIdentifier;

        String principalName;

        // Create a key of the wrong type
        SecretKey secretKey;
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("HmacMD5");
            keyGen.init(128, SecureRandom.getInstanceStrong());
            secretKey = keyGen.generateKey();
            generatedIdentifier = extensionTokenProcessor.generateToken(userPrincipal);
        } catch (InvalidKeyException | IllegalArgumentException | InvalidAlgorithmParameterException | IllegalBlockSizeException
            | BadPaddingException | NoSuchAlgorithmException | NoSuchPaddingException | IOException ex) {

            throw new Error(ex);
        }

        Exception exception = assertThrows(
            InvalidKeyException.class,
            () -> extensionTokenProcessor.extractPrincipal(generatedIdentifier, secretKey)
        );

        assertFalse(exception.getMessage().isEmpty());
        assertEquals(ExtensionTokenProcessor.INVALID_KEY_MESSAGE, exception.getMessage());
    }

    public void testGeneratePrincipalWithNullPrincipal() {
        String extensionUniqueId1 = "ext_1";
        ExtensionTokenProcessor extensionTokenProcessor = new ExtensionTokenProcessor(extensionUniqueId1);

        Exception exception = assertThrows(NullPointerException.class, () -> extensionTokenProcessor.generateToken(null));

        assertFalse(exception.getMessage().isEmpty());
        assertEquals(ExtensionTokenProcessor.INVALID_PRINCIPAL_MESSAGE, exception.getMessage());
    }

    public void testExtractPrincipalWithTokenInvalidExtension() throws InvalidAlgorithmParameterException, NoSuchPaddingException,
        IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException, IOException, InvalidKeyException {
        String extensionUniqueId1 = "ext_1";
        String extensionUniqueId2 = "ext_2";
        String token = userPrincipal.getName() + ":" + extensionUniqueId1;

        ExtensionTokenProcessor extensionTokenProcessor1 = new ExtensionTokenProcessor(extensionUniqueId1);
        ExtensionTokenProcessor extensionTokenProcessor2 = new ExtensionTokenProcessor(extensionUniqueId2);

        PrincipalIdentifierToken generatedIdentifier2 = extensionTokenProcessor2.generateToken(userPrincipal);
        SecretKey secretKey2 = extensionTokenProcessor2.getSecretKey();

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> extensionTokenProcessor1.extractPrincipal(generatedIdentifier2, secretKey2)
        );

        assertFalse(exception.getMessage().isEmpty());
        assertEquals(ExtensionTokenProcessor.INVALID_EXTENSION_MESSAGE, exception.getMessage());
    }
}
