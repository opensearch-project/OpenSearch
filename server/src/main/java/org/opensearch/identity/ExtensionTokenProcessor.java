/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.identity;

import java.security.Principal;
import java.security.SecureRandom;
import java.util.Map;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;




/**
 * Token processor class to handle token encryption/decryption
 * This processor is will be instantiated for every extension
 */
public class ExtensionTokenProcessor {
    public static final String INVALID_TOKEN_MESSAGE = "Token must not be null and must be a colon-separated String";
    public static final String INVALID_EXTENSION_MESSAGE = "Token passed here is for a different extension";

    private final String extensionUniqueId;
    private final byte[] extensionSecret;
    private final byte[] byteId;
    private byte[] saltedExtensionId; 

    public ExtensionTokenProcessor(String extensionUniqueId) {
        this.extensionUniqueId = extensionUniqueId;
        //Look into the role mapping for the auto-generated extension id:secret pair-->grab the secret 
        this.extensionSecret = ExtensionDataSample.getExtensionSecret(this.extensionUniqueId);
        this.byteId = this.extensionUniqueId.getBytes(StandardCharsets.UTF_8);
    }

    public String getExtensionUniqueId() {
        return extensionUniqueId;
    }

   /**
    * Uses the extension secret to salt the extensionID and then transform it into a deterministic hashed string 
    * @return A hashed string that is only verifiable by core
    */
    public String saltExtensionId() throws NoSuchAlgorithmException{
        
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        this.saltedExtensionId = this.byteId; //Secret allows only core to feasibly reproduce
        BigInteger convertedNumber = new BigInteger(1, this.saltedExtensionId);
        StringBuilder hexString = new StringBuilder(convertedNumber.toString(16)); 

        while (hexString.length() < 64){
            hexString.insert(0, '0');
        }

        String hashedExtensionId = hexString.toString();
        return hashedExtensionId;
    }

    /**
     * Create a two-way encrypted access token for given principal for this extension
     * @return token generated from principal
     */
    public PrincipalIdentifierToken generateToken(Principal principal) {
        // This is a placeholder implementation
        // More concrete implementation will be covered in https://github.com/opensearch-project/OpenSearch/issues/4485
        String token = principal.getName() + ":" + extensionUniqueId;
        
        return new PrincipalIdentifierToken(token);
    }

    /**
     * Decrypt the token and extract Principal
     * @param token the requester identity token, should not be null
     * @return Principal
     *
     * @opensearch.internal
     *
     * This method contains a placeholder implementation.
     * More concrete implementation will be covered in https://github.com/opensearch-project/OpenSearch/issues/4485
     */
    public Principal extractPrincipal(PrincipalIdentifierToken token) throws IllegalArgumentException {
        // check is token is valid, we don't do anything if it is valid
        // else we re-throw the thrown exception
        validateToken(token);

        String[] parts = token.getToken().split(":");
        final String principalName = parts[0];
        return () -> principalName;
    }

    /**
     * Checks validity of the requester identifier token
     * @param token The requester identifier token
     * @throws IllegalArgumentException when token is invalid
     *
     * This method contains a placeholder implementation.
     * More concrete implementation will be covered in https://github.com/opensearch-project/OpenSearch/issues/4485
     */
    public void validateToken(PrincipalIdentifierToken token) throws IllegalArgumentException {

        if (token == null || token.getToken() == null) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }

        String[] parts = token.getToken().split(":");

        // check whether token is malformed
        if (parts.length != 2) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }
        // check whether token is for this extension
        if (!parts[1].equals(extensionUniqueId)) {
            throw new IllegalArgumentException(INVALID_EXTENSION_MESSAGE);
        }
    }
}
