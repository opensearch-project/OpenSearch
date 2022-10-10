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
    public static final String INVALID_ALGO_MESSAGE = "Failed to create a token because an invalid hashing algorithm was used.";
    public static final String INVALID_PRINCIPAL_MESSAGE = "Token passed here is for a different principal.";

    private final String extensionUniqueId;
    private final byte[] extensionSecret;
    private byte[] saltedExtensionId; 
    private Principal principal; 

    public ExtensionTokenProcessor(String extensionUniqueId) {
        this.extensionUniqueId = extensionUniqueId;
        //Look into the role mapping for the auto-generated extension id:secret pair-->grab the secret 
        this.extensionSecret = ExtensionDataSample.getExtensionSecret(this.extensionUniqueId);
       
    }

    public String getExtensionUniqueId() {
        return extensionUniqueId;
    }

   /**
    * Uses the extension secret to salt the extensionID and then transform it into a deterministic hashed string 
    * Adapted from https://www.geeksforgeeks.org/sha-256-hash-in-java/
    * @return A hashed string that is only verifiable by core
    */
    public String hashExtensionId() throws NoSuchAlgorithmException{
        
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        messageDigest.update(this.extensionSecret, 0, 0); // Applies the secret as salt to MessageDigest 
        this.saltedExtensionId =  messageDigest.digest(this.extensionUniqueId.getBytes(StandardCharsets.UTF_8));
        BigInteger convertedNumber = new BigInteger(1, this.saltedExtensionId);
        StringBuilder hexString = new StringBuilder(convertedNumber.toString(16)); 
        String hashedExtensionId = hexString.toString();
        return hashedExtensionId;
    }

    public String hashPrincipalName(Principal principal) throws NoSuchAlgorithmException{

        this.principal = principal;
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        messageDigest.update(this.extensionSecret, 0, 0); // Applies the secret as salt to MessageDigest 
        byte[] hashedPrincipal =  messageDigest.digest(principal.getName().getBytes(StandardCharsets.UTF_8));
        BigInteger convertedNumber = new BigInteger(1, hashedPrincipal);
        StringBuilder hexString = new StringBuilder(convertedNumber.toString(16)); 
        return hexString.toString();
        
    }

    /**
     * Create a two-way encrypted access token for given principal for this extension
     * @return token generated from principal
     */
    public PrincipalIdentifierToken generateToken(Principal principal) {
        // This is a placeholder implementation
        // More concrete implementation will be covered in https://github.com/opensearch-project/OpenSearch/issues/4485

        try {
            String hashedExtensionId = this.hashExtensionId();
            String hashedPrincipalId = this.hashPrincipalName(principal);
            String token = hashedPrincipalId + ":" + hashedExtensionId;
            return new PrincipalIdentifierToken(token);
        }
        catch (NoSuchAlgorithmException noAlgo) {
            noAlgo.printStackTrace();
            System.exit(1);
            String token = INVALID_ALGO_MESSAGE;
            return new PrincipalIdentifierToken(token);
        }
    }

    /**
     * Decrypt the token and extract Principal
     * @param token the requester identity token, should not be null
     * @return Principal
     *
     * @opensearch.internal
     *
     * This method contains a placeholder simplementation.
     * More concrete implementation will be covered in https://github.com/opensearch-project/OpenSearch/issues/4485
     */
    public Principal extractPrincipal(PrincipalIdentifierToken token) throws IllegalArgumentException {
        // check is token is valid, we don't do anything if it is valid
        // else we re-throw the thrown exception
        validateToken(token);
        return this.principal;
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

        // check whether token is for this principal

        try {
            String expectedHashedPrincipal = hashPrincipalName(this.principal);
            if (!parts[0].equals(expectedHashedPrincipal)){
                throw new IllegalArgumentException(INVALID_PRINCIPAL_MESSAGE);
                
            }
        }

        catch (NoSuchAlgorithmException noAlgo) {
            noAlgo.printStackTrace();
            System.exit(1);            
        }

        // check whether token is for this extension
        try {   
                String hashedId = hashExtensionId();
                if (!parts[1].equals(hashedId)) {
                    throw new IllegalArgumentException(INVALID_EXTENSION_MESSAGE);
                }
            }
        catch (NoSuchAlgorithmException noAlgo) {
            noAlgo.printStackTrace();
            System.exit(1);            
        }
    }
}
