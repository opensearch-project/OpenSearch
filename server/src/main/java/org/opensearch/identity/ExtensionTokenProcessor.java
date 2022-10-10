/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.identity;

import java.security.Principal;
import java.security.SecureRandom;
import java.util.Map;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.message.Message;



/**
 * Token processor class to handle token encryption/decryption
 * This processor is will be instantiated for every extension
 */
public class ExtensionTokenProcessor {
    public static final String INVALID_TOKEN_MESSAGE = "Token must not be null and must be a colon-separated String";
    public static final String INVALID_EXTENSION_MESSAGE = "Token passed here is for a different extension";
    public static final String INVALID_ALGO_MESSAGE = "Failed to create a token because an invalid hashing algorithm was used.";
    public static final String INVALID_PRINCIPAL_MESSAGE = "Token passed here is for a different principal.";

    public static final String ALGORITHM = "AES";
    public static final int KEY_SIZE = 128;
    public static final int INITIALIZATION_VECTOR_SIZE = 96;
    public static final int TAG_LENGTH = 128;
    
    private final byte[] extensionUniqueId;

    private final byte[] initializationVector; 
    private final SecretKey secretKey;  


    public ExtensionTokenProcessor(String extensionUniqueId) {
        this.extensionUniqueId = extensionUniqueId.getBytes();
        this.initializationVector = generateRandomIV(INITIALIZATION_VECTOR_SIZE);;
        try {
            this.secretKey = generateSecretKey(KEY_SIZE);
        }
        catch (NoSuchAlgorithmException noAlgo) {
            
            noAlgo.printStackTrace();
            throw new Error(noAlgo);
        }
    }

    public static byte[] generateRandomIV(int ivLength){

        byte[] nonce = new byte[ivLength/8];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(nonce);
        return nonce;
    }

    public static SecretKey generateSecretKey(int keyLength) throws NoSuchAlgorithmException {

        KeyGenerator keyGen = KeyGenerator.getInstance(ALGORITHM);
        keyGen.init(keyLength);
        return keyGen.generateKey();
    }
    

    /**
     * Create a two-way encrypted access token for given principal for this extension
     * @param: principal being sent to the extension
     * @return token generated from principal
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidAlgorithmParameterException
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     * @throws IOException
     */
    public PrincipalIdentifierToken generateToken(Principal principal) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, IOException {

        Cipher extensionCipher = Cipher.getInstance(ALGORITHM);
        extensionCipher.init(Cipher.ENCRYPT_MODE, this.secretKey, new GCMParameterSpec(TAG_LENGTH, this.initializationVector));
        byte[] extensionEncoding = extensionCipher.doFinal(this.extensionUniqueId);
        
        // Note that this will use the same IV and secret key for every principal on the given extension--this will be the same as the extension's secret key & iv
        Cipher principalCipher = Cipher.getInstance(ALGORITHM);
        principalCipher.init(Cipher.ENCRYPT_MODE, this.secretKey, new GCMParameterSpec(TAG_LENGTH, this.initializationVector));
        byte[] principalEncoding = principalCipher.doFinal(principal.getName().getBytes());

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(principalEncoding);
        output.write(extensionEncoding);

        byte[] combinedEncodings = output.toByteArray();
        
        String token = combinedEncodings.toString();
        
        return new PrincipalIdentifierToken(token);
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

        String token_name = token.getWriteableName();
        byte[] token_bytes = token_name.getBytes();

        validateToken(token);
        String[] parts = token.getToken().split(String.format("*{%d}", KEY_SIZE));
        String pricipalNameEncoded = parts[0];
        byte[] principalNameEncodedBytes = pricipalNameEncoded.getBytes();
        String extensionNameEncoded = parts[1];


        Cipher principalCipher = Cipher.getInstance(ALGORITHM);
        principalCipher.init(Cipher.DECRYPT_MODE, this.secretKey, new GCMParameterSpec(TAG_LENGTH, this.initializationVector));
        byte[] principalEncoding = principalCipher.doFinal(principalNameEncodedBytes);
        String principalName = principalEncoding.toString();
        
        //Have to be able to look at the principals and find the match if you want to actually return the Principal object and not just a String    
        for (Principal p : Principals){
            if (p.NAME.equals(principalName)) {
                return p;
            }
        }
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

        //TODO: This may not work after the conversion back and forth to Strings and byte[]s, check afterwards
        /* 
        String[] parts = token.getWriteableName().split(String.format("*{%d}", 2*KEY_SIZE));

        // check whether token is malformed
        if (parts.length != 2) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }
        */

    }
}
