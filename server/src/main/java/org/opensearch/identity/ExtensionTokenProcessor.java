/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.identity;

import java.security.Principal;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;  
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;



/**
 * Token processor class to handle token encryption/decryption
 * This processor is will be instantiated for every extension
 */
public class ExtensionTokenProcessor {

    public static final String INVALID_TOKEN_MESSAGE = "Token must not be null and must be a colon-separated String";
    public static final String INVALID_EXTENSION_MESSAGE = "Token passed here is for a different extension";
    public static final String INVALID_ALGO_MESSAGE = "Failed to create a token because an invalid hashing algorithm was used.";
    public static final String INVALID_PRINCIPAL_MESSAGE = "Token passed here is for a different principal.";

    public static final int KEY_SIZE_BITS = 256;
    public static final int INITIALIZATION_VECTOR_SIZE_BYTES = 96;
    public static final int TAG_LENGTH_BITS = 128;
    
    public final String extensionUniqueId;

    private SecretKey secretKey; 
    private SecretKeySpec secretKeySpec; 
    private Cipher encryptionCipher;


    public ExtensionTokenProcessor(String extensionUniqueId) {
        
        //The extension ID should ALWAYS stay the same 
        this.extensionUniqueId = extensionUniqueId;
      
    }

    /**
     * Allow for the reseting of the extension processor key. This will remove all access to existing encryptions. 
     */
    public SecretKey generateKey() throws NoSuchAlgorithmException {

        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(KEY_SIZE_BITS, SecureRandom.getInstanceStrong());
        this.secretKey = keyGen.generateKey();
        return this.secretKey;
    }

    /**
     * Getter for the extensionTokenProcessor's secretKey
     */
    public SecretKey getSecretKey() {

        return this.secretKey;
    }

    /**
     * Creates a new initialization vector for encryption--CAN ONLY BE USED ONCE PER KEY
     * @returns A new initialization vector
     */
    public byte[] generateInitializationVector() {

        byte[] initializationVector = new byte[INITIALIZATION_VECTOR_SIZE_BYTES];
        SecureRandom random = new SecureRandom();
        random.nextBytes(initializationVector);
        return initializationVector;
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
        
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(this.extensionUniqueId.getBytes());
        output.write(principal.getName().getBytes());
        byte[] combinedAttributes = output.toByteArray();

        SecretKey secretKey = generateKey(); 
        byte[] initializationVector = generateInitializationVector(); 
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        
        GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BITS, initializationVector);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, spec);
      
        
        byte[] combinedEncoding = cipher.doFinal(combinedAttributes);
        
        byte[] combinedEncodingWithIV = ByteBuffer.allocate(INITIALIZATION_VECTOR_SIZE_BYTES + combinedEncoding.length)
            .put(initializationVector)
            .put(combinedEncoding)
            .array();

        String s = Base64.getEncoder().encodeToString(combinedEncodingWithIV);
        return new PrincipalIdentifierToken(s);
    }

    public static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    /**
     * Decrypt the token and extract Principal
     * @param token the requester identity token, should not be null
     * @return Principal
     * @throws InvalidAlgorithmParameterException
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     *
     * @opensearch.internal
     */
    public String extractPrincipal(PrincipalIdentifierToken token, SecretKey secretKey) throws IllegalArgumentException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, NoSuchAlgorithmException, NoSuchPaddingException {
    
        String tokenString = token.getToken();
        byte[] tokenBytes = Base64.getDecoder().decode(tokenString);


        ByteBuffer bb = ByteBuffer.wrap(tokenBytes);

        byte[] iv = new byte[INITIALIZATION_VECTOR_SIZE_BYTES];
        bb.get(iv);
        //bb.get(iv, 0, iv.length);

        byte[] cipherText = new byte[bb.remaining()];
        bb.get(cipherText);

        Cipher decryptionCipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BITS, iv);
        decryptionCipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
      
        byte[] combinedEncoding = decryptionCipher.doFinal(cipherText);
       
        String decoded = new String(combinedEncoding, StandardCharsets.UTF_8).replace(this.extensionUniqueId, "");
        return decoded;
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

        // Check whether token exists 
        if (token == null || token.getToken() == null) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }

        String tokenName = token.getWriteableName();
        byte[] tokenBytes = Base64.getDecoder().decode(token.getToken());
        // Check whether token is correct length 
        if (tokenBytes.length >= INITIALIZATION_VECTOR_SIZE_BYTES) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }
    }
}