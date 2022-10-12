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

    public static final int KEY_SIZE_BITS = 128;
    public static final int INITIALIZATION_VECTOR_SIZE_BYTES = 96;
    public static final int TAG_LENGTH_BITS = 128;
    
    private final byte[] extensionUniqueId;

    private SecretKey secretKey; 
    private SecretKeySpec secretKeySpec; 
    private Cipher encryptionCipher;


    public ExtensionTokenProcessor(String extensionUniqueId) {
        
        //The extension ID should ALWAYS stay the same 
        this.extensionUniqueId = extensionUniqueId.getBytes();
        try {
            generateCipher();
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException | IOException ex) {

            System.out.println("Failed to generate cipher at instantiation");
            ex.printStackTrace();
            throw new Error(ex);
        }
    }

    /**
     * Allow for the reseting of the extension processor key. This will remove all access to existing encryptions. 
     */
    public void generateKey() throws NoSuchAlgorithmException {

        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(KEY_SIZE_BITS, SecureRandom.getInstanceStrong());
        this.secretKey = keyGen.generateKey();
    }

    /**
     * 
     */
    public void generateInitializationVector() {

        byte[] initializationVector = new byte[INITIALIZATION_VECTOR_SIZE_BYTES];
        SecureRandom random = new SecureRandom();
        random.nextBytes(initializationVector);
        return initializationVector;
    }


    /**
     * Completely resets and reinitializes the extensionCipher and all its constituents
     */
    public void generateCipher() throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, IOException {

        Cipher newCipher = Cipher.getInstance("AES/GCM/NoPadding");
        byte[] initializationVector = generateInitializationVector(); 
        generateKey(); 
        GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BITS, this.initializationVector);
        newCipher.init(Cipher.ENCRYPT_MODE, this.secretKey, spec);
        this.encryptionCipher = newCipher;
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
        output.write(this.extensionUniqueId);
        output.write(principal.getName().getBytes());
        byte[] combinedAttributes = output.toByteArray();

        System.out.println(String.format("combinedAttributes String before encryption %s", new String(combinedAttributes, StandardCharsets.UTF_8)));
        byte[] combinedEncoding = this.encryptionCipher.doFinal(combinedAttributes);
        
        byte[] combinedEncodingWithIV = ByteBuffer.allocate(INITIALIZATION_VECTOR_SIZE_BYTES + combinedEncoding.length)
            .put(this.initializationVector)
            .put(combinedEncoding)
            .array();

        //System.out.println(String.format("On Encrypt IV is: %s", new String(this.initializationVector, StandardCharsets.UTF_8)));
        System.out.println(String.format("PIT encoded string at end of generateToken is %s", new String(combinedEncoding, StandardCharsets.UTF_8)));

        return new PrincipalIdentifierToken(new String(combinedEncodingWithIV, StandardCharsets.UTF_8));
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
    public String extractPrincipal(PrincipalIdentifierToken token) throws IllegalArgumentException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, NoSuchAlgorithmException, NoSuchPaddingException {
    
        System.out.println("Top of extract prinicipal");
        String tokenString = token.getToken();
        byte[] tokenBytes = tokenString.getBytes(StandardCharsets.UTF_8);


        ByteBuffer bb = ByteBuffer.wrap(tokenBytes);

        byte[] iv = new byte[INITIALIZATION_VECTOR_SIZE_BYTES];
        bb.get(iv);
        //bb.get(iv, 0, iv.length);

        byte[] cipherText = new byte[bb.remaining()];
        bb.get(cipherText);

        Cipher decryptionCipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BITS, this.initializationVector);
        decryptionCipher.init(Cipher.DECRYPT_MODE, this.secretKey, spec);
        System.out.println(String.format("PIT encoding String before decryption is %s", tokenString));
        byte[] combinedEncoding = decryptionCipher.doFinal(cipherText);
        System.out.println(String.format("PIT encoding String after decryption is %s", new String(combinedEncoding, StandardCharsets.UTF_8)));
        //String principalName = new String(combinedEncoding, StandardCharsets.UTF_8);
        //System.out.println(String.format("Principal Name is %s", principalName)); 

        String decoded = new String(combinedEncoding, StandardCharsets.UTF_8);
        return decoded;
        //Have to be able to look at the principals and find the match if you want to actually return the Principal object and not just a String    
        
        //for (Principal p : Principals){
        //   if (p.NAME.equals(principalName)) {
        //        return p;
        //    }
        //}
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
        byte[] tokenBytes = tokenName.getBytes();
        // Check whether token is correct length 
        if (tokenBytes.length != KEY_SIZE_BITS*2) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }
    }
}