/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.identity;

import java.security.Principal;
import java.util.Arrays;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;


/**
 * Token processor class to handle token encryption/decryption
 * This processor is will be instantiated for every extension
 */
public class ExtensionTokenProcessor {
    public static final String INVALID_TOKEN_MESSAGE = "Token must not be null and must be a colon-separated String";
    public static final String INVALID_EXTENSION_MESSAGE = "Token passed here is for a different extension";
    public static final String INVALID_ALGO_MESSAGE = "Failed to create a token because an invalid hashing algorithm was used.";
    public static final String INVALID_PRINCIPAL_MESSAGE = "Token passed here is for a different principal.";

    
    public static final int KEY_SIZE = 128;
    public static final int INITIALIZATION_VECTOR_SIZE = 96;
    public static final int TAG_LENGTH = 128;
    
    private final byte[] extensionUniqueId;
 
    private final SecretKey secretKey;  
    private final Cipher encryptionCipher;

    public ExtensionTokenProcessor(String extensionUniqueId) {
        this.extensionUniqueId = extensionUniqueId.getBytes();
        try {
            this.encryptionCipher = generateCipher(); 
            this.secretKey = generateSecretKey(KEY_SIZE);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException ex) {
            
            ex.printStackTrace();
            throw new Error(ex);
        }
    }

    /**
     * This method generates the secret key for a given extension.
     * @param keyLength: The number of bits in the key
     * @return The generated key
     */
    public static SecretKey generateSecretKey(int keyLength) throws NoSuchAlgorithmException {

        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(keyLength);
        return keyGen.generateKey();
    }

    /**
     * Creates the encryption cipher which will perist for encryption and decryption of this extension
     * @return The instance's encryption cipher 
     */
    public static Cipher generateCipher() throws NoSuchAlgorithmException, NoSuchPaddingException{

        Cipher encryptionCipher = Cipher.getInstance("AES/GCM/NoPadding");
        return encryptionCipher;
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
        
        this.encryptionCipher.init(Cipher.ENCRYPT_MODE, this.secretKey);
        byte[] extensionEncoding = this.encryptionCipher.doFinal(this.extensionUniqueId);
        //byte[] principalEncoding = this.encryptionCipher.doFinal(principal.getName().getBytes());

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        //output.write(principalEncoding);
        output.write(extensionEncoding);

        byte[] combinedEncodings = output.toByteArray();
        
        String token = combinedEncodings.toString();
        
        return new PrincipalIdentifierToken(token);
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
    
        String tokenName = token.getWriteableName();
        byte[] tokenBytes = tokenName.getBytes();

        //validateToken(token);
        
        byte[] principalNameEncodedBytes = Arrays.copyOfRange(tokenBytes, 0, KEY_SIZE);
        byte[] extensionNameEncodedBytes = Arrays.copyOfRange(tokenBytes, KEY_SIZE, tokenBytes.length);
        Cipher decryptionCipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH, this.encryptionCipher.getIV());
        decryptionCipher.init(Cipher.DECRYPT_MODE, this.secretKey, spec);
        byte[] principalEncoding = decryptionCipher.doFinal(principalNameEncodedBytes);
        String principalName = principalEncoding.toString();

        return principalName;
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
        if (tokenBytes.length != KEY_SIZE*2) {
            throw new IllegalArgumentException(INVALID_TOKEN_MESSAGE);
        }
    }
}