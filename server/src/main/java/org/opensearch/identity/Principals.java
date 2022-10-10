/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.NoSuchAlgorithmException;
import java.security.Principal;

/**
 * Available OpenSearch internal principals
 *
 * @opensearch.experimental
 */
public enum Principals {

    /**
     * Represents a principal which has not been authenticated
     */
    UNAUTHENTICATED(new StringPrincipal("Unauthenticated"));

    private final Principal principal;
    private final SecretKey secretKey;
    private final byte[] initializationVector; 
    public static final String ALGORITHM = "AES";
    public static final int KEY_SIZE = 128;
    public static final int INITIALIZATION_VECTOR_SIZE = 96;
    public static final int TAG_LENGTH = 128;

    private Principals(final Principal principal) {
        this.principal = principal;
        this.initializationVector = generateIV(); 
        try{ 
            this.secretKey = generateSecretKey();
        }
        catch (NoSuchAlgorithmException noAlgo) {
            throw new Error(noAlgo);

        }
    }
    

    public static byte[] generateIV(int ivLength){

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
     * Returns the underlying principal for this
     */
    public Principal getPrincipal() {
        return principal;
    }

}
