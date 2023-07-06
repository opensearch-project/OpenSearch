/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.frame.core;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.security.GeneralSecurityException;
import java.security.spec.AlgorithmParameterSpec;

/**
 * This class provides a cryptographic cipher handler powered by an underlying block cipher. The
 * block cipher performs authenticated encryption of the provided bytes using Additional
 * Authenticated Data (AAD).
 *
 * <p>This class implements a method called cipherData() that encrypts or decrypts a byte array by
 * calling methods on the underlying block cipher.
 */
public class CipherHandler {
    private final int cipherMode_;
    private final SecretKey key_;
    private final CryptoAlgorithm cryptoAlgorithm_;
    private final Cipher cipher_;

    /**
     * Process data through the cipher.
     *
     * <p>This method calls the <code>update</code> and <code>doFinal</code> methods on the underlying
     * cipher to complete processing of the data.
     *
     * @param nonce the nonce to be used by the underlying cipher
     * @param contentAad the optional additional authentication data to be used by the underlying
     *     cipher
     * @param content the content to be processed by the underlying cipher
     * @param off the offset into content array to be processed
     * @param len the number of bytes to process
     * @return the bytes processed by the underlying cipher
     * @throws AwsCryptoException if cipher initialization fails
     * @throws BadCiphertextException if processing the data through the cipher fails
     */
    public byte[] cipherData(byte[] nonce, byte[] contentAad, final byte[] content, final int off, final int len) {
        if (nonce.length != cryptoAlgorithm_.getNonceLen()) {
            throw new IllegalArgumentException("Invalid nonce length: " + nonce.length);
        }
        final AlgorithmParameterSpec spec = new GCMParameterSpec(cryptoAlgorithm_.getTagLen() * 8, nonce, 0, nonce.length);

        try {
            cipher_.init(cipherMode_, key_, spec);
            if (contentAad != null) {
                cipher_.updateAAD(contentAad);
            }
        } catch (final GeneralSecurityException gsx) {
            throw new AwsCryptoException(gsx);
        }
        try {
            return cipher_.doFinal(content, off, len);
        } catch (final GeneralSecurityException gsx) {
            throw new BadCiphertextException(gsx);
        }
    }

    /**
     * Create a cipher handler for processing bytes using an underlying block cipher.
     *
     * @param key the key to use in encrypting or decrypting bytes
     * @param cipherMode the mode for processing the bytes as defined in {@link Cipher#init(int,
     *     java.security.Key)}
     * @param cryptoAlgorithm the cryptography algorithm to be used by the underlying block cipher.
     */
    public CipherHandler(final SecretKey key, final int cipherMode, final CryptoAlgorithm cryptoAlgorithm) {
        this.cipherMode_ = cipherMode;
        this.key_ = key;
        this.cryptoAlgorithm_ = cryptoAlgorithm;
        this.cipher_ = buildCipherObject(cryptoAlgorithm);
    }

    private static Cipher buildCipherObject(final CryptoAlgorithm alg) {
        try {
            // Right now, just GCM is supported
            return Cipher.getInstance("AES/GCM/NoPadding");
        } catch (final GeneralSecurityException ex) {
            throw new IllegalStateException("Java does not support the requested algorithm", ex);
        }
    }
}
