/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.core;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.encryption.frame.core.CipherHandler;

import java.nio.charset.StandardCharsets;

public class CipherHandlerTests {

    @Test
    public void testInvalidNonce() {
        CipherHandler cipherHandler = new CipherHandler(null, 1, CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY);
        byte[] nonce = "random".getBytes(StandardCharsets.UTF_8);
        byte[] content = "content".getBytes(StandardCharsets.UTF_8);
        Assert.assertThrows(IllegalArgumentException.class, () -> cipherHandler.cipherData(nonce, null, content, 0, content.length));
    }

    @Test
    public void testInvalidSecretKey() {
        CryptoAlgorithm cryptoAlgorithm = CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY;
        CipherHandler cipherHandler = new CipherHandler(null, 1, cryptoAlgorithm);
        byte[] nonce = new byte[cryptoAlgorithm.getNonceLen()];
        byte[] content = "content".getBytes(StandardCharsets.UTF_8);
        Assert.assertThrows(AwsCryptoException.class, () -> cipherHandler.cipherData(nonce, null, content, 0, content.length));
    }
}
