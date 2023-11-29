/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.frame;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.nio.charset.StandardCharsets;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;

public class CipherHandlerTests extends OpenSearchTestCase {

    public void testInvalidNonce() {
        CipherHandler cipherHandler = new CipherHandler(null, 1, CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256);
        byte[] nonce = "random".getBytes(StandardCharsets.UTF_8);
        byte[] content = "content".getBytes(StandardCharsets.UTF_8);
        Assert.assertThrows(IllegalArgumentException.class, () -> cipherHandler.cipherData(nonce, null, content, 0, content.length));
    }

    public void testInvalidSecretKey() {
        CryptoAlgorithm cryptoAlgorithm = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256;
        CipherHandler cipherHandler = new CipherHandler(null, 1, cryptoAlgorithm);
        byte[] nonce = new byte[cryptoAlgorithm.getNonceLen()];
        byte[] content = "content".getBytes(StandardCharsets.UTF_8);
        Assert.assertThrows(AwsCryptoException.class, () -> cipherHandler.cipherData(nonce, null, content, 0, content.length));
    }
}
