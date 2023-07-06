/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.core;

import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.model.EncryptionMaterialsRequest;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.encryption.MockKeyProvider;
import org.opensearch.encryption.frame.core.EncryptionMetadata;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class EncryptionMetadataTests {

    @Test
    public void testCommitmentPolicy() {
        CryptoAlgorithm cryptoAlgorithm = CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY;
        CommitmentPolicy commitmentPolicy = CommitmentPolicy.ForbidEncryptAllowDecrypt;
        EncryptionMaterialsRequest.Builder requestBuilder = EncryptionMaterialsRequest.newBuilder()
            .setContext(new HashMap<>())
            .setRequestedAlgorithm(cryptoAlgorithm)
            .setPlaintextSize(0) // To avoid skipping cache
            .setCommitmentPolicy(commitmentPolicy);

        CachingCryptoMaterialsManager cachingMaterialsManager = CachingCryptoMaterialsManager.newBuilder()
            .withMasterKeyProvider(new MockKeyProvider())
            .withCache(new LocalCryptoMaterialsCache(10))
            .withMaxAge(100, TimeUnit.MILLISECONDS)
            .build();

        Assert.assertThrows(
            AwsCryptoException.class,
            () -> new EncryptionMetadata(100, cachingMaterialsManager.getMaterialsForEncrypt(requestBuilder.build()), commitmentPolicy)
        );

        cryptoAlgorithm = CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384;
        requestBuilder.setRequestedAlgorithm(cryptoAlgorithm);
        requestBuilder.setCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt);
        Assert.assertThrows(
            AwsCryptoException.class,
            () -> new EncryptionMetadata(
                100,
                cachingMaterialsManager.getMaterialsForEncrypt(requestBuilder.build()),
                CommitmentPolicy.RequireEncryptRequireDecrypt
            )
        );
    }
}
