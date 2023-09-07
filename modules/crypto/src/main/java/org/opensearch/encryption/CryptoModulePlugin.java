/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.encryption.frame.AwsCrypto;
import org.opensearch.encryption.frame.EncryptionMetadata;
import org.opensearch.encryption.frame.FrameCryptoHandler;
import org.opensearch.encryption.keyprovider.CryptoMasterKey;
import org.opensearch.plugins.CryptoPlugin;
import org.opensearch.plugins.Plugin;

import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.ParsedCiphertext;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;

public class CryptoModulePlugin extends Plugin implements CryptoPlugin<EncryptionMetadata, ParsedCiphertext> {

    private final int dataKeyCacheSize = 500;
    private final String algorithm = "ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256";

    // - Cache TTL and Jitter is used to decide the Crypto Cache TTL.
    // - Random number between: (TTL Jitter, TTL - Jitter)
    private final long dataKeyCacheTTL = TimeValue.timeValueDays(2).getMillis();
    private static final long dataKeyCacheJitter = TimeUnit.MINUTES.toMillis(30); // - 30 minutes

    public CryptoHandler<EncryptionMetadata, ParsedCiphertext> getOrCreateCryptoHandler(
        MasterKeyProvider keyProvider,
        String keyProviderName,
        String keyProviderType,
        Runnable onClose
    ) {
        CachingCryptoMaterialsManager materialsManager = createMaterialsManager(
            keyProvider,
            keyProviderName,
            getDataKeyAlgorithm(algorithm)
        );
        return createCryptoHandler(algorithm, materialsManager, keyProvider, onClose);
    }

    private String getDataKeyAlgorithm(String algorithm) {
        if ("ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256".equals(algorithm)) {
            return CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256.getDataKeyAlgo();
        }
        return "";
    }

    // package private for tests
    CryptoHandler<EncryptionMetadata, ParsedCiphertext> createCryptoHandler(
        String algorithm,
        CachingCryptoMaterialsManager materialsManager,
        MasterKeyProvider masterKeyProvider,
        Runnable onClose
    ) {
        // Supporting only 256 bit algorithm as of now. To provide support for other bit size algorithms, necessary
        // changes in key providers are required. Following 2 constraints should be satisfied to add support for
        // another algorithm :
        // 1. It should be safe to cache. Unsafe cache algorithms can't be used at it would require generation of data
        // keys on every encrypt which is not a practical approach.
        // 2. It shouldn't have any trailing metadata. This is needed to handle cases where full content is read
        // till the length of the decrypted bytes are reached. This skips reading trailing metadata and closes
        // remote streams. Remote store can throw an error for such reads saying that content wasn't fully read.
        // With the above constraints ESDK, currently we can only add support for one algorithm.
        if (algorithm.equals("ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256")) {
            return new FrameCryptoHandler(
                new AwsCrypto(materialsManager, CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256),
                masterKeyProvider.getEncryptionContext(),
                onClose
            );
        }
        throw new IllegalArgumentException("Unsupported algorithm: " + algorithm);
    }

    // Package private for tests
    CachingCryptoMaterialsManager createMaterialsManager(MasterKeyProvider masterKeyProvider, String keyProviderName, String algorithm) {
        SecureRandom r = new SecureRandom();
        long low = dataKeyCacheTTL - dataKeyCacheJitter;
        long high = dataKeyCacheTTL + dataKeyCacheJitter;
        long masterKeyCacheTTL = r.nextInt((int) (high - low)) + low;

        CryptoMasterKey cryptoMasterKey = new CryptoMasterKey(masterKeyProvider, keyProviderName, algorithm);
        return CachingCryptoMaterialsManager.newBuilder()
            .withMasterKeyProvider(cryptoMasterKey)
            .withCache(new LocalCryptoMaterialsCache(dataKeyCacheSize))
            .withMaxAge(masterKeyCacheTTL, TimeUnit.MILLISECONDS)
            .build();
    }
}
