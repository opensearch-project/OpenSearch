/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import org.opensearch.common.crypto.CryptoProvider;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.encryption.keyprovider.CryptoMasterKey;

import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

public class CryptoManagerFactory {

    private final int dataKeyCacheSize;
    private final String algorithm;

    // - Cache TTL and Jitter is used to decide the Crypto Cache TTL.
    // - Random number between: (TTL Jitter, TTL - Jitter)
    private final long dataKeyCacheTTL;
    private static final long dataKeyCacheJitter = TimeUnit.MINUTES.toMillis(30); // - 30 minutes

    public CryptoManagerFactory(String algorithm, TimeValue keyRefreshInterval, int keyCacheSize) {
        this.dataKeyCacheSize = keyCacheSize;
        validateAndGetAlgorithmId(algorithm);
        this.algorithm = algorithm;
        dataKeyCacheTTL = keyRefreshInterval.getMillis();
    }

    private String validateAndGetAlgorithmId(String algorithm) {
        // Supporting only 256 bit algorithm
        switch (algorithm) {
            case "ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY":
                return CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY.getDataKeyAlgo();
            case "ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY_ECDSA_P384":
                return CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY_ECDSA_P384.getDataKeyAlgo();
            default:
                throw new IllegalArgumentException("Unsupported algorithm: " + algorithm);
        }
    }

    public CryptoManager getOrCreateCryptoManager(
        MasterKeyProvider keyProvider,
        String keyProviderName,
        String keyProviderType,
        Runnable onClose
    ) {
        CachingCryptoMaterialsManager materialsManager = createMaterialsManager(
            keyProvider,
            keyProviderName,
            validateAndGetAlgorithmId(algorithm)
        );
        CryptoProvider cryptoProvider = createCryptoProvider(algorithm, materialsManager, keyProvider);
        return createCryptoManager(cryptoProvider, keyProviderType, keyProviderName, onClose);
    }

    // package private for tests
    CryptoProvider createCryptoProvider(
        String algorithm,
        CachingCryptoMaterialsManager materialsManager,
        MasterKeyProvider masterKeyProvider
    ) {
        return new NoOpCryptoProvider();
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

    // package private for tests
    CryptoManager createCryptoManager(CryptoProvider cryptoProvider, String keyProviderType, String keyProviderName, Runnable onClose) {
        return new CryptoManagerImpl(keyProviderName, keyProviderType) {
            @Override
            protected void closeInternal() {
                onClose.run();
            }

            @Override
            public String type() {
                return keyProviderType;
            }

            @Override
            public String name() {
                return keyProviderName;
            }

            @Override
            public CryptoProvider getCryptoProvider() {
                return cryptoProvider;
            }
        };
    }

    private static abstract class CryptoManagerImpl extends AbstractRefCounted implements CryptoManager {
        public CryptoManagerImpl(String keyProviderName, String keyProviderType) {
            super(keyProviderName + "-" + keyProviderType);
        }
    }
}
