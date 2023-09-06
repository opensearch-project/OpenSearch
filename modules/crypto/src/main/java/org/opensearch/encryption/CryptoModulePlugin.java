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
import org.opensearch.encryption.keyprovider.CryptoMasterKey;
import org.opensearch.plugins.CryptoPlugin;
import org.opensearch.plugins.Plugin;

import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;

public class CryptoModulePlugin extends Plugin implements CryptoPlugin<Object, Object> {

    private final int dataKeyCacheSize = 500;
    private final String algorithm = "ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY";

    // - Cache TTL and Jitter is used to decide the Crypto Cache TTL.
    // - Random number between: (TTL Jitter, TTL - Jitter)
    private final long dataKeyCacheTTL = TimeValue.timeValueDays(2).getMillis();
    private static final long dataKeyCacheJitter = TimeUnit.MINUTES.toMillis(30); // - 30 minutes

    public CryptoHandler<Object, Object> getOrCreateCryptoHandler(
        MasterKeyProvider keyProvider,
        String keyProviderName,
        String keyProviderType,
        Runnable onClose
    ) {
        CachingCryptoMaterialsManager materialsManager = createMaterialsManager(keyProvider, keyProviderName, algorithm);
        return createCryptoHandler(algorithm, materialsManager, keyProvider);
    }

    // package private for tests
    CryptoHandler<Object, Object> createCryptoHandler(
        String algorithm,
        CachingCryptoMaterialsManager materialsManager,
        MasterKeyProvider masterKeyProvider
    ) {
        return new NoOpCryptoHandler();
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
