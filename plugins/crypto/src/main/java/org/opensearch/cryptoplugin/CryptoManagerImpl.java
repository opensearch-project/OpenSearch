/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cryptoplugin;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.crypto.CryptoProvider;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.crypto.CryptoManager;
import org.opensearch.cryptoplugin.cache.MasterKeyCache;
import org.opensearch.encryption.frame.FrameCryptoProvider;
import org.opensearch.encryption.frame.core.AwsCrypto;

import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CryptoManagerImpl extends AbstractRefCounted implements CryptoManager {

    private static final Logger logger = LogManager.getLogger(CryptoManagerImpl.class);
    private static final int cryptoCacheSize = 500;

    // - Cache TTL and Jitter is used to decide the Crypto Cache TTL.
    // - Random number between: (TTL Jitter, TTL - Jitter)
    private static final long cryptoCacheTTL = TimeUnit.DAYS.toMillis(5);
    private static final long cryptoCacheJitter = TimeUnit.MINUTES.toMillis(30); // - 30 minutes

    private final Runnable onClose;
    private final MasterKeyCache masterKeyCache;
    private final String keyProviderName;
    private final String keyProviderType;

    private final CryptoProvider cryptoProvider;

    public CryptoManagerImpl(
        String keyProviderName,
        String keyProviderType,
        Runnable onClose,
        MasterKeyCache keyProvider,
        Map<String, String> encryptionContext,
        CryptoAlgorithm cryptoAlgorithm
    ) {
        super(keyProviderName + "-" + keyProviderType);
        SecureRandom r = new SecureRandom();
        long low = cryptoCacheTTL - cryptoCacheJitter;
        long high = cryptoCacheTTL + cryptoCacheJitter;
        long masterKeyCacheTTL = r.nextInt((int) (high - low)) + low;

        CachingCryptoMaterialsManager cachingMaterialsManager = CachingCryptoMaterialsManager.newBuilder()
            .withMasterKeyProvider(keyProvider)
            .withCache(new LocalCryptoMaterialsCache(cryptoCacheSize))
            .withMaxAge(masterKeyCacheTTL, TimeUnit.MILLISECONDS)
            .build();

        this.cryptoProvider = new FrameCryptoProvider(new AwsCrypto(cachingMaterialsManager, cryptoAlgorithm), encryptionContext);
        this.onClose = onClose;
        this.masterKeyCache = keyProvider;
        this.keyProviderName = keyProviderName;
        this.keyProviderType = keyProviderType;
    }

    /**
     * @return Type of the key provider/implementation
     */
    @Override
    public String type() {
        return keyProviderType;
    }

    /**
     * @return name of the key provider
     */
    @Override
    public String name() {
        return keyProviderName;
    }

    /**
     * @return Crypto provider
     */
    public CryptoProvider getCryptoProvider() {
        return cryptoProvider;
    }

    @Override
    public void closeInternal() {
        try {
            masterKeyCache.close();
        } catch (Exception ex) {
            logger.error(
                () -> new ParameterizedMessage("Failed to close key provider of type [{}], name [{}]", keyProviderType, keyProviderName),
                ex
            );
        }
        onClose.run();
    }
}
