/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.crypto.cache.MasterKeyCache;
import org.opensearch.encryption.CryptoUtils;
import org.opensearch.encryption.core.AwsCrypto;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.io.InputStream;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CryptoStore extends AbstractRefCounted implements CryptoClient {

    private static final Logger logger = LogManager.getLogger(CryptoStore.class);
    private static final int cryptoCacheSize = 500;

    // - Cache TTL and Jitter is used to decide the Crypto Cache TTL.
    // - Random number between: (TTL Jitter, TTL - Jitter)
    private static final long cryptoCacheTTL = TimeUnit.DAYS.toMillis(5);
    private static final long cryptoCacheJitter = TimeUnit.MINUTES.toMillis(30); // - 30 minutes

    private final CryptoUtils cryptoUtils;
    private final Runnable onClose;
    private final MasterKeyCache masterKeyCache;
    private final String keyProviderName;
    private final String keyProviderType;

    // package private for tests
    static int FRAME_SIZE = (int) (new ByteSizeValue(8, ByteSizeUnit.KB).getBytes());

    public CryptoStore(
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

        this.cryptoUtils = new CryptoUtils(new AwsCrypto(cachingMaterialsManager, FRAME_SIZE, cryptoAlgorithm), encryptionContext);
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

    @Override
    public Object initCryptoContext() {
        return cryptoUtils.initCryptoContext();
    }

    @Override
    public long adjustEncryptedStreamSize(Object cryptoContextObj, long streamSize) {
        return cryptoUtils.adjustEncryptedStreamSize(cryptoContextObj, streamSize);
    }

    @Override
    public long estimateEncryptedLength(Object cryptoContextObj, long contentLength) {
        return cryptoUtils.estimateEncryptedLength(cryptoContextObj, contentLength);
    }

    @Override
    public InputStreamContainer createEncryptingStream(Object cryptoContext, InputStreamContainer inputStreamContainer) {
        return cryptoUtils.createEncryptingStream(cryptoContext, inputStreamContainer);
    }

    @Override
    public InputStreamContainer createEncryptingStreamOfPart(
        Object cryptoContextObj,
        InputStreamContainer streamContainer,
        int totalStreams,
        int streamIdx
    ) {
        return cryptoUtils.createEncryptingStreamOfPart(cryptoContextObj, streamContainer, totalStreams, streamIdx);
    }

    @Override
    public InputStream createDecryptingStream(InputStream encryptingStream) {
        return cryptoUtils.createDecryptingStream(encryptingStream);
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
