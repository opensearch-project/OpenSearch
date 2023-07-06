/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.crypto.cache;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.DataKey;
import com.amazonaws.encryptionsdk.EncryptedDataKey;
import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.crypto.keyprovider.CryptoMasterKey;
import org.opensearch.cryptospi.MasterKeyProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Master key cache which exposes methods to be used by refresher job to periodically generate data key pair on expiry.
 **/
public final class MasterKeyCache extends MasterKey<CryptoMasterKey> implements CacheRefreshable, Closeable {
    private final CryptoMasterKey cryptoMasterKey;
    private final Map<String, String> encryptionContext;
    private final CryptoAlgorithm cryptoAlgorithm;

    private final long maxAgeInMilliSecs; // - TTL for cachedDataKey
    private volatile long lastLocalRefresh;

    private volatile DataKey<CryptoMasterKey> dataKey;

    private static final Logger logger = LogManager.getLogger(MasterKeyCache.class);

    public MasterKeyCache(
        final CryptoAlgorithm cryptoAlgorithm,
        final long cacheTTL,
        final MasterKeyProvider masterKeyProvider,
        final String keyProviderName,
        final Map<String, String> encryptionContext
    ) {
        this.cryptoMasterKey = new CryptoMasterKey(masterKeyProvider, keyProviderName);
        this.encryptionContext = encryptionContext;
        this.cryptoAlgorithm = cryptoAlgorithm;
        this.maxAgeInMilliSecs = cacheTTL;
    }

    @Override
    public String getProviderId() {
        return cryptoMasterKey.getProviderId();
    }

    @Override
    public String getKeyId() {
        return cryptoMasterKey.getKeyId();
    }

    @Override
    public DataKey<CryptoMasterKey> generateDataKey(final CryptoAlgorithm algorithm, final Map<String, String> encryptionContext) {
        // Refresh must not happen here since crypto refresher should eagerly refresh cache.
        // This is to handle cases where element expiry is too less.
        if (shouldRefresh()) {
            refresh();
        }

        return dataKey;
    }

    @Override
    public boolean shouldRefresh() {
        return (System.currentTimeMillis() - this.lastLocalRefresh) >= (maxAgeInMilliSecs / 5);
    }

    @Override
    public void refresh() {
        this.dataKey = refreshCache(cryptoAlgorithm, encryptionContext);
    }

    private synchronized DataKey<CryptoMasterKey> refreshCache(CryptoAlgorithm algorithm, Map<String, String> encryptionContext) {

        if (!shouldRefresh()) {
            return this.dataKey;
        }

        logger.info("Generating new data key and refreshing cache");
        DataKey<CryptoMasterKey> curDataKey = cryptoMasterKey.generateDataKey(algorithm, encryptionContext);
        this.lastLocalRefresh = System.currentTimeMillis();
        return curDataKey;
    }

    @Override
    public DataKey<CryptoMasterKey> encryptDataKey(
        final CryptoAlgorithm algorithm,
        final Map<String, String> encryptionContext,
        final DataKey<?> dataKey
    ) {
        return cryptoMasterKey.encryptDataKey(algorithm, encryptionContext, dataKey);
    }

    @Override
    public DataKey<CryptoMasterKey> decryptDataKey(
        final CryptoAlgorithm algorithm,
        final Collection<? extends EncryptedDataKey> encryptedDataKeys,
        final Map<String, String> encryptionContext
    ) throws AwsCryptoException {
        return cryptoMasterKey.decryptDataKey(algorithm, encryptedDataKeys, encryptionContext);
    }

    @Override
    public void close() throws IOException {
        cryptoMasterKey.close();
    }
}
