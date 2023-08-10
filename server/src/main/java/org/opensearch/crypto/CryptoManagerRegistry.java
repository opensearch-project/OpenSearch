/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.SetOnce;
import org.opensearch.plugins.CryptoPlugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * During node bootstrap, installed key provider extensions responsible for generating data keys are loaded.
 * Crypto factories against the respective extensions are cached. A crypto factory is used to register crypto
 * manager against an {@link org.opensearch.common.blobstore.EncryptedBlobStore}
 */
public class CryptoManagerRegistry {
    private static final Logger logger = LogManager.getLogger(CryptoManagerRegistry.class);
    // Package private for tests
    static SetOnce<Map<String, CryptoManager.Factory>> registry = new SetOnce<>();
    private static final Map<String, CryptoManager> registeredCryptoManagers = new HashMap<>();

    /**
     * Initializes the registry with crypto factories for the installed crypto key providers.
     *
     * @param cryptoPlugins The list of installed crypto plugins containing key providers.
     */
    public static void initRegistry(List<CryptoPlugin> cryptoPlugins) {
        registry.set(loadCryptoFactories(cryptoPlugins));
    }

    private static Map<String, CryptoManager.Factory> loadCryptoFactories(List<CryptoPlugin> cryptoPlugins) {
        Map<String, CryptoManager.Factory> cryptoFactories = new HashMap<>();
        for (CryptoPlugin cryptoPlugin : cryptoPlugins) {
            for (String keyProviderType : cryptoPlugin.getKeyProviderTypes()) {
                if (cryptoFactories.containsKey(keyProviderType)) {
                    throw new IllegalArgumentException("Crypto plugin key provider type [" + keyProviderType + "] is already registered");
                }
                cryptoFactories.put(keyProviderType, cryptoPlugin.createClientFactory(keyProviderType));
            }
        }

        return cryptoFactories;
    }

    /**
     * Retrieves the crypto factory associated with the given key provider type (extension id).
     *
     * @param keyProviderType The unique extension type for which the factory is to be fetched.
     * @return The crypto factory used to create {@link CryptoManager}
     *         instances in a {@link org.opensearch.common.blobstore.EncryptedBlobStore}.
     * @throws IllegalStateException If the crypto registry is not yet loaded.
     */
    public static CryptoManager.Factory getCryptoManagerFactory(String keyProviderType) {
        if (registry.get() == null) {
            throw new IllegalStateException("Crypto registry is not yet loaded");
        }
        return Objects.requireNonNull(registry.get()).get(keyProviderType);
    }

    /**
     * Fetches the cached crypto manager for the provided crypto metadata or creates a new one if not found.
     * If the key provider is not installed, it throws a {@link CryptoRegistryException}.
     *
     * @param cryptoMetadata The crypto metadata for which the key provider is to be created.
     * @return The crypto manager for performing encrypt/decrypt operations.
     * @throws CryptoRegistryException If the key provider is not installed or there is an error during crypto manager creation.
     */
    public static CryptoManager fetchCryptoManager(CryptoMetadata cryptoMetadata) {
        String cryptoKey = cryptoManagerKey(cryptoMetadata.keyProviderName(), cryptoMetadata.keyProviderType());
        CryptoManager cryptoManager = registeredCryptoManagers.get(cryptoKey);
        if (cryptoManager == null) {
            synchronized (registeredCryptoManagers) {
                cryptoManager = registeredCryptoManagers.get(cryptoKey);
                if (cryptoManager == null) {
                    cryptoManager = createCryptoManager(cryptoMetadata);
                    registeredCryptoManagers.put(cryptoKey, cryptoManager);
                }
            }
        }
        return cryptoManager;
    }

    private static CryptoManager createCryptoManager(CryptoMetadata cryptoMetadata) {
        logger.debug("creating crypto client [{}][{}]", cryptoMetadata.keyProviderType(), cryptoMetadata.keyProviderName());
        CryptoManager.Factory factory = getCryptoManagerFactory(cryptoMetadata.keyProviderType());
        if (factory == null) {
            throw new CryptoRegistryException(
                cryptoMetadata.keyProviderName(),
                "Crypto manager of type [" + cryptoMetadata.keyProviderType() + " is not installed ]"
            );
        }

        CryptoManager cryptoManager;
        try {
            cryptoManager = factory.create(cryptoMetadata.settings(), cryptoMetadata.keyProviderName());
            return cryptoManager;
        } catch (Exception e) {
            logger.warn(
                new ParameterizedMessage(
                    "failed to create crypto manager [{}][{}]",
                    cryptoMetadata.keyProviderType(),
                    cryptoMetadata.keyProviderName()
                ),
                e
            );
            throw new CryptoRegistryException(cryptoMetadata.keyProviderName(), cryptoMetadata.keyProviderType(), e);
        }
    }

    private static String cryptoManagerKey(String keyProviderName, String keyProviderType) {
        return keyProviderName + "#" + keyProviderType;
    }

}
