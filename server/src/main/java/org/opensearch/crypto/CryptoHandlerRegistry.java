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
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CryptoKeyProviderPlugin;
import org.opensearch.plugins.CryptoPlugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * During node bootstrap, installed key provider extensions responsible for generating data keys are loaded.
 * Crypto factories against the respective KP plugins are cached. A crypto factory is used to register crypto
 * handler against an {@link org.opensearch.common.blobstore.EncryptedBlobStore}
 */
public class CryptoHandlerRegistry {
    private static final Logger logger = LogManager.getLogger(CryptoHandlerRegistry.class);
    // Package private for tests
    SetOnce<Map<String, CryptoKeyProviderPlugin>> registry = new SetOnce<>();

    // Package private for tests
    SetOnce<CryptoPlugin> cryptoHandlerPlugin = new SetOnce<>();
    private final Map<CryptoMetadata, CryptoHandler> registeredCryptoHandlers = new HashMap<>();

    private static volatile CryptoHandlerRegistry instance;
    private static final Object lock = new Object();

    /**
     * Initializes the registry with crypto factories for the installed crypto key providers.
     *
     * @param cryptoPlugins The list of installed crypto key provider plugins.
     * @param settings Crypto settings.
     */
    protected CryptoHandlerRegistry(
        List<CryptoPlugin> cryptoPlugins,
        List<CryptoKeyProviderPlugin> cryptoKeyProviderPlugins,
        Settings settings
    ) {
        if (cryptoPlugins == null || cryptoPlugins.size() == 0) {
            return;
        }
        if (cryptoPlugins.size() > 1) {
            // We can remove this to support multiple implementations in future if needed.
            throw new IllegalStateException("More than 1 implementation of crypto plugin found.");
        }

        cryptoHandlerPlugin.set(cryptoPlugins.get(0));
        registry.set(loadCryptoFactories(cryptoKeyProviderPlugins));
    }

    public static CryptoHandlerRegistry getInstance() {
        return instance;
    }

    public static CryptoHandlerRegistry initRegistry(
        List<CryptoPlugin> cryptoPlugins,
        List<CryptoKeyProviderPlugin> cryptoKeyProviderPlugins,
        Settings settings
    ) {
        CryptoHandlerRegistry curInstance = instance;
        if (curInstance == null) {
            synchronized (lock) {
                curInstance = instance;
                if (curInstance == null) {
                    instance = curInstance = new CryptoHandlerRegistry(cryptoPlugins, cryptoKeyProviderPlugins, settings);
                }
            }
        }
        return curInstance;
    }

    // For tests
    protected Map<String, CryptoKeyProviderPlugin> loadCryptoFactories(List<CryptoKeyProviderPlugin> cryptoKPPlugins) {
        Map<String, CryptoKeyProviderPlugin> cryptoFactories = new HashMap<>();
        for (CryptoKeyProviderPlugin cryptoKPPlugin : cryptoKPPlugins) {
            if (cryptoFactories.containsKey(cryptoKPPlugin.type())) {
                throw new IllegalArgumentException("Crypto plugin key provider type [" + cryptoKPPlugin.type() + "] is already registered");
            }
            cryptoFactories.put(cryptoKPPlugin.type(), cryptoKPPlugin);
        }

        return Map.copyOf(cryptoFactories);
    }

    /**
     * Retrieves the crypto factory associated with the given key provider type .
     *
     * @param keyProviderType The unique provider type for which the factory is to be fetched.
     * @return The crypto factory used to create {@link CryptoHandler}
     *         instances in a {@link org.opensearch.common.blobstore.EncryptedBlobStore}.
     * @throws IllegalStateException If the crypto registry is not yet loaded.
     */
    public CryptoKeyProviderPlugin getCryptoKeyProviderPlugin(String keyProviderType) {
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
    public CryptoHandler<?, ?> fetchCryptoHandler(CryptoMetadata cryptoMetadata) {
        CryptoHandler<?, ?> cryptoHandler = registeredCryptoHandlers.get(cryptoMetadata);
        if (cryptoHandler == null) {
            synchronized (registeredCryptoHandlers) {
                cryptoHandler = registeredCryptoHandlers.get(cryptoMetadata);
                if (cryptoHandler == null) {
                    Runnable onClose = () -> {
                        synchronized (registeredCryptoHandlers) {
                            registeredCryptoHandlers.remove(cryptoMetadata);
                        }
                    };
                    cryptoHandler = createCryptoHandler(cryptoMetadata, onClose);
                    registeredCryptoHandlers.put(cryptoMetadata, cryptoHandler);
                }
            }
        }
        return cryptoHandler;
    }

    private CryptoHandler<?, ?> createCryptoHandler(CryptoMetadata cryptoMetadata, Runnable onClose) {
        logger.debug("creating crypto client [{}][{}]", cryptoMetadata.keyProviderType(), cryptoMetadata.keyProviderName());
        CryptoKeyProviderPlugin keyProviderPlugin = getCryptoKeyProviderPlugin(cryptoMetadata.keyProviderType());
        if (keyProviderPlugin == null) {
            throw new CryptoRegistryException(cryptoMetadata.keyProviderName(), cryptoMetadata.keyProviderType());
        }

        try {
            MasterKeyProvider masterKeyProvider = keyProviderPlugin.createKeyProvider(cryptoMetadata);
            return Objects.requireNonNull(cryptoHandlerPlugin.get())
                .getOrCreateCryptoHandler(masterKeyProvider, cryptoMetadata.keyProviderName(), cryptoMetadata.keyProviderType(), onClose);

        } catch (Exception e) {
            logger.warn(
                new ParameterizedMessage(
                    "failed to create crypto manager of name [{}] and type [{}]",
                    cryptoMetadata.keyProviderName(),
                    cryptoMetadata.keyProviderType()
                ),
                e
            );
            throw new CryptoRegistryException(cryptoMetadata.keyProviderName(), cryptoMetadata.keyProviderType(), e);
        }
    }

}
