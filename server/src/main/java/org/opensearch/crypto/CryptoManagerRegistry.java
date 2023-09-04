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
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.encryption.CryptoManager;
import org.opensearch.encryption.CryptoManagerFactory;
import org.opensearch.plugins.CryptoKeyProviderPlugin;

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
    SetOnce<Map<String, CryptoKeyProviderPlugin>> registry = new SetOnce<>();

    // Package private for tests
    SetOnce<CryptoManagerFactory> cryptoManagerFactory = new SetOnce<CryptoManagerFactory>();
    private final Map<CryptoMetadata, CryptoManager> registeredCryptoManagers = new HashMap<>();

    private static volatile CryptoManagerRegistry instance;
    private static final Object lock = new Object();

    /**
     * Initializes the registry with crypto factories for the installed crypto key providers.
     *
     * @param cryptoPlugins The list of installed crypto key provider plugins.
     * @param settings Crypto settings.
     */
    protected CryptoManagerRegistry(List<CryptoKeyProviderPlugin> cryptoPlugins, Settings settings) {
        cryptoManagerFactory.set(new CryptoManagerFactory("ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY", TimeValue.timeValueDays(2), 500));
        registry.set(loadCryptoFactories(cryptoPlugins));
    }

    public static CryptoManagerRegistry getInstance() {
        return instance;
    }

    public static CryptoManagerRegistry initRegistry(List<CryptoKeyProviderPlugin> cryptoPlugins, Settings settings) {
        CryptoManagerRegistry curInstance = instance;
        if (curInstance == null) {
            synchronized (lock) {
                curInstance = instance;
                if (curInstance == null) {
                    instance = curInstance = new CryptoManagerRegistry(cryptoPlugins, settings);
                }
            }
        }
        return curInstance;
    }

    // For tests
    protected Map<String, CryptoKeyProviderPlugin> loadCryptoFactories(List<CryptoKeyProviderPlugin> cryptoPlugins) {
        Map<String, CryptoKeyProviderPlugin> cryptoFactories = new HashMap<>();
        for (CryptoKeyProviderPlugin cryptoPlugin : cryptoPlugins) {
            if (cryptoFactories.containsKey(cryptoPlugin.type())) {
                throw new IllegalArgumentException("Crypto plugin key provider type [" + cryptoPlugin.type() + "] is already registered");
            }
            cryptoFactories.put(cryptoPlugin.type(), cryptoPlugin);
        }

        return Map.copyOf(cryptoFactories);
    }

    /**
     * Retrieves the crypto factory associated with the given key provider type (extension id).
     *
     * @param keyProviderType The unique extension type for which the factory is to be fetched.
     * @return The crypto factory used to create {@link CryptoManager}
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
    public CryptoManager fetchCryptoManager(CryptoMetadata cryptoMetadata) {
        CryptoManager cryptoManager = registeredCryptoManagers.get(cryptoMetadata);
        if (cryptoManager == null) {
            synchronized (registeredCryptoManagers) {
                cryptoManager = registeredCryptoManagers.get(cryptoMetadata);
                if (cryptoManager == null) {
                    Runnable onClose = () -> {
                        synchronized (registeredCryptoManagers) {
                            registeredCryptoManagers.remove(cryptoMetadata);
                        }
                    };
                    cryptoManager = createCryptoManager(cryptoMetadata, onClose);
                    registeredCryptoManagers.put(cryptoMetadata, cryptoManager);
                }
            }
        }
        return cryptoManager;
    }

    private CryptoManager createCryptoManager(CryptoMetadata cryptoMetadata, Runnable onClose) {
        logger.debug("creating crypto client [{}][{}]", cryptoMetadata.keyProviderType(), cryptoMetadata.keyProviderName());
        CryptoKeyProviderPlugin keyProviderPlugin = getCryptoKeyProviderPlugin(cryptoMetadata.keyProviderType());
        if (keyProviderPlugin == null) {
            throw new CryptoRegistryException(cryptoMetadata.keyProviderName(), cryptoMetadata.keyProviderType());
        }

        try {
            MasterKeyProvider masterKeyProvider = keyProviderPlugin.createKeyProvider(cryptoMetadata);
            return Objects.requireNonNull(cryptoManagerFactory.get())
                .getOrCreateCryptoManager(masterKeyProvider, cryptoMetadata.keyProviderName(), cryptoMetadata.keyProviderType(), onClose);

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
