/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.cryptoplugin;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.cryptospi.CryptoKeyProviderExtension;
import org.opensearch.cryptospi.MasterKeyProvider;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.cryptoplugin.cache.CacheRefresher;
import org.opensearch.cryptoplugin.cache.MasterKeyCache;
import org.opensearch.crypto.CryptoManager;
import org.opensearch.plugins.CryptoPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * AWS plugin for encryption and decryption support.
 */
public class CryptoBasePlugin extends Plugin implements CryptoPlugin, ExtensiblePlugin {
    private static final Logger logger = LogManager.getLogger(CryptoBasePlugin.class);
    private static final String NAME = "crypto";
    private final Map<String, CryptoKeyProviderExtension> keyProviderExtensions;
    protected final Map<String, Map<String, CryptoManagerImpl>> keyProviderCryptoManagers;
    private final long cacheTTL = TimeUnit.DAYS.toMillis(7);
    private CacheRefresher cacheRefresher;
    private static final String CRYPTO_ASYNC_REFRESH_THREADPOOL = "crypto_async_refresh";

    /**
     * Constructor for crypto
     */
    public CryptoBasePlugin() {
        keyProviderExtensions = new HashMap<>();
        keyProviderCryptoManagers = new HashMap<>();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(new FixedExecutorBuilder(settings, CRYPTO_ASYNC_REFRESH_THREADPOOL, 1, 10, CRYPTO_ASYNC_REFRESH_THREADPOOL));
        return executorBuilders;
    }

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver expressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        cacheRefresher = new CacheRefresher(CRYPTO_ASYNC_REFRESH_THREADPOOL, threadPool);
        return Collections.emptyList();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public CryptoManager.Factory createClientFactory(String keyProviderType) {
        return (cryptoSettings, keyProviderName) -> {
            if (!keyProviderExtensions.containsKey(keyProviderType)) {
                throw new IllegalArgumentException(
                    "Key provider extension of type " + keyProviderType + " not found in the installed crypto extension plugins."
                );
            }

            synchronized (keyProviderCryptoManagers) {
                Map<String, CryptoManagerImpl> existingCryptoStores = keyProviderCryptoManagers.get(keyProviderType);
                if (existingCryptoStores != null && existingCryptoStores.containsKey(keyProviderName)) {
                    existingCryptoStores.get(keyProviderName).incRef();
                    return existingCryptoStores.get(keyProviderName);
                }

                MasterKeyProvider masterKeyProvider = keyProviderExtensions.get(keyProviderType).createKeyProvider(cryptoSettings);
                Map<String, String> encryptionContext = new HashMap<>();
                CryptoAlgorithm cryptoAlgorithm = getCryptoAlgorithm();
                MasterKeyCache masterKeyCache = new MasterKeyCache(
                    cryptoAlgorithm,
                    cacheTTL,
                    masterKeyProvider,
                    keyProviderName,
                    encryptionContext
                );
                cacheRefresher.register(masterKeyCache);
                CryptoManagerImpl cryptoManagerImpl = new CryptoManagerImpl(keyProviderName, keyProviderType, () -> {
                    cacheRefresher.deregister(masterKeyCache);
                    keyProviderCryptoManagers.get(keyProviderType).remove(keyProviderName);
                    if (keyProviderCryptoManagers.get(keyProviderType).isEmpty() == true) {
                        keyProviderCryptoManagers.remove(keyProviderType);
                    }
                }, masterKeyCache, encryptionContext, cryptoAlgorithm);
                cryptoManagerImpl.incRef();
                keyProviderCryptoManagers.putIfAbsent(keyProviderType, new HashMap<>());
                keyProviderCryptoManagers.get(keyProviderType).put(keyProviderName, cryptoManagerImpl);
            }
            return keyProviderCryptoManagers.get(keyProviderType).get(keyProviderName);
        };
    }

    CryptoAlgorithm getCryptoAlgorithm() {
        return CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY;
    }

    @Override
    public Set<String> getKeyProviderTypes() {
        return Collections.unmodifiableSet(keyProviderExtensions.keySet());
    }

    @Override
    public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
        logger.info("CryptoBasePlugin reloadSPI called");
        Iterable<CryptoKeyProviderExtension> iterable = loader.loadExtensions(CryptoKeyProviderExtension.class);
        List<CryptoKeyProviderExtension> keyProviderExtensions = new ArrayList<>();
        iterable.forEach(keyProviderExtensions::add);
        if (keyProviderExtensions.isEmpty()) {
            logger.error("Unable to find any implementation for CryptoKeyProviderExtension");
            throw new IllegalArgumentException("Unable to find any implementation for CryptoKeyProviderExtension");
        } else {
            keyProviderExtensions.forEach(keyProvider -> {
                logger.info("Loaded crypto key provider extension " + keyProvider.type());
                this.keyProviderExtensions.put(keyProvider.type(), keyProvider);
            });
        }
    }

    @Override
    public void close() {
        cacheRefresher.close();
    }
}
