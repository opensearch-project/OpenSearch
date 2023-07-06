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
import org.junit.Assert;
import org.mockito.Mockito;
import org.opensearch.common.settings.Settings;
import org.opensearch.crypto.MockExtensionPlugin;
import org.opensearch.encryption.core.Utils;
import org.opensearch.crypto.keyprovider.CryptoMasterKey;
import org.opensearch.cryptospi.MasterKeyProvider;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;

public class KeyCacheTests {

    public void testKeyRefresh() throws InterruptedException, IOException {
        MockExtensionPlugin mockExtensionPlugin = new MockExtensionPlugin();
        MasterKeyProvider keyProvider = Mockito.spy(mockExtensionPlugin.createKeyProvider(Settings.EMPTY));
        long cacheTTL = 100;
        try (
            MasterKeyCache masterKeyCache = new MasterKeyCache(
                CryptoAlgorithm.ALG_AES_128_GCM_IV12_TAG16_HKDF_SHA256,
                cacheTTL,
                keyProvider,
                "test-key-provider",
                new HashMap<>()
            )
        ) {
            masterKeyCache.refresh();
            masterKeyCache.refresh();
            masterKeyCache.refresh();
            Mockito.verify(keyProvider, Mockito.times(1)).generateDataPair();
            Thread.sleep(cacheTTL);
            masterKeyCache.refresh();
            Mockito.verify(keyProvider, Mockito.times(2)).generateDataPair();
        }
    }

    static class MockCacheRefresher extends CacheRefresher {

        public MockCacheRefresher(String threadpoolName, ThreadPool threadpool) {
            super(threadpoolName, threadpool);
        }

        @Override
        public void rescheduleIfNecessary() {
            // DO nothing here
        }
    }

    public void testCacheRefresher() {
        MockCacheRefresher cacheRefresher = Mockito.spy(new MockCacheRefresher("mock", Mockito.mock(ThreadPool.class)));
        CacheRefreshable openRefreshable = Mockito.mock(CacheRefreshable.class);
        Mockito.when(openRefreshable.shouldRefresh()).thenReturn(true);
        Mockito.doNothing().when(openRefreshable).refresh();
        cacheRefresher.register(openRefreshable);
        Mockito.verify(openRefreshable, Mockito.times(1)).refresh();
        Mockito.verify(openRefreshable, Mockito.times(1)).shouldRefresh();
        cacheRefresher.runInternal();
        Mockito.verify(openRefreshable, Mockito.times(2)).refresh();
        Mockito.verify(openRefreshable, Mockito.times(2)).shouldRefresh();
        cacheRefresher.deregister(openRefreshable);
        cacheRefresher.runInternal();
        cacheRefresher.runInternal();
        Mockito.verify(openRefreshable, Mockito.times(2)).refresh();
        Mockito.verify(openRefreshable, Mockito.times(2)).shouldRefresh();

        Assert.assertTrue(cacheRefresher.mustReschedule());
        CacheRefreshable closedRefreshable = Mockito.mock(CacheRefreshable.class);
        cacheRefresher.register(closedRefreshable);
        Mockito.when(closedRefreshable.shouldRefresh()).thenReturn(false);
        Mockito.verify(closedRefreshable, Mockito.times(0)).refresh();
        Mockito.verify(closedRefreshable, Mockito.times(1)).shouldRefresh();
        cacheRefresher.runInternal();
        Mockito.verify(closedRefreshable, Mockito.times(0)).refresh();
        Mockito.verify(closedRefreshable, Mockito.times(2)).shouldRefresh();
        Mockito.when(closedRefreshable.shouldRefresh()).thenReturn(true);
        cacheRefresher.runInternal();
        Mockito.verify(closedRefreshable, Mockito.times(1)).refresh();
        cacheRefresher.close();
        Assert.assertFalse(cacheRefresher.mustReschedule());
    }

    private int randomIntBetween(int min, int max) {
        SecureRandom random = new SecureRandom();
        return random.nextInt(max - min) + min;
    }

    public void testKeyEncryption() throws IOException {
        MockExtensionPlugin mockExtensionPlugin = new MockExtensionPlugin();
        MasterKeyProvider keyProvider = Mockito.spy(mockExtensionPlugin.createKeyProvider(Settings.EMPTY));
        long cacheTTL = 5;
        CryptoAlgorithm cryptoAlgorithm = CryptoAlgorithm.ALG_AES_128_GCM_IV12_TAG16_HKDF_SHA256;
        try (
            MasterKeyCache masterKeyCache = new MasterKeyCache(cryptoAlgorithm, cacheTTL, keyProvider, "test-key-provider", new HashMap<>())
        ) {
            DataKey<CryptoMasterKey> generateDataKey = masterKeyCache.generateDataKey(cryptoAlgorithm, new HashMap<>());
            EncryptedDataKey encryptedDataKey = new EncryptedDataKey() {
                @Override
                public String getProviderId() {
                    return generateDataKey.getProviderId();
                }

                @Override
                public byte[] getProviderInformation() {
                    return generateDataKey.getProviderInformation();
                }

                @Override
                public byte[] getEncryptedDataKey() {
                    return generateDataKey.getEncryptedDataKey();
                }
            };

            masterKeyCache.decryptDataKey(cryptoAlgorithm, Collections.singletonList(encryptedDataKey), new HashMap<>());
        }
    }

    public void testInvalidKeys() throws IOException {
        MockExtensionPlugin mockExtensionPlugin = new MockExtensionPlugin();
        MasterKeyProvider keyProvider = Mockito.spy(mockExtensionPlugin.createKeyProvider(Settings.EMPTY));
        long cacheTTL = 5;
        CryptoAlgorithm cryptoAlgorithm = CryptoAlgorithm.ALG_AES_128_GCM_IV12_TAG16_HKDF_SHA256;
        try (
            MasterKeyCache masterKeyCache = new MasterKeyCache(cryptoAlgorithm, cacheTTL, keyProvider, "test-key-provider", new HashMap<>())
        ) {
            Assert.assertThrows(
                IllegalArgumentException.class,
                () -> masterKeyCache.decryptDataKey(cryptoAlgorithm, Collections.emptyList(), new HashMap<>())
            );
            Assert.assertThrows(
                IllegalArgumentException.class,
                () -> masterKeyCache.decryptDataKey(cryptoAlgorithm, null, new HashMap<>())
            );

            EncryptedDataKey invalidKey = new EncryptedDataKey() {
                @Override
                public String getProviderId() {
                    return "Invalid-provider";
                }

                @Override
                public byte[] getProviderInformation() {
                    return new byte[] { 9, 12, 31 };
                }

                @Override
                public byte[] getEncryptedDataKey() {
                    return new byte[] { 9, 12, 31 };
                }
            };

            Assert.assertThrows(
                IllegalArgumentException.class,
                () -> masterKeyCache.decryptDataKey(cryptoAlgorithm, Collections.singletonList(invalidKey), new HashMap<>())
            );
        }
    }

    public void testUtils() {
        Assert.assertThrows(NullPointerException.class, () -> Utils.assertNonNull(null, "null-param"));
    }
}
