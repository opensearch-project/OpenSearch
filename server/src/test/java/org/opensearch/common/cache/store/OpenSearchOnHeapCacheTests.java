/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.stats.CacheStatsDimension;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

public class OpenSearchOnHeapCacheTests extends OpenSearchTestCase {
    private final static long keyValueSize = 50;
    private final static List<String> dimensionNames = List.of("dim1", "dim2");

    public void testStats() throws Exception {
        MockRemovalListener<String, String> listener = new MockRemovalListener<>();
        int maxKeys = between(10, 50);
        int numEvicted = between(10, 20);
        OpenSearchOnHeapCache<String, String> cache = getCache(maxKeys, listener);

        List<ICacheKey<String>> keysAdded = new ArrayList<>();
        int numAdded = maxKeys + numEvicted;
        for (int i = 0; i < numAdded; i++) {
            ICacheKey<String> key = getICacheKey(UUID.randomUUID().toString());
            keysAdded.add(key);
            cache.computeIfAbsent(key, getLoadAwareCacheLoader());

            assertEquals(i + 1, cache.stats().getTotalMisses());
            assertEquals(0, cache.stats().getTotalHits());
            assertEquals(Math.min(maxKeys, i + 1), cache.stats().getTotalEntries());
            assertEquals(Math.min(maxKeys, i + 1) * keyValueSize, cache.stats().getTotalMemorySize());
            assertEquals(Math.max(0, i + 1 - maxKeys), cache.stats().getTotalEvictions());
        }
        // do gets from the last part of the list, which should be hits
        for (int i = numAdded - maxKeys; i < numAdded; i++) {
            cache.computeIfAbsent(keysAdded.get(i), getLoadAwareCacheLoader());
            int numHits = i + 1 - (numAdded - maxKeys);

            assertEquals(numAdded, cache.stats().getTotalMisses());
            assertEquals(numHits, cache.stats().getTotalHits());
            assertEquals(maxKeys, cache.stats().getTotalEntries());
            assertEquals(maxKeys * keyValueSize, cache.stats().getTotalMemorySize());
            assertEquals(numEvicted, cache.stats().getTotalEvictions());
        }

        // invalidate keys
        for (int i = numAdded - maxKeys; i < numAdded; i++) {
            cache.invalidate(keysAdded.get(i));
            int numInvalidated = i + 1 - (numAdded - maxKeys);

            assertEquals(numAdded, cache.stats().getTotalMisses());
            assertEquals(maxKeys, cache.stats().getTotalHits());
            assertEquals(maxKeys - numInvalidated, cache.stats().getTotalEntries());
            assertEquals((maxKeys - numInvalidated) * keyValueSize, cache.stats().getTotalMemorySize());
            assertEquals(numEvicted, cache.stats().getTotalEvictions());
        }
    }

    private OpenSearchOnHeapCache<String, String> getCache(int maxSizeKeys, MockRemovalListener<String, String> listener) {
        ICache.Factory onHeapCacheFactory = new OpenSearchOnHeapCache.OpenSearchOnHeapCacheFactory();
        Settings settings = Settings.builder()
            .put(
                OpenSearchOnHeapCacheSettings.getSettingListForCacheType(CacheType.INDICES_REQUEST_CACHE)
                    .get(MAXIMUM_SIZE_IN_BYTES_KEY)
                    .getKey(),
                maxSizeKeys * keyValueSize + "b"
            )
            .build();

        CacheConfig<String, String> cacheConfig = new CacheConfig.Builder<String, String>().setKeyType(String.class)
            .setValueType(String.class)
            .setWeigher((k, v) -> keyValueSize)
            .setRemovalListener(listener)
            .setSettings(settings)
            .setDimensionNames(dimensionNames)
            .build();
        return (OpenSearchOnHeapCache<String, String>) onHeapCacheFactory.create(cacheConfig, CacheType.INDICES_REQUEST_CACHE, null);
    }

    private static class MockRemovalListener<K, V> implements RemovalListener<ICacheKey<K>, V> {
        CounterMetric numRemovals;

        MockRemovalListener() {
            numRemovals = new CounterMetric();
        }

        @Override
        public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
            numRemovals.inc();
        }
    }

    private ICacheKey<String> getICacheKey(String key) {
        List<CacheStatsDimension> dims = new ArrayList<>();
        for (String dimName : dimensionNames) {
            dims.add(new CacheStatsDimension(dimName, "0"));
        }
        return new ICacheKey<>(key, dims);
    }

    private LoadAwareCacheLoader<ICacheKey<String>, String> getLoadAwareCacheLoader() {
        return new LoadAwareCacheLoader<>() {
            boolean isLoaded = false;

            @Override
            public String load(ICacheKey<String> key) {
                isLoaded = true;
                return UUID.randomUUID().toString();
            }

            @Override
            public boolean isLoaded() {
                return isLoaded;
            }
        };
    }
}
