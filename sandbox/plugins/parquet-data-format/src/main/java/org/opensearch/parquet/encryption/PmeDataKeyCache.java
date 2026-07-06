/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Node-level cache for PME index data keys.
 *
 * <p>Follows the same singleton pattern as {@code NodeLevelKeyCache} in the Lucene
 * storage-encryption plugin: a single instance is created during plugin startup via
 * {@link #initialize()} and accessed through {@link #getInstance()}. Unlike a bare
 * utility class with only static fields, this allows the cache to hold per-instance
 * configuration in the future (e.g. TTL settings passed to {@code initialize}) and
 * supports clean teardown via {@link #reset()} in tests.
 *
 * <p>Internally keyed by {@link PmeCacheKey} ({@code indexUuid} + {@code shardId}),
 * mirroring how {@code NodeLevelKeyCache} uses {@code ShardCacheKey}. Shard-level keying
 * gives precise lifecycle control: each shard's entry is evicted independently on shard
 * close, with no need to reference-count how many shards of the same index are still open.
 * Multiple shards of the same index each hold a separately cached copy of the same decrypted
 * data key — an acceptable memory tradeoff for the lifecycle simplicity it provides. The
 * public API accepts individual parameters and constructs the key internally, exactly as
 * {@code NodeLevelKeyCache.get(indexUuid, shardId, indexName)} does.
 *
 * <p>On eviction, the internal key material is zeroed via {@link PmeDataKey#zero()}.
 *
 * <p>The singleton is initialised by {@link #initialize()}, which is called once from
 * {@link org.opensearch.parquet.ParquetDataFormatPlugin#createComponents}.
 *
 * <p>TODO: The Lucene storage-encryption plugin ({@code NodeLevelKeyCache} /
 * {@code MasterKeyHealthMonitor}) adds proactive KMS health monitoring on top of a
 * similar cache: it runs a periodic background check for all encrypted indices, applies
 * index-level read/write blocks when the KMS is unreachable, and removes them
 * automatically once the KMS recovers. Consider adding equivalent functionality here:
 * <ul>
 *   <li>A configurable TTL ({@code expireAfterWrite}) so that a KMS outage is detected
 *       at the next background refresh rather than only on the next cold cache miss.</li>
 *   <li>A background health-check thread that proactively re-loads all cached data keys
 *       at the configured interval.</li>
 *   <li>Block management ({@code index.blocks.read} / {@code index.blocks.write}) when
 *       the KMS cannot be reached and the cached key has expired.</li>
 *   <li>Automatic block removal and shard-retry trigger upon KMS recovery.</li>
 * </ul>
 */
public final class PmeDataKeyCache {

    private static final Logger logger = LogManager.getLogger(PmeDataKeyCache.class);

    private static PmeDataKeyCache INSTANCE;

    private final ConcurrentHashMap<PmeCacheKey, PmeDataKey> cache = new ConcurrentHashMap<>();

    private PmeDataKeyCache() {}

    /**
     * Initialises the singleton instance. Must be called once during plugin startup
     * (from {@link org.opensearch.parquet.ParquetDataFormatPlugin#createComponents})
     * before any call to {@link #getInstance()}.
     *
     * <p>Idempotent: a second call has no effect if the instance already exists.
     */
    public static synchronized void initialize() {
        if (INSTANCE == null) {
            INSTANCE = new PmeDataKeyCache();
        }
    }

    /**
     * Returns the singleton instance.
     *
     * @throws IllegalStateException if {@link #initialize()} has not been called yet
     */
    static PmeDataKeyCache getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("PmeDataKeyCache not initialized.");
        }
        return INSTANCE;
    }

    /**
     * Returns the cached {@link PmeDataKey}, or loads it via {@code loader}, caches it,
     * and returns it.
     *
     * <p>Mirrors {@code NodeLevelKeyCache.get(indexUuid, shardId, indexName)}: individual
     * parameters are accepted and the {@link PmeCacheKey} is constructed internally.
     *
     * <p>The raw key bytes returned by {@code loader} are defensively zeroed immediately
     * after the {@link PmeDataKey} is constructed.
     *
     * @param indexUuid     the index UUID
     * @param shardId       the shard ID
     * @param dataKeyId     the data-key identifier ({@link PmeFileKeyMetadata#DEFAULT_DATA_KEY_ID} for now)
     * @param loader        called exactly once on cache miss; the caller is responsible for
     *                      closing over any path or context needed to load the key material
     * @return the cached or freshly loaded data key
     * @throws IOException if the loader fails
     */
    PmeDataKey getOrLoad(String indexUuid, int shardId, String dataKeyId, DataKeyLoader loader) throws IOException {
        Objects.requireNonNull(indexUuid, "indexUuid must not be null");
        Objects.requireNonNull(dataKeyId, "dataKeyId must not be null");

        PmeCacheKey key = new PmeCacheKey(indexUuid, shardId, dataKeyId);
        PmeDataKey existing = cache.get(key);
        if (existing != null) {
            logger.trace("PME key cache: HIT for index=[{}] shard=[{}] keyId=[{}]", indexUuid, shardId, dataKeyId);
            return existing;
        }
        synchronized (cache) {
            existing = cache.get(key);
            if (existing != null) {
                logger.trace("PME key cache: HIT (double-check) for index=[{}] shard=[{}] keyId=[{}]", indexUuid, shardId, dataKeyId);
                return existing;
            }
            logger.trace("PME key cache: MISS for index=[{}] shard=[{}] keyId=[{}] — loading from keyfile", indexUuid, shardId, dataKeyId);
            byte[] rawKey = loader.load();
            try {
                PmeDataKey dataKey = new PmeDataKey(rawKey);
                cache.put(key, dataKey);
                logger.trace("PME key cache: stored new key for index=[{}] shard=[{}] keyId=[{}]", indexUuid, shardId, dataKeyId);
                return dataKey;
            } finally {
                Arrays.fill(rawKey, (byte) 0);
            }
        }
    }

    /**
     * Removes the data key from the cache and zeros its internal key material.
     * Should be called when the shard is closed, mirroring the shard-lifecycle eviction
     * used by {@code NodeLevelKeyCache.evict(indexUuid, shardId, indexName)}.
     *
     * @param indexUuid the index UUID
     * @param shardId   the shard ID
     * @param dataKeyId the data-key identifier
     */
    void evict(String indexUuid, int shardId, String dataKeyId) {
        PmeCacheKey key = new PmeCacheKey(indexUuid, shardId, dataKeyId);
        PmeDataKey removed = cache.remove(key);
        if (removed != null) {
            logger.trace("PME key cache: evicted and zeroed key for index=[{}] shard=[{}] keyId=[{}]", indexUuid, shardId, dataKeyId);
            removed.zero();
        } else {
            logger.trace("PME key cache: evict called but no entry found for index=[{}] shard=[{}] keyId=[{}]", indexUuid, shardId, dataKeyId);
        }
    }

    /**
     * Clears all cached keys, zeroing their key material.
     * Primarily for testing purposes.
     */
    void clear() {
        cache.forEach((k, v) -> v.zero());
        cache.clear();
    }

    /**
     * Resets the singleton instance completely.
     * Primarily for testing purposes where complete cleanup between tests is needed.
     *
     * <p>Mirrors {@code NodeLevelKeyCache.reset()} in the Lucene storage-encryption plugin.
     */
    static synchronized void reset() {
        if (INSTANCE != null) {
            INSTANCE.clear();
            INSTANCE = null;
        }
    }

    /**
     * Supplier that may throw {@link IOException}, used for loading key material
     * from the keyfile on a cache miss.
     */
    @FunctionalInterface
    interface DataKeyLoader {
        byte[] load() throws IOException;
    }
}


