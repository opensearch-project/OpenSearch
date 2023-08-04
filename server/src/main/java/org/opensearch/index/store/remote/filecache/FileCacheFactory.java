/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.index.store.remote.utils.cache.SegmentedCache;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.opensearch.ExceptionsHelper.catchAsRuntimeException;

/**
 * File Cache (FC) is introduced to solve the problem that the local disk cannot hold
 * the entire dataset on remote store. It maintains a node level view of index files with priorities,
 * caching only those index files needed by queries. The file with the lowest priority
 * (Least Recently Used) in the FC is replaced first.
 *
 * <p>The two main interfaces of FC are put and get. When a new file index input is added
 * to the file cache, the file will be added at cache head, which means it has the highest
 * priority.
 * <p> The get function does not add file to cache, but it promotes the priority
 * of a given file (since it makes it the most recently used).
 *
 * <p>Once file cache reaches its capacity, it starts evictions. Eviction removes the file
 * items from cache tail and triggers a callback to clean up the file from disk. The
 * cleanup process also includes closing file’s descriptor.
 *
 * @opensearch.internal
 */
public class FileCacheFactory {

    public static FileCache createConcurrentLRUFileCache(long capacity, CircuitBreaker circuitBreaker) {
        return new FileCache(createDefaultBuilder().capacity(capacity).build(), circuitBreaker);
    }

    public static FileCache createConcurrentLRUFileCache(long capacity, int concurrencyLevel, CircuitBreaker circuitBreaker) {
        return new FileCache(createDefaultBuilder().capacity(capacity).concurrencyLevel(concurrencyLevel).build(), circuitBreaker);
    }

    private static SegmentedCache.Builder<Path, CachedIndexInput> createDefaultBuilder() {
        return SegmentedCache.<Path, CachedIndexInput>builder()
            // use length in bytes as the weight of the file item
            .weigher(CachedIndexInput::length)
            .listener((removalNotification) -> {
                RemovalReason removalReason = removalNotification.getRemovalReason();
                CachedIndexInput value = removalNotification.getValue();
                Path key = removalNotification.getKey();
                if (removalReason != RemovalReason.REPLACED) {
                    catchAsRuntimeException(value::close);
                    catchAsRuntimeException(() -> Files.deleteIfExists(key));
                }
            });
    }

}
