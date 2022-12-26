/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.index.store.remote.utils.cache.CacheUsage;
import org.opensearch.index.store.remote.utils.cache.RefCountedCache;
import org.opensearch.index.store.remote.utils.cache.SegmentedCache;
import org.opensearch.index.store.remote.utils.cache.stats.CacheStats;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.BiFunction;

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
public class FileCache implements RefCountedCache<Path, CachedIndexInput> {
    private final SegmentedCache<Path, CachedIndexInput> theCache;

    public FileCache(SegmentedCache<Path, CachedIndexInput> cache) {
        this.theCache = cache;
    }

    public long capacity() {
        return theCache.capacity();
    }

    public CachedIndexInput put(Path filePath, CachedIndexInput indexInput) {
        return theCache.put(filePath, indexInput);
    }

    @Override
    public void putAll(Map<? extends Path, ? extends CachedIndexInput> m) {
        theCache.putAll(m);
    }

    @Override
    public CachedIndexInput computeIfPresent(
        Path key,
        BiFunction<? super Path, ? super CachedIndexInput, ? extends CachedIndexInput> remappingFunction
    ) {
        return theCache.computeIfPresent(key, remappingFunction);
    }

    /**
     * Given a file path, gets the corresponding file index input from FileCache.
     * This API also updates the priority for the given file
     *
     * @param filePath given file path
     * @return corresponding file index input from FileCache.
     */
    public CachedIndexInput get(Path filePath) {
        return theCache.get(filePath);
    }

    /**
     * Given a file path, remove the file from cache.
     * Even if the file is pinned or it's still in use, the reclaim
     * still take effect.
     *
     * @param filePath given file path
     */
    public void remove(final Path filePath) {
        theCache.remove(filePath);
    }

    @Override
    public void removeAll(Iterable<? extends Path> keys) {
        theCache.removeAll(keys);
    }

    @Override
    public void clear() {
        theCache.clear();
    }

    @Override
    public long size() {
        return theCache.size();
    }

    @Override
    public void incRef(Path key) {
        theCache.incRef(key);
    }

    @Override
    public void decRef(Path key) {
        theCache.decRef(key);
    }

    @Override
    public long prune() {
        return theCache.prune();
    }

    @Override
    public CacheUsage usage() {
        return theCache.usage();
    }

    @Override
    public CacheStats stats() {
        return theCache.stats();
    }
}
