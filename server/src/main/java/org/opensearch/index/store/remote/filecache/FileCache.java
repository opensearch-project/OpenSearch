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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiFunction;

import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;

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

    @Override
    public CachedIndexInput put(Path filePath, CachedIndexInput indexInput) {
        return theCache.put(filePath, indexInput);
    }

    @Override
    public CachedIndexInput compute(
        Path key,
        BiFunction<? super Path, ? super CachedIndexInput, ? extends CachedIndexInput> remappingFunction
    ) {
        return theCache.compute(key, remappingFunction);
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

    /**
     * Restores the file cache instance performing a folder scan of the
     * {@link org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory#LOCAL_STORE_LOCATION}
     * directory within the provided file cache path.
     */
    public void restoreFromDirectory(List<Path> fileCacheDataPaths) {
        fileCacheDataPaths.stream()
            .filter(Files::isDirectory)
            .map(path -> path.resolve(LOCAL_STORE_LOCATION))
            .filter(Files::isDirectory)
            .flatMap(dir -> {
                try {
                    return Files.list(dir);
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Unable to process file cache directory. Please clear the file cache for node startup.",
                        e
                    );
                }
            })
            .filter(Files::isRegularFile)
            .forEach(path -> {
                try {
                    put(path.toAbsolutePath(), new FileCachedIndexInput.ClosedIndexInput(Files.size(path)));
                } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Unable to retrieve cache file details. Please clear the file cache for node startup.",
                        e
                    );
                }
            });
    }
}
