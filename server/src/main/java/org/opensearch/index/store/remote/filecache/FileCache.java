/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;
import org.opensearch.index.store.remote.utils.cache.RefCountedCache;
import org.opensearch.index.store.remote.utils.cache.SegmentedCache;
import org.opensearch.index.store.remote.utils.cache.stats.AggregateRefCountedCacheStats;
import org.opensearch.index.store.remote.utils.cache.stats.IRefCountedCacheStats;
import org.opensearch.index.store.remote.utils.cache.stats.RefCountedCacheStats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.opensearch.env.NodeEnvironment.processDirectoryFiles;
import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;
import static org.opensearch.index.store.remote.utils.FileTypeUtils.INDICES_FOLDER_IDENTIFIER;

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
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public class FileCache implements RefCountedCache<Path, CachedIndexInput> {
    private static final Logger logger = LogManager.getLogger(FileCache.class);
    private final SegmentedCache<Path, CachedIndexInput> theCache;

    private final CircuitBreaker circuitBreaker = null;

    /**
     * @deprecated Use {@link FileCache(SegmentedCache<Path, CachedIndexInput>)}. CircuitBreaker parameter is not used.
     */
    @Deprecated(forRemoval = true)
    public FileCache(SegmentedCache<Path, CachedIndexInput> cache, CircuitBreaker circuitBreaker) {
        this(cache);
    }

    public FileCache(SegmentedCache<Path, CachedIndexInput> theCache) {
        this.theCache = theCache;
    }

    public long capacity() {
        return theCache.capacity();
    }

    @Override
    public CachedIndexInput put(Path filePath, CachedIndexInput indexInput) {
        CachedIndexInput cachedIndexInput = theCache.put(filePath, indexInput);
        return cachedIndexInput;
    }

    @Override
    public CachedIndexInput compute(
        Path key,
        BiFunction<? super Path, ? super CachedIndexInput, ? extends CachedIndexInput> remappingFunction
    ) {
        CachedIndexInput cachedIndexInput = theCache.compute(key, remappingFunction);
        return cachedIndexInput;
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

    /**
     * Pins the key in the cache, preventing it from being evicted.
     *
     * @param key
     */
    @Override
    public void pin(Path key) {
        theCache.pin(key);
    }

    /**
     * Unpins the key in the cache, allowing it to be evicted.
     *
     * @param key
     */
    @Override
    public void unpin(Path key) {
        theCache.unpin(key);
    }

    @Override
    public Integer getRef(Path key) {
        return theCache.getRef(key);
    }

    @Override
    public long prune() {
        return theCache.prune();
    }

    @Override
    public long prune(Predicate<Path> keyPredicate) {
        return theCache.prune(keyPredicate);
    }

    @Override
    public long usage() {
        return theCache.usage();
    }

    @Override
    public long activeUsage() {
        return theCache.activeUsage();
    }

    /**
     * Returns the pinned usage of this cache.
     *
     * @return the combined pinned weight of the values in this cache.
     */
    @Override
    public long pinnedUsage() {
        return theCache.pinnedUsage();
    }

    @Override
    public IRefCountedCacheStats stats() {
        return theCache.stats();
    }

    // To be used only for debugging purposes
    public void logCurrentState() {
        logger.trace("CURRENT STATE OF FILE CACHE \n");
        long cacheUsage = theCache.usage();
        logger.trace("Total Usage: " + cacheUsage + " , Active Usage: " + theCache.activeUsage());
        theCache.logCurrentState();
    }

    // To be used only in testing framework.
    public void closeIndexInputReferences() {
        theCache.closeIndexInputReferences();
    }

    /**
     * Restores the file cache instance performing a folder scan of the
     * {@link org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory#LOCAL_STORE_LOCATION}
     * directory within the provided file cache path.
     */
    public void restoreFromDirectory(List<Path> fileCacheDataPaths) {
        Stream.concat(
            fileCacheDataPaths.stream()
                .filter(Files::isDirectory)
                .map(path -> path.resolve(LOCAL_STORE_LOCATION))
                .filter(Files::isDirectory),
            fileCacheDataPaths.stream()
                .filter(Files::isDirectory)
                .map(path -> path.resolve(INDICES_FOLDER_IDENTIFIER))
                .filter(Files::isDirectory)
        ).flatMap(dir -> {
            try {
                return Files.list(dir);
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to process file cache directory. Please clear the file cache for node startup.", e);
            }
        }).filter(Files::isRegularFile).forEach(path -> {
            try {
                put(path.toAbsolutePath(), new RestoredCachedIndexInput(Files.size(path)));
                decRef(path.toAbsolutePath());
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to retrieve cache file details. Please clear the file cache for node startup.", e);
            }
        });
    }

    /**
     * Returns the current {@link AggregateFileCacheStats}
     */
    public AggregateFileCacheStats fileCacheStats() {
        final AggregateRefCountedCacheStats stats = (AggregateRefCountedCacheStats) stats();

        final RefCountedCacheStats overallCacheStats = stats.getOverallCacheStats();
        final RefCountedCacheStats fullFileCacheStats = stats.getFullFileCacheStats();
        final RefCountedCacheStats blockFileCacheStats = stats.getBlockFileCacheStats();
        final RefCountedCacheStats pinnedFileCacheStats = stats.getPinnedFileCacheStats();
        return new AggregateFileCacheStats(
            System.currentTimeMillis(),
            new FileCacheStats(
                overallCacheStats.activeUsage(),
                capacity(),
                overallCacheStats.usage(),
                overallCacheStats.pinnedUsage(),
                overallCacheStats.evictionWeight(),
                overallCacheStats.removeWeight(),
                overallCacheStats.hitCount(),
                overallCacheStats.missCount(),
                FileCacheStatsType.OVER_ALL_STATS
            ),
            new FileCacheStats(
                fullFileCacheStats.activeUsage(),
                capacity(),
                fullFileCacheStats.usage(),
                fullFileCacheStats.pinnedUsage(),
                fullFileCacheStats.evictionWeight(),
                fullFileCacheStats.removeWeight(),
                fullFileCacheStats.hitCount(),
                fullFileCacheStats.missCount(),
                FileCacheStatsType.FULL_FILE_STATS
            ),
            new FileCacheStats(
                blockFileCacheStats.activeUsage(),
                capacity(),
                blockFileCacheStats.usage(),
                blockFileCacheStats.pinnedUsage(),
                blockFileCacheStats.evictionWeight(),
                blockFileCacheStats.removeWeight(),
                blockFileCacheStats.hitCount(),
                blockFileCacheStats.missCount(),
                FileCacheStatsType.BLOCK_FILE_STATS
            ),
            new FileCacheStats(
                pinnedFileCacheStats.activeUsage(),
                capacity(),
                pinnedFileCacheStats.usage(),
                pinnedFileCacheStats.pinnedUsage(),
                pinnedFileCacheStats.evictionWeight(),
                pinnedFileCacheStats.removeWeight(),
                pinnedFileCacheStats.hitCount(),
                pinnedFileCacheStats.missCount(),
                FileCacheStatsType.PINNED_FILE_STATS
            )
        );
    }

    /**
     * Placeholder for the existing file blocks that are in the disk-based
     * local cache at node startup time. We can't open a file handle to these
     * blocks at this point, so we store this placeholder object in the cache.
     * If a block is needed, then these entries will be replaced with a proper
     * entry that will open the actual file handle to create the IndexInput.
     * These entries are eligible for eviction so if nothing needs to reference
     * them they will be deleted when the disk-based local cache fills up.
     */
    public static class RestoredCachedIndexInput implements CachedIndexInput {
        private final long length;

        public RestoredCachedIndexInput(long length) {
            this.length = length;
        }

        @Override
        public IndexInput getIndexInput() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public void close() throws Exception {}
    }

    /**
     * A recursive task for loading file cache entries from disk during node startup.
     * Uses fork-join parallelism to efficiently scan directories and restore cached files.
     */
    public static class LoadTask extends RecursiveAction {
        private final Path path;
        private final FileCache fc;
        private final boolean processedDirectory;
        private final SetOnce<UncheckedIOException> exception;

        public LoadTask(Path path, FileCache fc, SetOnce<UncheckedIOException> exception) {
            this.path = path;
            this.fc = fc;
            this.exception = exception;
            this.processedDirectory = false;
        }

        public LoadTask(Path path, FileCache fc, SetOnce<UncheckedIOException> exception, boolean processedDirectory) {
            this.path = path;
            this.fc = fc;
            this.exception = exception;
            this.processedDirectory = processedDirectory;
        }

        @Override
        public void compute() {
            List<LoadTask> subTasks = new ArrayList<>();
            try {
                if (processedDirectory) {
                    this.fc.restoreFromDirectory(List.of(path));
                } else {
                    if (Files.isDirectory(path)) {
                        try (DirectoryStream<Path> indexStream = Files.newDirectoryStream(path)) {
                            for (Path indexPath : indexStream) {
                                if (Files.isDirectory(indexPath)) {
                                    List<Path> indexSubPaths = new ArrayList<>();
                                    processDirectoryFiles(indexPath, indexSubPaths);
                                    for (Path indexSubPath : indexSubPaths) {
                                        subTasks.add(new LoadTask(indexSubPath, fc, exception, true));
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (IOException | UncheckedIOException e) {
                try {
                    if (e instanceof UncheckedIOException) {
                        exception.set((UncheckedIOException) e);
                    } else {
                        exception.set(new UncheckedIOException("Unable to process directories.", (IOException) e));
                    }
                } catch (SetOnce.AlreadySetException ignore) {

                }
                return;
            }
            invokeAll(subTasks);
        }
    }
}
