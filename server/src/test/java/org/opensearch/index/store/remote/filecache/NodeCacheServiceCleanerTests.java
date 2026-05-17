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
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.WriteStateException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.shard.ShardStateMetadata;
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;
import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;
import static org.opensearch.index.store.remote.utils.FileTypeUtils.INDICES_FOLDER_IDENTIFIER;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link NodeCacheServiceCleaner}: verifies that both the
 * {@link FileCache} (Lucene LRU) and any registered {@link BlockCache} instances
 * are deterministically evicted when a shard or index is deleted.
 */
public class NodeCacheServiceCleanerTests extends OpenSearchTestCase {
    private static final ShardId SHARD_0 = new ShardId("index", "uuid-0", 0);
    private static final ShardId SHARD_1 = new ShardId("index", "uuid-1", 0);
    private static final Settings SETTINGS = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
        .put("index.store.type", "remote_snapshot")
        .build();
    private static final IndexSettings INDEX_SETTINGS = new IndexSettings(
        IndexMetadata.builder("index").settings(SETTINGS).build(),
        SETTINGS
    );

    private static final ShardId WARM_SHARD_0 = new ShardId("warm-index-0", "warm-uuid-0", 0);
    private static final ShardId WARM_SHARD_1 = new ShardId("warm-index-1", "warm-uuid-1", 1);

    private static final Settings WARM_SETTINGS = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
        .put(IS_WARM_INDEX_SETTING.getKey(), true)
        .build();

    private static final IndexSettings WARM_INDEX_SETTINGS_0 = new IndexSettings(
        IndexMetadata.builder("warm-index-0").settings(WARM_SETTINGS).build(),
        WARM_SETTINGS
    );

    private static final IndexSettings WARM_INDEX_SETTINGS_1 = new IndexSettings(
        IndexMetadata.builder("warm-index-1").settings(WARM_SETTINGS).build(),
        WARM_SETTINGS
    );

    private static final Logger logger = LogManager.getLogger(FileCache.class);

    private final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024 * 1024, 1);
    private final Map<ShardId, Path> files = new HashMap<>();
    private NodeEnvironment env;
    private NodeCacheService orchestrator;
    private NodeCacheServiceCleaner cleaner;

    @Before
    public void setUpFileCache() throws IOException {
        env = newNodeEnvironment(SETTINGS);
        orchestrator = new NodeCacheService(fileCache);
        cleaner = new NodeCacheServiceCleaner(() -> orchestrator);
        files.put(SHARD_0, addFile(fileCache, env, SHARD_0));
        files.put(SHARD_1, addFile(fileCache, env, SHARD_1));

        // add files in filecache for warm index shards.
        Path[] paths0 = env.availableShardPaths(WARM_SHARD_0);
        Path[] paths1 = env.availableShardPaths(WARM_SHARD_1);
        Path path1 = randomFrom(paths0);
        Path path2 = randomFrom(paths1);
        writeShardStateMetadata("warm-uuid-0", path1);
        writeShardStateMetadata("warm-uuid-1", path2);
        files.put(WARM_SHARD_0, addFileForWarmIndex(fileCache, env, WARM_SHARD_0));
        files.put(WARM_SHARD_1, addFileForWarmIndex(fileCache, env, WARM_SHARD_1));

        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
    }

    private static void writeShardStateMetadata(String indexUUID, Path... paths) throws WriteStateException {
        ShardStateMetadata.FORMAT.writeAndCleanup(
            new ShardStateMetadata(true, indexUUID, AllocationId.newInitializing(), ShardStateMetadata.IndexDataLocation.LOCAL),
            paths
        );
    }

    private static Path addFile(FileCache fileCache, NodeEnvironment env, ShardId shardId) throws IOException {
        final ShardPath shardPath = ShardPath.loadFileCachePath(env, shardId);
        final Path localStorePath = shardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
        Files.createDirectories(localStorePath);
        final Path file = Files.createFile(localStorePath.resolve("file"));
        fileCache.put(file, new CachedIndexInput() {
            @Override
            public IndexInput getIndexInput() {
                return null;
            }

            @Override
            public long length() {
                return 1024;
            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {

            }
        });
        return file;
    }

    private static Path addFileForWarmIndex(FileCache fileCache, NodeEnvironment env, ShardId shardId) throws IOException {
        final ShardPath shardPath = ShardPath.loadShardPath(logger, env, shardId, "");
        final Path localStorePath = shardPath.getDataPath().resolve(INDICES_FOLDER_IDENTIFIER);

        logger.info("warm index local store location [{}]", localStorePath);

        Files.createDirectories(localStorePath);
        final Path file = Files.createFile(localStorePath.resolve("file"));

        fileCache.put(file, new CachedIndexInput() {
            @Override
            public IndexInput getIndexInput() {
                return null;
            }

            @Override
            public long length() {
                return 1024;
            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {

            }
        });
        return file;
    }

    @After
    public void tearDownFileCache() {
        env.close();
    }

    // ── FileCache eviction (original tests, adapted) ────────────────────────

    public void testShardRemoved() {
        final Path cachePath = ShardPath.loadFileCachePath(env, SHARD_0).getDataPath();
        assertTrue(Files.exists(cachePath));
        // Initially 4 files exits in fileCache.
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
        // cleanup shard_0 files.
        cleaner.beforeShardPathDeleted(SHARD_0, INDEX_SETTINGS, env);
        // assert fileCache has 3 files.
        MatcherAssert.assertThat(fileCache.size(), equalTo(3L));
        assertNull(fileCache.get(files.get(SHARD_0)));
        assertFalse(Files.exists(files.get(SHARD_0)));
        assertTrue(Files.exists(files.get(SHARD_1)));
        assertFalse(Files.exists(cachePath));
    }

    public void testShardRemovedForWarmIndex() throws IOException {
        final Path indexFilePath0 = ShardPath.loadShardPath(logger, env, WARM_SHARD_0, "").getDataPath();
        final Path indexFilePath1 = ShardPath.loadShardPath(logger, env, WARM_SHARD_1, "").getDataPath();
        assertTrue(Files.exists(indexFilePath0));
        // Initially 4 files exits in fileCache.
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
        // clean warm_shard_0 files
        cleaner.beforeShardPathDeleted(WARM_SHARD_0, WARM_INDEX_SETTINGS_0, env);
        MatcherAssert.assertThat(fileCache.size(), equalTo(3L));
        assertNull(fileCache.get(files.get(WARM_SHARD_0)));
        assertFalse(Files.exists(files.get(WARM_SHARD_0)));
        assertTrue(Files.exists(files.get(WARM_SHARD_1)));
        assertFalse(Files.exists(indexFilePath0));

        // clean warm_shard_1 files as well.
        cleaner.beforeShardPathDeleted(WARM_SHARD_1, WARM_INDEX_SETTINGS_1, env);
        MatcherAssert.assertThat(fileCache.size(), equalTo(2L));
        assertNull(fileCache.get(files.get(WARM_SHARD_1)));
        assertFalse(Files.exists(files.get(WARM_SHARD_1)));
        assertFalse(Files.exists(indexFilePath1));
    }

    public void testIndexRemovedForWarmIndexWhenShardPathDeletedFirst() throws IOException {
        final Path indexFilePath = ShardPath.loadShardPath(logger, env, WARM_SHARD_0, "").getDataPath();
        assertTrue(Files.exists(indexFilePath));
        // assert filecache contains 4 files
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
        // clean shard path files first.
        cleaner.beforeShardPathDeleted(WARM_SHARD_0, WARM_INDEX_SETTINGS_0, env);
        // now clean the index path.
        cleaner.beforeIndexPathDeleted(WARM_INDEX_SETTINGS_0.getIndex(), WARM_INDEX_SETTINGS_0, env);
        // Assert that index path deleted and fileCache also doesn't have files for that index shard.
        MatcherAssert.assertThat(fileCache.size(), equalTo(3L));
        assertFalse(Files.exists(indexFilePath));
    }

    public void testIndexNotRemovedForWarmIndexWhenShardPathNotDeletedFirst() throws IOException {
        final Path indexFilePath = ShardPath.loadShardPath(logger, env, WARM_SHARD_0, "").getDataPath();
        assertTrue(Files.exists(indexFilePath));
        // assert filecache contains 4 files
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
        // Try to not clean the index path without calling beforeShardPathDeleted method
        cleaner.beforeIndexPathDeleted(WARM_INDEX_SETTINGS_0.getIndex(), WARM_INDEX_SETTINGS_0, env);
        // Assert that index path should exists and fileCache also have files for that index shard.
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
        assertTrue(Files.exists(indexFilePath));
    }

    public void testFileCacheNotClearedAndWithFileAlreadyDeleted() throws IOException {
        // Delete the shard path to simulate IOException when trying to load shard path
        Path shardPath = ShardPath.loadShardPath(logger, env, WARM_SHARD_0, "").getDataPath();
        Path shardFilePath = shardPath.resolve(INDICES_FOLDER_IDENTIFIER).resolve("file");
        assertTrue(Files.exists(shardFilePath));
        Files.delete(shardFilePath);

        // Initially 4 files exist in fileCache
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));

        // Try to clean up the deleted shard path
        cleaner.beforeShardPathDeleted(WARM_SHARD_0, WARM_INDEX_SETTINGS_0, env);

        // Verify fileCache still contains all files as remove operation won't get executed.
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
        // Shard path is still deleted
        assertFalse(Files.exists(shardPath));
    }

    public void testShardRemovedForWarmIndexWithIOExceptionOnDirectoryStream() throws IOException {
        Path shardPath = ShardPath.loadShardPath(logger, env, WARM_SHARD_0, "").getDataPath();
        Path indicesPath = shardPath.resolve(INDICES_FOLDER_IDENTIFIER);

        // Make the indices directory non-readable to cause IOException during directory stream
        Files.setPosixFilePermissions(indicesPath, new HashSet<>());

        // Initially 4 files exist in fileCache
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));

        // Try to clean up with non-readable directory
        cleaner.beforeShardPathDeleted(WARM_SHARD_0, WARM_INDEX_SETTINGS_0, env);

        // Restore permissions for cleanup
        Files.setPosixFilePermissions(indicesPath, PosixFilePermissions.fromString("rwxrwxrwx"));

        // Verify fileCache still contains all files as operation failed
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
    }

    public void testIndexRemoved() {
        final Path indexCachePath = env.fileCacheNodePath().fileCachePath.resolve(SHARD_0.getIndex().getUUID());
        assertTrue(Files.exists(indexCachePath));

        cleaner.beforeShardPathDeleted(SHARD_0, INDEX_SETTINGS, env);
        cleaner.beforeShardPathDeleted(SHARD_1, INDEX_SETTINGS, env);
        cleaner.beforeIndexPathDeleted(SHARD_0.getIndex(), INDEX_SETTINGS, env);
        MatcherAssert.assertThat(fileCache.size(), equalTo(2L));
        assertFalse(Files.exists(indexCachePath));
    }

    // ── BlockCache eviction tests ─────────────────────────────────────────────

    /**
     * When a warm shard is deleted, evict_prefix is called on all registered block
     * caches with the shard's data path as the prefix.
     */
    public void testBlockCacheEvictedOnWarmShardDeletion() throws IOException {
        final List<String> evictedPrefixes = new ArrayList<>();
        BlockCache mockCache = new BlockCache() {
            @Override
            public BlockCacheStats stats() {
                return new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L);
            }

            @Override
            public void close() {}

            @Override
            public void evictPrefix(String prefix) {
                evictedPrefixes.add(prefix);
            }
        };
        orchestrator.addBlockCache(mockCache);

        final Path warmShardDataPath = ShardPath.loadShardPath(logger, env, WARM_SHARD_0, "").getDataPath();
        cleaner.beforeShardPathDeleted(WARM_SHARD_0, WARM_INDEX_SETTINGS_0, env);

        assertEquals("evict_prefix must be called exactly once", 1, evictedPrefixes.size());
        assertEquals("evict_prefix must use the shard data path", warmShardDataPath.toString(), evictedPrefixes.get(0));
    }

    /**
     * Block cache eviction is NOT called for remote snapshot shards (isWarmIndex=false).
     */
    public void testBlockCacheNotEvictedForRemoteSnapshotShard() {
        final List<String> evictedPrefixes = new ArrayList<>();
        BlockCache mockCache = new BlockCache() {
            @Override
            public BlockCacheStats stats() {
                return new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L);
            }

            @Override
            public void close() {}

            @Override
            public void evictPrefix(String prefix) {
                evictedPrefixes.add(prefix);
            }
        };
        orchestrator.addBlockCache(mockCache);

        // SHARD_0 uses INDEX_SETTINGS (remote_snapshot, not warm index)
        cleaner.beforeShardPathDeleted(SHARD_0, INDEX_SETTINGS, env);

        assertTrue("evict_prefix must NOT be called for remote snapshot shards", evictedPrefixes.isEmpty());
    }

    /**
     * When multiple block caches are registered, all of them receive evict_prefix
     * when a warm shard is deleted.
     */
    public void testAllBlockCachesEvictedOnWarmShardDeletion() throws IOException {
        final List<String> evicted1 = new ArrayList<>();
        final List<String> evicted2 = new ArrayList<>();

        orchestrator.addBlockCache(new BlockCache() {
            @Override
            public BlockCacheStats stats() {
                return new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L);
            }

            @Override
            public void close() {}

            @Override
            public void evictPrefix(String prefix) {
                evicted1.add(prefix);
            }
        });
        orchestrator.addBlockCache(new BlockCache() {
            @Override
            public BlockCacheStats stats() {
                return new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L);
            }

            @Override
            public void close() {}

            @Override
            public void evictPrefix(String prefix) {
                evicted2.add(prefix);
            }
        });

        final Path warmShardDataPath = ShardPath.loadShardPath(logger, env, WARM_SHARD_0, "").getDataPath();
        cleaner.beforeShardPathDeleted(WARM_SHARD_0, WARM_INDEX_SETTINGS_0, env);

        assertEquals("first cache must receive evict_prefix", 1, evicted1.size());
        assertEquals("second cache must receive evict_prefix", 1, evicted2.size());
        assertEquals(warmShardDataPath.toString(), evicted1.get(0));
        assertEquals(warmShardDataPath.toString(), evicted2.get(0));
    }

    /**
     * Block cache eviction and FileCache eviction happen together in the same
     * beforeShardPathDeleted call — both caches are cleaned atomically.
     */
    public void testBothFileCacheAndBlockCacheEvictedTogether() throws IOException {
        final List<String> blockCacheEvictions = new ArrayList<>();
        orchestrator.addBlockCache(new BlockCache() {
            @Override
            public BlockCacheStats stats() {
                return new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0L, 0L, 0L, 0L);
            }

            @Override
            public void close() {}

            @Override
            public void evictPrefix(String prefix) {
                blockCacheEvictions.add(prefix);
            }
        });

        // Before: 4 files in FileCache, 0 block cache evictions
        MatcherAssert.assertThat(fileCache.size(), equalTo(4L));
        assertTrue(blockCacheEvictions.isEmpty());

        cleaner.beforeShardPathDeleted(WARM_SHARD_0, WARM_INDEX_SETTINGS_0, env);

        // After: FileCache evicted the shard's file, BlockCache evict_prefix called
        MatcherAssert.assertThat(fileCache.size(), equalTo(3L));
        assertNull(fileCache.get(files.get(WARM_SHARD_0)));
        assertEquals(1, blockCacheEvictions.size());
    }

    /**
     * If no block caches are registered (hot nodes), no evict_prefix calls are made
     * and FileCache cleanup proceeds normally.
     */
    public void testNoCrashWhenNoBlockCachesRegistered() throws IOException {
        // orchestrator has no block caches by default
        assertTrue(orchestrator.blockCaches().isEmpty());

        // Should not throw and should clean FileCache normally
        cleaner.beforeShardPathDeleted(WARM_SHARD_0, WARM_INDEX_SETTINGS_0, env);
        MatcherAssert.assertThat(fileCache.size(), equalTo(3L));
        assertNull(fileCache.get(files.get(WARM_SHARD_0)));
    }
}
