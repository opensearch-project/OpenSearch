/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.breaker.CircuitBreaker;
import org.opensearch.common.breaker.NoopCircuitBreaker;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;

public class FileCacheCleanerTests extends OpenSearchTestCase {
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

    private final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(
        1024 * 1024 * 1024,
        1,
        new NoopCircuitBreaker(CircuitBreaker.REQUEST)
    );
    private final Map<ShardId, Path> files = new HashMap<>();
    private NodeEnvironment env;
    private FileCacheCleaner cleaner;

    @Before
    public void setUpFileCache() throws IOException {
        env = newNodeEnvironment(SETTINGS);
        cleaner = new FileCacheCleaner(env, fileCache);
        files.put(SHARD_0, addFile(fileCache, env, SHARD_0));
        files.put(SHARD_1, addFile(fileCache, env, SHARD_1));
        MatcherAssert.assertThat(fileCache.size(), equalTo(2L));
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

    @After
    public void tearDownFileCache() {
        env.close();
    }

    public void testShardRemoved() {
        final Path cachePath = ShardPath.loadFileCachePath(env, SHARD_0).getDataPath();
        assertTrue(Files.exists(cachePath));

        cleaner.beforeIndexShardDeleted(SHARD_0, SETTINGS);
        MatcherAssert.assertThat(fileCache.size(), equalTo(1L));
        assertNull(fileCache.get(files.get(SHARD_0)));
        assertFalse(Files.exists(files.get(SHARD_0)));
        assertTrue(Files.exists(files.get(SHARD_1)));
        cleaner.afterIndexShardDeleted(SHARD_0, SETTINGS);
        assertFalse(Files.exists(cachePath));
    }

    public void testIndexRemoved() {
        final Path indexCachePath = env.fileCacheNodePath().fileCachePath.resolve(SHARD_0.getIndex().getUUID());
        assertTrue(Files.exists(indexCachePath));

        cleaner.beforeIndexShardDeleted(SHARD_0, SETTINGS);
        cleaner.afterIndexShardDeleted(SHARD_0, SETTINGS);
        cleaner.beforeIndexShardDeleted(SHARD_1, SETTINGS);
        cleaner.afterIndexShardDeleted(SHARD_1, SETTINGS);
        cleaner.afterIndexRemoved(
            SHARD_0.getIndex(),
            INDEX_SETTINGS,
            IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED
        );
        MatcherAssert.assertThat(fileCache.size(), equalTo(0L));
        assertFalse(Files.exists(indexCachePath));
    }
}
