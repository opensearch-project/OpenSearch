/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.index.IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY;
import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.SEARACHBLE_SNAPSHOT_BLOCK_SIZE_SETTING;

public class RemoteSnapshotDirectoryFactoryTests extends OpenSearchTestCase {

    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private ThreadPool threadPool;
    private RepositoriesService repositoriesService;
    private FileCache remoteStoreFileCache;
    private Path path;

    private static final String REPO_NAME = "test_repo";
    private static final String INDEX_NAME = "test_index";
    private final static int GIGA_BYTES = 1024 * 1024 * 1024;
    private final static int SHARD_ID = 0;

    @Before
    public void setup() throws IOException {
        repositoriesServiceSupplier = mock(Supplier.class);
        threadPool = new TestThreadPool("test");
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        remoteStoreFileCache = FileCacheFactory.createConcurrentLRUFileCache(GIGA_BYTES, 1);
        path = createTempDir("RemoteSnapshotDirectoryFactoryTests");
    }

    @After
    public void teardown() {
        threadPool.shutdown();
    }

    public void testNewDirectoryBlockException() {
        RemoteSnapshotDirectoryFactory remoteSnapshotDirectoryFactory = new RemoteSnapshotDirectoryFactory(
            repositoriesServiceSupplier,
            threadPool,
            remoteStoreFileCache
        );
        Settings repositorySettings = Settings.builder()
            .put(SEARCHABLE_SNAPSHOT_REPOSITORY.getKey(), REPO_NAME)
            .put(SEARACHBLE_SNAPSHOT_BLOCK_SIZE_SETTING.getKey(), new ByteSizeValue(3, ByteSizeUnit.MB))
            .build();
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        when(repositoriesService.repository(eq(REPO_NAME))).thenReturn(blobStoreRepository);

        IndexSettings settings = newIndexSettings(newIndexMeta(INDEX_NAME, repositorySettings), Settings.EMPTY);
        ShardPath shardPath = new ShardPath(
            false,
            path.resolve(INDEX_NAME).resolve(String.valueOf(SHARD_ID)),
            path.resolve(INDEX_NAME).resolve(String.valueOf(SHARD_ID)),
            new ShardId(INDEX_NAME, INDEX_NAME, SHARD_ID)
        );
        assertThrows(SettingsException.class, () -> remoteSnapshotDirectoryFactory.newDirectory(settings, shardPath));
    }

    private IndexSettings newIndexSettings(IndexMetadata metadata, Settings nodeSettings, Setting<?>... settings) {
        Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        if (settings.length > 0) {
            settingSet.addAll(Arrays.asList(settings));
        }
        return new IndexSettings(metadata, nodeSettings, new IndexScopedSettings(Settings.EMPTY, settingSet));
    }

    private IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(indexSettings)
            .build();
        return IndexMetadata.builder(name).settings(build).build();
    }

}
