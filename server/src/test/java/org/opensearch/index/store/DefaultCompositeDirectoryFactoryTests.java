/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.test.IndexSettingsModule;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultCompositeDirectoryFactoryTests extends BaseRemoteSegmentStoreDirectoryTests {

    private DefaultCompositeDirectoryFactory directoryFactory;
    private IndexSettings indexSettings;
    private ShardPath shardPath;
    private IndexStorePlugin.DirectoryFactory localDirectoryFactory;
    private FSDirectory localDirectory;
    private FileCache fileCache;

    @Before
    public void setup() throws IOException {
        indexSettings = IndexSettingsModule.newIndexSettings("foo", Settings.builder().build());
        Path tempDir = createTempDir().resolve(indexSettings.getUUID()).resolve("0");
        shardPath = new ShardPath(false, tempDir, tempDir, new ShardId(indexSettings.getIndex(), 0));
        localDirectoryFactory = mock(IndexStorePlugin.DirectoryFactory.class);
        localDirectory = FSDirectory.open(createTempDir());
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(10000);
        when(localDirectoryFactory.newDirectory(indexSettings, shardPath)).thenReturn(localDirectory);
        setupRemoteSegmentStoreDirectory();
        populateMetadata();
        remoteSegmentStoreDirectory.init();
    }

    public void testNewDirectory() throws IOException {
        directoryFactory = new DefaultCompositeDirectoryFactory();
        Directory directory = directoryFactory.newDirectory(
            indexSettings,
            shardPath,
            localDirectoryFactory,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool
        );
        assertNotNull(directory);
        assert (directory instanceof CompositeDirectory);
        verify(localDirectoryFactory).newDirectory(indexSettings, shardPath);
    }

}
