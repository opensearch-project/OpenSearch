/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.FSDirectory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;

/**
 * Unit tests for {@link DefaultDataFormatAwareStoreDirectoryFactory}.
 */
public class DefaultDataFormatAwareStoreDirectoryFactoryTests extends OpenSearchTestCase {

    private ShardPath createShardPath(Path tempDir) throws IOException {
        String indexUUID = "test-index-uuid";
        int shardId = 0;
        Path shardDataPath = tempDir.resolve(indexUUID).resolve(Integer.toString(shardId));
        Path indexPath = shardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME);
        Files.createDirectories(indexPath);

        ShardId sid = new ShardId(new Index("test-index", indexUUID), shardId);
        return new ShardPath(false, shardDataPath, shardDataPath, sid);
    }

    private IndexSettings createIndexSettings() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(SETTING_INDEX_UUID, "test-index-uuid")
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test-index").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }

    private IndexStorePlugin.DirectoryFactory createFsDirectoryFactory() {
        return new IndexStorePlugin.DirectoryFactory() {
            @Override
            public org.apache.lucene.store.Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
                return FSDirectory.open(shardPath.resolveIndex());
            }

            @Override
            public org.apache.lucene.store.Directory newFSDirectory(
                Path location,
                org.apache.lucene.store.LockFactory lockFactory,
                IndexSettings indexSettings
            ) throws IOException {
                return FSDirectory.open(location, lockFactory);
            }
        };
    }

    // ═══════════════════════════════════════════════════════════════
    // newDataFormatAwareStoreDirectory Tests
    // ═══════════════════════════════════════════════════════════════

    public void testNewDataFormatAwareStoreDirectory_CreatesSuccessfully() throws IOException {
        DefaultDataFormatAwareStoreDirectoryFactory factory = new DefaultDataFormatAwareStoreDirectoryFactory();
        Path tempDir = createTempDir();
        ShardPath shardPath = createShardPath(tempDir);
        IndexSettings indexSettings = createIndexSettings();

        DataFormatAwareStoreDirectory directory = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardPath.getShardId(),
            shardPath,
            createFsDirectoryFactory(),
            Map.of()
        );

        assertNotNull("Factory should create a non-null DataFormatAwareStoreDirectory", directory);
    }

    public void testNewDataFormatAwareStoreDirectory_HasCorrectShardPath() throws IOException {
        DefaultDataFormatAwareStoreDirectoryFactory factory = new DefaultDataFormatAwareStoreDirectoryFactory();
        Path tempDir = createTempDir();
        ShardPath shardPath = createShardPath(tempDir);
        IndexSettings indexSettings = createIndexSettings();

        DataFormatAwareStoreDirectory directory = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardPath.getShardId(),
            shardPath,
            createFsDirectoryFactory(),
            Map.of()
        );

        assertEquals(shardPath, directory.getShardPath());
    }

    public void testNewDataFormatAwareStoreDirectory_CanListFiles() throws IOException {
        DefaultDataFormatAwareStoreDirectoryFactory factory = new DefaultDataFormatAwareStoreDirectoryFactory();
        Path tempDir = createTempDir();
        ShardPath shardPath = createShardPath(tempDir);
        IndexSettings indexSettings = createIndexSettings();

        DataFormatAwareStoreDirectory directory = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardPath.getShardId(),
            shardPath,
            createFsDirectoryFactory(),
            Map.of()
        );

        // Should not throw
        String[] files = directory.listAll();
        assertNotNull(files);
    }

    public void testNewDataFormatAwareStoreDirectory_MultipleCalls_CreatesSeparateInstances() throws IOException {
        DefaultDataFormatAwareStoreDirectoryFactory factory = new DefaultDataFormatAwareStoreDirectoryFactory();
        Path tempDir1 = createTempDir();
        Path tempDir2 = createTempDir();
        ShardPath shardPath1 = createShardPath(tempDir1);
        ShardPath shardPath2 = createShardPath(tempDir2);
        IndexSettings indexSettings = createIndexSettings();

        DataFormatAwareStoreDirectory dir1 = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardPath1.getShardId(),
            shardPath1,
            createFsDirectoryFactory(),
            Map.of()
        );
        DataFormatAwareStoreDirectory dir2 = factory.newDataFormatAwareStoreDirectory(
            indexSettings,
            shardPath2.getShardId(),
            shardPath2,
            createFsDirectoryFactory(),
            Map.of()
        );

        assertNotNull(dir1);
        assertNotNull(dir2);
        assertNotSame("Each call should create a new instance", dir1, dir2);
    }

    public void testNewDataFormatAwareStoreDirectory_InvalidPath_ThrowsIOException() throws IOException {
        DefaultDataFormatAwareStoreDirectoryFactory factory = new DefaultDataFormatAwareStoreDirectoryFactory();
        IndexSettings indexSettings = createIndexSettings();

        // Create a valid shard path structure (must end with shardId, parent with indexUUID)
        // but place a regular file where the "index" directory should be, so FSDirectory.open() fails
        String indexUUID = "test-index-uuid";
        int shardId = 0;
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve(indexUUID).resolve(Integer.toString(shardId));
        Files.createDirectories(shardDataPath);
        // Create a FILE named "index" instead of a directory — FSDirectory.open() will fail
        Path indexFile = shardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME);
        Files.createFile(indexFile);

        ShardId sid = new ShardId(new Index("test-index", indexUUID), shardId);
        ShardPath invalidShardPath = new ShardPath(false, shardDataPath, shardDataPath, sid);

        // This should trigger the catch block which wraps the exception as IOException
        IOException exception = expectThrows(
            IOException.class,
            () -> factory.newDataFormatAwareStoreDirectory(
                indexSettings,
                invalidShardPath.getShardId(),
                invalidShardPath,
                createFsDirectoryFactory(),
                Map.of()
            )
        );
        assertTrue(
            "Exception message should mention shard, but was: " + exception.getMessage(),
            exception.getMessage().contains("Failed to create DataFormatAwareStoreDirectory for shard")
        );
    }
}
