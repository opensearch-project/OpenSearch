/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tests for {@link LuceneCommitterFactory}.
 */
public class LuceneCommitterFactoryTests extends OpenSearchTestCase {

    /**
     * Test that getCommitter() returns a non-empty Optional containing
     * a LuceneCommitter instance.
     */
    public void testGetCommitterReturnsLuceneCommitter() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        Store store = new Store(shardId, indexSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId));

        Committer committer = null;
        try {
            LuceneCommitterFactory committerFactory = new LuceneCommitterFactory();
            CommitterConfig settings = new CommitterConfig(indexSettings, null, store, CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY);
            committer = committerFactory.getCommitter(settings);

            assertTrue("getCommitter() should return a LuceneCommitter instance", committer instanceof LuceneCommitter);
        } finally {
            if (committer != null) {
                committer.close();
                ;
            }
            store.close();
        }
    }
}
