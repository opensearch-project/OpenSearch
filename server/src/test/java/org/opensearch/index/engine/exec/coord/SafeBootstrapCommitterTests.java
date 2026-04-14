/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.SafeBootstrapCommitter;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for {@link SafeBootstrapCommitter}.
 * In exec.coord package to access DataformatAwareCatalogSnapshot's package-private constructor.
 */
public class SafeBootstrapCommitterTests extends OpenSearchTestCase {

    /**
     * Concrete test subclass. Since instance fields are not initialized when super() calls
     * the abstract method, we use static fields set before each test.
     * Tests are single-threaded per instance so static fields are safe here.
     */
    private static boolean discoverAndTrimCalled = false;

    private static void reset() {
        discoverAndTrimCalled = false;
    }

    private static class TestCommitter extends SafeBootstrapCommitter {

        TestCommitter(CommitterConfig config) throws IOException {
            super(config);
        }

        @Override
        protected void discoverAndTrimUnsafeCommits(Store store, CatalogSnapshotDeletionPolicy policy) {
            discoverAndTrimCalled = true;
        }

        @Override
        public void commit(Map<String, String> commitData) {}

        @Override
        public Map<String, String> getLastCommittedData() {
            return Map.of();
        }

        @Override
        public CommitStats getCommitStats() {
            return null;
        }

        @Override
        public SafeCommitInfo getSafeCommitInfo() {
            return SafeCommitInfo.EMPTY;
        }

        @Override
        public List<CatalogSnapshot> listCommittedSnapshots() {
            return List.of();
        }

        @Override
        public void deleteCommit(CatalogSnapshot snapshot) {}

        @Override
        public void close() {}
    }

    private Store createStore() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        return new Store(shardId, indexSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId), Store.OnClose.EMPTY, shardPath);
    }

    private CombinedCatalogSnapshotDeletionPolicy createPolicy(long globalCheckpoint) {
        return new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            new AtomicLong(globalCheckpoint)::get
        );
    }

    public void testThrowsWhenNullStore() {
        reset();
        CatalogSnapshotDeletionPolicy policy = createPolicy(100);
        expectThrows(IllegalArgumentException.class, () -> new TestCommitter(new CommitterConfig(null, null, null, policy)));
    }

    public void testThrowsWhenNullDeletionPolicy() throws IOException {
        reset();
        Store store = createStore();
        try {
            expectThrows(IllegalArgumentException.class, () -> new TestCommitter(new CommitterConfig(null, null, store, null)));
        } finally {
            store.close();
        }
    }

    public void testDiscoverAndTrimCalledWithValidConfig() throws IOException {
        reset();
        Store store = createStore();
        try {
            CatalogSnapshotDeletionPolicy policy = createPolicy(100);
            new TestCommitter(new CommitterConfig(null, null, store, policy));
            assertTrue(discoverAndTrimCalled);
        } finally {
            store.close();
        }
    }
}
