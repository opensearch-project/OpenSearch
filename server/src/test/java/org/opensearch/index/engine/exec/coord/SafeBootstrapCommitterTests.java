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
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.SafeBootstrapCommitter;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link SafeBootstrapCommitter}.
 * In exec.coord package to access DataformatAwareCatalogSnapshot's package-private constructor.
 */
public class SafeBootstrapCommitterTests extends OpenSearchTestCase {

    private static boolean discoverAndTrimCalled = false;

    private static void reset() {
        discoverAndTrimCalled = false;
    }

    private static class TestCommitter extends SafeBootstrapCommitter {

        TestCommitter(CommitterConfig config) throws IOException {
            super(config);
        }

        @Override
        protected void discoverAndTrimUnsafeCommits(Store store, Path translogPath) {
            discoverAndTrimCalled = true;
        }

        @Override
        public CommitResult commit(Map<String, String> commitData) {
            return null;
        }

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
        public boolean isCommitManagedFile(String fileName) {
            return false;
        }

        @Override
        public byte[] serializeToCommitFormat(CatalogSnapshot snapshot) {
            throw new UnsupportedOperationException("test stub does not serialize commits");
        }

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

    private EngineConfig buildEngineConfig(Store store, Path translogPath) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardId shardId = new ShardId("test", "_na_", 0);
        EngineConfig.Builder builder = new EngineConfig.Builder().indexSettings(indexSettings)
            .store(store)
            .retentionLeasesSupplier(() -> new RetentionLeases(0, 0, Collections.emptyList()));
        if (translogPath != null) {
            builder.translogConfig(new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false));
        }
        return builder.build();
    }

    public void testThrowsWhenNullEngineConfig() {
        reset();
        expectThrows(IllegalArgumentException.class, () -> new TestCommitter(new CommitterConfig(null, () -> {})));
    }

    public void testThrowsWhenNullTranslogConfig() throws IOException {
        reset();
        Store store = createStore();
        try {
            EngineConfig ec = new EngineConfig.Builder().indexSettings(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY))
                .store(store)
                .retentionLeasesSupplier(() -> new RetentionLeases(0, 0, Collections.emptyList()))
                .build();
            expectThrows(IllegalArgumentException.class, () -> new TestCommitter(new CommitterConfig(ec, () -> {})));
        } finally {
            store.close();
        }
    }

    public void testDiscoverAndTrimCalledWithValidConfig() throws IOException {
        reset();
        Store store = createStore();
        Path translogPath = createTempDir();
        try {
            new TestCommitter(new CommitterConfig(buildEngineConfig(store, translogPath), () -> {}));
            assertTrue(discoverAndTrimCalled);
        } finally {
            store.close();
        }
    }
}
