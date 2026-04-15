/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.util.Version;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogFactory;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LuceneCommitter}.
 */
public class LuceneCommitterTests extends OpenSearchTestCase {

    private CommitterConfig createCommitterConfig() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        Path translogPath = dataPath.resolve("translog");
        Files.createDirectories(translogPath);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        Store store = new Store(shardId, indexSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId));
        store.createEmpty(Version.LATEST);
        PluginsService mockPluginsService = mock(PluginsService.class);
        when(mockPluginsService.filterPlugins(EnginePlugin.class)).thenReturn(List.of(new LucenePlugin()));

        EngineConfig engineConfig = new EngineConfigFactory(mockPluginsService, indexSettings).newEngineConfig(
            shardId,
            null,
            indexSettings,
            null,
            store,
            null,
            new MockAnalyzer(random()),
            null,
            new CodecService(null, indexSettings, LogManager.getLogger(getClass()), List.of()),
            null,
            null,
            null,
            new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false),
            TimeValue.timeValueMinutes(5),
            null,
            null,
            null,
            null,
            null,
            () -> new RetentionLeases(0, 0, Collections.emptyList()),
            null,
            null,
            false,
            () -> Boolean.TRUE,
            new InternalTranslogFactory(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        return new CommitterConfig(engineConfig);
    }

    public void testConstructorOpensIndexWriter() throws IOException {
        CommitterConfig settings = createCommitterConfig();
        LuceneCommitter committer = new LuceneCommitter(settings);
        try {
            IndexWriter writer = committer.getIndexWriter();
            assertNotNull(writer);
            assertTrue(writer.isOpen());
        } finally {
            committer.close();
            settings.engineConfig().getStore().close();
        }
    }

    public void testCloseReleasesIndexWriter() throws IOException {
        CommitterConfig settings = createCommitterConfig();
        LuceneCommitter committer = new LuceneCommitter(settings);
        assertNotNull(committer.getIndexWriter());

        committer.close();
        expectThrows(IllegalStateException.class, committer::getIndexWriter);
        settings.engineConfig().getStore().close();
    }

    public void testCommitRoundTrip() throws IOException {
        CommitterConfig settings = createCommitterConfig();
        LuceneCommitter committer = new LuceneCommitter(settings);
        try {
            Map<String, String> commitData = Map.of("key1", "value1", "key2", "value2", "_snapshot_", "serialized-data");
            committer.commit(commitData);

            Map<String, String> readBack = committer.getLastCommittedData();

            assertEquals("value1", readBack.get("key1"));
            assertEquals("value2", readBack.get("key2"));
            assertEquals("serialized-data", readBack.get("_snapshot_"));

            CommitStats stats = committer.getCommitStats();
            assertEquals(2L, stats.getGeneration());
            assertEquals(readBack, stats.getUserData());
        } finally {
            committer.close();
            settings.engineConfig().getStore().close();
        }
    }

    public void testCommitWithEmptyData() throws IOException {
        CommitterConfig settings = createCommitterConfig();
        LuceneCommitter committer = new LuceneCommitter(settings);
        try {
            committer.commit(Map.of());
            assertTrue(committer.getLastCommittedData().isEmpty());
        } finally {
            committer.close();
            settings.engineConfig().getStore().close();
        }
    }

    public void testCommitAfterCloseThrows() throws IOException {
        CommitterConfig config = createCommitterConfig();
        LuceneCommitter committer = new LuceneCommitter(config);
        committer.close();

        expectThrows(IllegalStateException.class, () -> committer.commit(Map.of("key", "value")));
        config.engineConfig().getStore().close();
    }
}
