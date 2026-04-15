/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogFactory;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LuceneCommitterFactory}.
 */
public class LuceneCommitterFactoryTests extends OpenSearchTestCase {

    /**
     * Test that getCommitter() returns a LuceneCommitter instance.
     */
    public void testGetCommitterReturnsLuceneCommitter() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        Store store = new Store(shardId, indexSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId));

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
            null,
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

        Committer committer = null;
        try {
            LuceneCommitterFactory committerFactory = new LuceneCommitterFactory();
            CommitterConfig settings = new CommitterConfig(engineConfig);
            committer = committerFactory.getCommitter(settings);

            assertTrue("getCommitter() should return a LuceneCommitter instance", committer instanceof LuceneCommitter);
        } finally {
            if (committer != null) {
                committer.close();
            }
            store.close();
        }
    }
}
