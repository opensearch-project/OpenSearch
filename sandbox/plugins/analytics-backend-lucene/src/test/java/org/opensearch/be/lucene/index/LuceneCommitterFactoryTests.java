/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/**
 * Tests for {@link LuceneCommitterFactory}.
 */
public class LuceneCommitterFactoryTests extends OpenSearchTestCase {

    public void testGetCommitterReturnsLuceneCommitter() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        Path translogPath = dataPath.resolve("translog");
        Files.createDirectories(translogPath);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        Store store = new Store(shardId, indexSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId));
        store.createEmpty(Version.LATEST);

        Committer committer = null;
        try {
            EngineConfig engineConfig = new EngineConfig.Builder().indexSettings(indexSettings)
                .store(store)
                .codecService(new CodecService(null, indexSettings, LogManager.getLogger(getClass()), java.util.List.of()))
                .translogConfig(new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "", false))
                .retentionLeasesSupplier(() -> new RetentionLeases(0, 0, Collections.emptyList()))
                .build();
            LuceneCommitterFactory committerFactory = new LuceneCommitterFactory();
            committer = committerFactory.getCommitter(new CommitterConfig(engineConfig));

            assertTrue("getCommitter() should return a LuceneCommitter instance", committer instanceof LuceneCommitter);
        } finally {
            if (committer != null) {
                committer.close();
            }
            store.close();
        }
    }
}
