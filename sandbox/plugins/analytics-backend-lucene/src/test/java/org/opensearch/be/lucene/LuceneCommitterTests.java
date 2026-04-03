/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link LuceneCommitter}.
 */
public class LuceneCommitterTests extends OpenSearchTestCase {

    private CommitterSettings createCommitterSettings() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        return new CommitterSettings(shardPath, indexSettings);
    }

    public void testInitOpensIndexWriter() throws IOException {
        LuceneCommitter committer = new LuceneCommitter();
        try {
            assertNull(committer.getIndexWriter());
            committer.init(createCommitterSettings());
            IndexWriter writer = committer.getIndexWriter();
            assertNotNull(writer);
            assertTrue(writer.isOpen());
        } finally {
            committer.close();
        }
    }

    public void testCloseReleasesIndexWriter() throws IOException {
        LuceneCommitter committer = new LuceneCommitter();
        committer.init(createCommitterSettings());
        assertNotNull(committer.getIndexWriter());

        committer.close();
        assertNull(committer.getIndexWriter());
    }

    public void testCommitRoundTrip() throws IOException {
        LuceneCommitter committer = new LuceneCommitter();
        try {
            committer.init(createCommitterSettings());

            Map<String, String> commitData = Map.of("key1", "value1", "key2", "value2", "_snapshot_", "serialized-data");
            committer.commit(commitData);

            Map<String, String> readBack = new HashMap<>();
            for (Map.Entry<String, String> entry : committer.getIndexWriter().getLiveCommitData()) {
                readBack.put(entry.getKey(), entry.getValue());
            }

            assertEquals("value1", readBack.get("key1"));
            assertEquals("value2", readBack.get("key2"));
            assertEquals("serialized-data", readBack.get("_snapshot_"));
        } finally {
            committer.close();
        }
    }

    public void testCommitWithEmptyData() throws IOException {
        LuceneCommitter committer = new LuceneCommitter();
        try {
            committer.init(createCommitterSettings());

            committer.commit(Map.of());

            Map<String, String> readBack = new HashMap<>();
            for (Map.Entry<String, String> entry : committer.getIndexWriter().getLiveCommitData()) {
                readBack.put(entry.getKey(), entry.getValue());
            }

            assertTrue(readBack.isEmpty());
        } finally {
            committer.close();
        }
    }

    public void testCommitAfterCloseThrows() throws IOException {
        LuceneCommitter committer = new LuceneCommitter();
        committer.init(createCommitterSettings());
        committer.close();

        expectThrows(IllegalStateException.class, () -> committer.commit(Map.of("key", "value")));
    }
}
