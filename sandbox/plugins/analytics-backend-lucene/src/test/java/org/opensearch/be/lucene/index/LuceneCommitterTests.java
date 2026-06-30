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
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.be.lucene.stats.LuceneStatsProvider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogFactory;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.plugin.stats.DataFormatStatsProviderRegistry;
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
        String translogUUID = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, 1L);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        Store store = new Store(shardId, indexSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId));
        store.createEmpty(Version.LATEST, translogUUID);
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
        return new CommitterConfig(engineConfig, () -> {});
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
            long genBeforeCommit = committer.getCommitStats().getGeneration();
            Map<String, String> commitData = Map.of("key1", "value1", "key2", "value2", "_snapshot_", "serialized-data");
            committer.commit(new Committer.CommitInput(commitData.entrySet(), null));

            Map<String, String> readBack = committer.getLastCommittedData();

            assertEquals("value1", readBack.get("key1"));
            assertEquals("value2", readBack.get("key2"));
            assertEquals("serialized-data", readBack.get("_snapshot_"));

            CommitStats stats = committer.getCommitStats();
            assertEquals(genBeforeCommit + 1, stats.getGeneration());
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
            committer.commit(new Committer.CommitInput(Map.<String, String>of().entrySet(), null));
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

        expectThrows(
            IllegalStateException.class,
            () -> committer.commit(new Committer.CommitInput(Map.of("key", "value").entrySet(), null))
        );
        config.engineConfig().getStore().close();
    }

    /**
     * Verifies that {@code lucene.commit.commit_failures} increments when {@code commit()}
     * throws, and {@code commit_total} does NOT increment for the failed attempt.
     *
     * <p>Setup: register a {@link LuceneStatsProvider} + tracker against the shard's id
     * so {@link LuceneCommitter#commit} can reach them via the global registry. Close the
     * underlying {@link IndexWriter} directly (without going through {@code committer.close()})
     * — this leaves {@code LuceneCommitter.isClosed=false} so {@code ensureOpen()} still
     * passes, but the next {@code indexWriter.commit()} throws {@code AlreadyClosedException}.
     * That {@code Throwable} lands in our catch block, which sets {@code threw=true} and
     * (in the finally) increments {@code commit_failures} instead of {@code commit_total}.
     *
     * <p>We read counter values by rendering {@code LuceneShardStats#toXContent} to JSON
     * (the class doesn't expose getters; this is the same surface end users observe).
     */
    public void testCommitFailureIncrementsCommitFailuresCounter() throws IOException {
        LuceneStatsProvider provider = new LuceneStatsProvider();
        DataFormatStatsProviderRegistry.INSTANCE.register(provider);

        CommitterConfig config = createCommitterConfig();
        ShardId shardId = config.engineConfig().getShardId();
        LuceneShardStatsTracker tracker = new LuceneShardStatsTracker();
        provider.register(shardId, tracker);

        LuceneCommitter committer = new LuceneCommitter(config);
        try {
            // Sanity baseline: both counters at zero before the failure.
            assertEquals("baseline commit_total", 0L, readCounter(tracker, "commit_total"));
            assertEquals("baseline commit_failures", 0L, readCounter(tracker, "commit_failures"));

            // Force the IndexWriter into a closed state. The committer's own isClosed flag
            // remains false (we did NOT call committer.close()) so ensureOpen() inside
            // commit() will pass; but indexWriter.commit() will throw AlreadyClosedException.
            committer.getIndexWriter().close();

            expectThrows(Throwable.class, () -> committer.commit(new Committer.CommitInput(Map.<String, String>of().entrySet(), null)));

            // Contract: commit_failures incremented by exactly 1, commit_total still 0.
            assertEquals("commit_failures must be 1 after one failed commit", 1L, readCounter(tracker, "commit_failures"));
            assertEquals("commit_total must still be 0 (failed attempt does not count)", 0L, readCounter(tracker, "commit_total"));
        } finally {
            provider.unregister(shardId);
            try {
                committer.close();
            } catch (Throwable ignored) {
                // committer.close() may complain because IndexWriter is already closed
            }
            config.engineConfig().getStore().close();
        }
    }

    /** Reads a single counter from a tracker by rendering its snapshot to JSON. */
    @SuppressWarnings("unchecked")
    private static long readCounter(LuceneShardStatsTracker tracker, String fieldName) throws IOException {
        var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder().startObject();
        tracker.stats().toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
            org.opensearch.common.xcontent.json.JsonXContent.jsonXContent,
            builder.toString(),
            true
        );
        // The field lives one level deep (e.g., parsed.commit.commit_failures).
        for (Object section : parsed.values()) {
            if (section instanceof Map<?, ?> map && map.containsKey(fieldName)) {
                return ((Number) map.get(fieldName)).longValue();
            }
        }
        throw new AssertionError("field not found: " + fieldName);
    }
}
