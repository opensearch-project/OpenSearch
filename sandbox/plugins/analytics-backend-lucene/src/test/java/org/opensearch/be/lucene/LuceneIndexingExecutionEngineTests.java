/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
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
 * Tests for {@link LuceneIndexingExecutionEngine}.
 */
public class LuceneIndexingExecutionEngineTests extends OpenSearchTestCase {

    private LuceneCommitter committer;
    private Store store;

    private LuceneCommitter createCommitter() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        store = new Store(shardId, indexSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId));

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
            null
        );
        CommitterConfig settings = new CommitterConfig(engineConfig);
        return new LuceneCommitter(settings);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        committer = createCommitter();
    }

    @Override
    public void tearDown() throws Exception {
        if (committer != null) {
            committer.close();
        }
        if (store != null) {
            store.close();
        }
        super.tearDown();
    }

    /**
     * Property 1: refresh incorporates Lucene segments when parent writer is present.
     * Validates: Requirements 2.3
     */
    public void testRefreshIncorporatesLuceneSegments() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer, store);
        IndexWriter writer = committer.getIndexWriter();
        assertEquals(0, writer.getDocStats().numDocs);

        int numDocs = randomIntBetween(1, 20);
        Path externalDir = createTempDir().resolve("lucene");
        Files.createDirectories(externalDir);
        try (
            Directory extDirectory = FSDirectory.open(externalDir);
            IndexWriter extWriter = new IndexWriter(extDirectory, new IndexWriterConfig())
        ) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", "doc_" + i, Field.Store.YES));
                extWriter.addDocument(doc);
            }
            extWriter.commit();
        }

        WriterFileSet writerFileSet = WriterFileSet.builder().directory(externalDir).writerGeneration(1L).addNumRows(numDocs).build();
        Segment segment = Segment.builder(1L).addSearchableFiles("lucene", writerFileSet).build();
        RefreshInput refreshInput = RefreshInput.builder().addSegment(segment).build();

        RefreshResult result = engine.refresh(refreshInput);
        assertNotNull(result);
        assertEquals(numDocs, writer.getDocStats().numDocs);
    }

    /**
     * Refresh skips WriterFileSets whose directory does not match the Lucene data format name.
     */
    public void testRefreshSkipsNonLuceneDirectories() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer, store);
        IndexWriter writer = committer.getIndexWriter();

        Path parquetDir = createTempDir().resolve("parquet");
        Files.createDirectories(parquetDir);
        try (
            Directory extDirectory = FSDirectory.open(parquetDir);
            IndexWriter extWriter = new IndexWriter(extDirectory, new IndexWriterConfig())
        ) {
            Document doc = new Document();
            doc.add(new StringField("id", "doc_0", Field.Store.YES));
            extWriter.addDocument(doc);
            extWriter.commit();
        }

        WriterFileSet writerFileSet = WriterFileSet.builder().directory(parquetDir).writerGeneration(1L).addNumRows(1).build();
        Segment segment = Segment.builder(1L).addSearchableFiles("parquet", writerFileSet).build();
        RefreshInput refreshInput = RefreshInput.builder().addSegment(segment).build();

        engine.refresh(refreshInput);
        assertEquals("Non-lucene directories should be skipped", 0, writer.getDocStats().numDocs);
    }

    /**
     * Refresh with no files skips addIndexes.
     */
    public void testRefreshWithNoLuceneFilesSkipsAddIndexes() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer, store);

        RefreshInput emptyInput = RefreshInput.builder().build();
        RefreshResult result = engine.refresh(emptyInput);
        assertNotNull(result);
        assertEquals(0, committer.getIndexWriter().getDocStats().numDocs);
    }

    /**
     * Constructor with null committer throws IllegalArgumentException.
     */
    public void testConstructorWithNullCommitterThrows() {
        expectThrows(IllegalArgumentException.class, () -> new LuceneIndexingExecutionEngine(null, null));
    }

    /**
     * Refresh with null input returns empty result.
     */
    public void testRefreshWithNullInputReturnsEmpty() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer, store);

        RefreshResult result = engine.refresh(null);
        assertNotNull(result);
        assertTrue(result.refreshedSegments().isEmpty());
    }

    /**
     * deleteFiles removes lucene-format files from the store directory.
     */
    public void testDeleteFilesRemovesLuceneFiles() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer, store);
        org.apache.lucene.store.Directory directory = store.directory();

        directory.createOutput("_0.cfs", org.apache.lucene.store.IOContext.DEFAULT).close();
        directory.createOutput("_0.cfe", org.apache.lucene.store.IOContext.DEFAULT).close();
        assertTrue(java.util.Set.of(directory.listAll()).contains("_0.cfs"));
        assertTrue(java.util.Set.of(directory.listAll()).contains("_0.cfe"));

        engine.deleteFiles(java.util.Map.of("lucene", java.util.List.of("_0.cfs", "_0.cfe")));

        assertFalse(java.util.Set.of(directory.listAll()).contains("_0.cfs"));
        assertFalse(java.util.Set.of(directory.listAll()).contains("_0.cfe"));
    }

    /**
     * deleteFiles ignores non-lucene format keys.
     */
    public void testDeleteFilesIgnoresNonLuceneFormat() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer, store);
        engine.deleteFiles(java.util.Map.of("parquet", java.util.List.of("_0.parquet")));
    }

    /**
     * deleteFiles silently ignores files already deleted by Lucene (e.g., during merge).
     */
    public void testDeleteFilesSilentlyIgnoresMissingFiles() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer, store);
        engine.deleteFiles(java.util.Map.of("lucene", java.util.List.of("_nonexistent.cfs")));
    }
}
