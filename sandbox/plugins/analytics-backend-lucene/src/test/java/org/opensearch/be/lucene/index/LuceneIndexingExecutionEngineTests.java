/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.shard.ShardPath;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LuceneIndexingExecutionEngine}.
 */
public class LuceneIndexingExecutionEngineTests extends OpenSearchTestCase {

    private LuceneCommitter committer;
    private Store store;
    private ShardPath shardPath;
    private MapperService mapperService;

    private MapperService createMockMapperService() {
        MapperService ms = mock(MapperService.class);
        when(ms.indexAnalyzer()).thenReturn(new MockAnalyzer(random()));
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        when(ms.getIndexSettings()).thenReturn(indexSettings);
        return ms;
    }

    private LuceneCommitter createCommitter() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        store = new Store(
            shardId,
            indexSettings,
            new NIOFSDirectory(dataPath),
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath
        );
        store.createEmpty(org.apache.lucene.util.Version.LATEST);

        PluginsService mockPluginsService = mock(PluginsService.class);
        when(mockPluginsService.filterPlugins(EnginePlugin.class)).thenReturn(List.of(new LucenePlugin()));

        Path translogPath = dataPath.resolve("translog");
        java.nio.file.Files.createDirectories(translogPath);
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
        CommitterConfig settings = new CommitterConfig(engineConfig);
        return new LuceneCommitter(settings);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        committer = createCommitter();
        mapperService = createMockMapperService();
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
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(luceneDataFormat, committer, mapperService, store);
        IndexWriter writer = committer.getIndexWriter();
        assertEquals(0, writer.getDocStats().numDocs);

        int numDocs = randomIntBetween(1, 20);

        // Use LuceneWriter to create segments (which sets the writer_generation attribute via LuceneWriterCodec)
        Path tempBase = createTempDir();
        MappedFieldType textField = mock(MappedFieldType.class);
        when(textField.typeName()).thenReturn("text");
        when(textField.name()).thenReturn("content");

        long generation = 1L;
        try (LuceneWriter luceneWriter = new LuceneWriter(generation, luceneDataFormat, tempBase, null, Codec.getDefault(), null)) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                luceneWriter.addDoc(input);
            }

            FileInfos fileInfos = luceneWriter.flush();
            WriterFileSet wfs = fileInfos.getWriterFileSet(luceneDataFormat).get();

            // Build a Segment from the FileInfos
            Segment segment = Segment.builder(generation).addSearchableFiles(luceneDataFormat, wfs).build();
            RefreshInput refreshInput = RefreshInput.builder().addSegment(segment).build();

            RefreshResult result = engine.refresh(refreshInput);
            assertNotNull(result);
            assertEquals(numDocs, writer.getDocStats().numDocs);

            // Verify the result has segments with actual file names from the shared directory
            assertFalse("Result should have segments", result.refreshedSegments().isEmpty());
            Segment resultSegment = result.refreshedSegments().get(0);
            WriterFileSet resultWfs = resultSegment.dfGroupedSearchableFiles().get(LuceneDataFormat.LUCENE_FORMAT_NAME);
            assertNotNull("Result segment should have lucene files", resultWfs);
            assertEquals(generation, resultWfs.writerGeneration());
            assertEquals(numDocs, resultWfs.numRows());
            // The directory should be the shared writer's directory (store index path)
            assertEquals(store.shardPath().resolveIndex().toString(), resultWfs.directory());
            assertFalse("Result should have file names", resultWfs.files().isEmpty());
        }
    }

    /**
     * Refresh skips WriterFileSets whose directory does not match the Lucene data format name.
     */
    public void testRefreshSkipsNonLuceneDirectories() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(new LuceneDataFormat(), committer, mapperService, store);
        IndexWriter writer = committer.getIndexWriter();

        // Create a segment directory that looks like it has files but is keyed under "parquet"
        Path parquetDir = createTempDir().resolve("parquet");
        Files.createDirectories(parquetDir);

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
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(new LuceneDataFormat(), committer, mapperService, store);

        RefreshInput emptyInput = RefreshInput.builder().build();
        RefreshResult result = engine.refresh(emptyInput);
        assertNotNull(result);
        assertEquals(0, committer.getIndexWriter().getDocStats().numDocs);
    }

    /**
     * Constructor with null committer throws IllegalArgumentException.
     */
    public void testConstructorWithNullCommitterThrows() {
        expectThrows(IllegalArgumentException.class, () -> new LuceneIndexingExecutionEngine(null, null, null, null));
    }

    /**
     * Refresh with null input returns empty result.
     */
    public void testRefreshWithNullInputReturnsEmpty() throws IOException {
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(new LuceneDataFormat(), committer, mapperService, store);

        RefreshResult result = engine.refresh(null);
        assertNotNull(result);
        assertTrue(result.refreshedSegments().isEmpty());
    }

    /**
     * Use engine.createWriter() to create a writer, add docs, flush, build segment,
     * call engine.refresh(). Verify the shared writer has the docs and the result
     * segments have correct file names from the shared directory.
     */
    public void testWriterCreationAndFlushThroughEngine() throws IOException {
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(luceneDataFormat, committer, mapperService, store);
        IndexWriter sharedWriter = committer.getIndexWriter();
        assertEquals(0, sharedWriter.getDocStats().numDocs);

        int numDocs = randomIntBetween(3, 15);
        long generation = 1L;

        MappedFieldType textField = mock(MappedFieldType.class);
        when(textField.typeName()).thenReturn("text");
        when(textField.name()).thenReturn("content");

        MappedFieldType keywordField = mock(MappedFieldType.class);
        when(keywordField.typeName()).thenReturn("keyword");
        when(keywordField.name()).thenReturn("tag");
        when(keywordField.hasDocValues()).thenReturn(true);

        // Create writer through the engine
        LuceneWriter writer = (LuceneWriter) engine.createWriter(generation);
        try {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = engine.newDocumentInput();
                input.addField(textField, "content value " + i);
                input.addField(keywordField, "tag_" + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush();
            WriterFileSet wfs = fileInfos.getWriterFileSet(luceneDataFormat).get();

            // Build segment and refresh
            Segment segment = Segment.builder(generation).addSearchableFiles(luceneDataFormat, wfs).build();
            RefreshInput refreshInput = RefreshInput.builder().addSegment(segment).build();

            RefreshResult result = engine.refresh(refreshInput);

            // Verify shared writer has the docs
            assertEquals(numDocs, sharedWriter.getDocStats().numDocs);

            // Verify result segments have correct file names from the shared directory
            assertFalse(result.refreshedSegments().isEmpty());
            Segment resultSegment = result.refreshedSegments().get(0);
            WriterFileSet resultWfs = resultSegment.dfGroupedSearchableFiles().get(LuceneDataFormat.LUCENE_FORMAT_NAME);
            assertNotNull(resultWfs);
            assertEquals(generation, resultWfs.writerGeneration());
            assertEquals(numDocs, resultWfs.numRows());
            assertEquals(store.shardPath().resolveIndex().toString(), resultWfs.directory());
            assertFalse(resultWfs.files().isEmpty());
        } finally {
            writer.close();
        }
    }

    /**
     * Create 2 writers via the engine, write docs to each, flush each, refresh with both.
     * Verify the shared writer has all docs and the result has 2 segments with correct generations.
     */
    public void testMultipleWriterRefreshAccumulatesInSharedWriter() throws IOException {
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(luceneDataFormat, committer, mapperService, store);
        IndexWriter sharedWriter = committer.getIndexWriter();

        MappedFieldType textField = mock(MappedFieldType.class);
        when(textField.typeName()).thenReturn("text");
        when(textField.name()).thenReturn("body");

        long gen1 = 1L;
        long gen2 = 2L;
        int numDocs1 = randomIntBetween(3, 10);
        int numDocs2 = randomIntBetween(3, 10);

        // Create writers through the engine — do NOT close them before refresh,
        // because close() deletes the temp directory that refresh needs to read.
        LuceneWriter writer1 = (LuceneWriter) engine.createWriter(gen1);
        LuceneWriter writer2 = (LuceneWriter) engine.createWriter(gen2);
        try {
            for (int i = 0; i < numDocs1; i++) {
                LuceneDocumentInput input = engine.newDocumentInput();
                input.addField(textField, "writer1 doc " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer1.addDoc(input);
            }
            FileInfos fileInfos1 = writer1.flush();

            for (int i = 0; i < numDocs2; i++) {
                LuceneDocumentInput input = engine.newDocumentInput();
                input.addField(textField, "writer2 doc " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer2.addDoc(input);
            }
            FileInfos fileInfos2 = writer2.flush();

            // Build segments and refresh with both
            WriterFileSet wfs1 = fileInfos1.getWriterFileSet(luceneDataFormat).get();
            WriterFileSet wfs2 = fileInfos2.getWriterFileSet(luceneDataFormat).get();

            Segment segment1 = Segment.builder(gen1).addSearchableFiles(luceneDataFormat, wfs1).build();
            Segment segment2 = Segment.builder(gen2).addSearchableFiles(luceneDataFormat, wfs2).build();

            RefreshInput refreshInput = RefreshInput.builder().addSegment(segment1).addSegment(segment2).build();
            RefreshResult result = engine.refresh(refreshInput);

            // Verify shared writer has all docs
            assertEquals(numDocs1 + numDocs2, sharedWriter.getDocStats().numDocs);

            // Verify result has 2 segments with correct generations
            assertEquals(2, result.refreshedSegments().size());

            boolean foundGen1 = false;
            boolean foundGen2 = false;
            for (Segment seg : result.refreshedSegments()) {
                WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(LuceneDataFormat.LUCENE_FORMAT_NAME);
                assertNotNull(wfs);
                if (wfs.writerGeneration() == gen1) {
                    foundGen1 = true;
                    assertEquals(numDocs1, wfs.numRows());
                } else if (wfs.writerGeneration() == gen2) {
                    foundGen2 = true;
                    assertEquals(numDocs2, wfs.numRows());
                }
                assertEquals(store.shardPath().resolveIndex().toString(), wfs.directory());
                assertFalse(wfs.files().isEmpty());
            }
            assertTrue("Should find segment with generation 1", foundGen1);
            assertTrue("Should find segment with generation 2", foundGen2);
        } finally {
            writer1.close();
            writer2.close();
        }
    }

    /**
     * deleteFiles removes lucene-format files from the store directory.
     */
    /**
     * deleteFiles is a no-op for Lucene format — Lucene's internal IndexFileDeleter
     * handles segment file cleanup via IndexCommit.delete() + deleteUnusedFiles().
     */
    /**
     * deleteFiles is a no-op — Lucene's internal IndexFileDeleter handles segment file cleanup.
     */
    public void testDeleteFilesIsNoOpForLuceneFormat() throws IOException {
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(luceneDataFormat, committer, mapperService, store);
        org.apache.lucene.store.Directory directory = store.directory();

        directory.createOutput("_0.cfs", org.apache.lucene.store.IOContext.DEFAULT).close();
        directory.createOutput("_0.cfe", org.apache.lucene.store.IOContext.DEFAULT).close();

        engine.deleteFiles(java.util.Map.of("lucene", java.util.List.of("_0.cfs", "_0.cfe")));

        // Files still exist — Lucene manages their lifecycle
        assertTrue(java.util.Set.of(directory.listAll()).contains("_0.cfs"));
        assertTrue(java.util.Set.of(directory.listAll()).contains("_0.cfe"));
    }

    /**
     * deleteFiles ignores non-lucene format keys.
     */
    public void testDeleteFilesIgnoresNonLuceneFormat() throws IOException {
        LuceneDataFormat luceneDataFormat = new LuceneDataFormat();
        LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(luceneDataFormat, committer, mapperService, store);
        engine.deleteFiles(java.util.Map.of("parquet", java.util.List.of("_0.parquet")));
    }
}
