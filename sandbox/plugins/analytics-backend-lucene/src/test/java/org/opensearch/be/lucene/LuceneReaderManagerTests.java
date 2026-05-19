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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.index.LuceneCommitter;
import org.opensearch.be.lucene.index.LuceneIndexingExecutionEngine;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link LuceneReaderManager} lifecycle with CatalogSnapshot interactions.
 */
public class LuceneReaderManagerTests extends OpenSearchTestCase {

    private IndexWriter indexWriter;
    private Directory directory;
    private DataFormat dataFormat;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path dir = createTempDir();
        directory = new MMapDirectory(dir);
        indexWriter = new IndexWriter(directory, new IndexWriterConfig());
        dataFormat = new DataFormat() {
            @Override
            public String name() {
                return "lucene";
            }

            @Override
            public long priority() {
                return 0;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        indexWriter.close();
        directory.close();
        super.tearDown();
    }

    private DirectoryReader openReader() throws IOException {
        return DirectoryReader.open(indexWriter);
    }

    private CatalogSnapshot stubSnapshot(long generation) {
        return stubSnapshot(generation, List.of());
    }

    /**
     * Builds a stub snapshot whose segment list contains the given writer generations.
     * Each segment includes a Lucene {@link WriterFileSet} whose files match what the
     * corresponding leaf will report from {@code SegmentCommitInfo.files()}.
     */
    private CatalogSnapshot stubSnapshot(long generation, List<Long> segmentGenerations) {
        // Build segments with file sets that match the current IndexWriter's segments.
        List<Segment> segs = buildSegmentsWithFiles(segmentGenerations);
        return buildCatalogSnapshot(generation, segs);
    }

    @SuppressForbidden(reason = "Need reflection to read SegmentInfos for building test file sets")
    private List<Segment> buildSegmentsWithFiles(List<Long> segmentGenerations) {
        if (segmentGenerations.isEmpty()) {
            return List.of();
        }
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            SegmentInfos segInfos = (SegmentInfos) segInfosField.get(indexWriter);
            List<Segment> result = new java.util.ArrayList<>();
            for (SegmentCommitInfo sci : segInfos) {
                String genAttr = sci.info.getAttribute(LuceneWriter.WRITER_GENERATION_ATTRIBUTE);
                if (genAttr == null) continue;
                long gen = Long.parseLong(genAttr);
                if (segmentGenerations.contains(gen)) {
                    WriterFileSet wfs = new WriterFileSet(
                        sci.info.dir.toString(),
                        gen,
                        new java.util.HashSet<>(sci.files()),
                        sci.info.maxDoc()
                    );
                    result.add(Segment.builder(gen).addSearchableFiles(LuceneDataFormat.LUCENE_FORMAT_NAME, wfs).build());
                }
            }
            return result;
        } catch (ReflectiveOperationException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CatalogSnapshot buildCatalogSnapshot(long generation, List<Segment> segs) {
        return new CatalogSnapshot("test", generation, 1) {
            @Override
            protected void closeInternal() {}

            @Override
            public Map<String, String> getUserData() {
                return Map.of();
            }

            @Override
            public long getId() {
                return generation;
            }

            @Override
            public List<Segment> getSegments() {
                return segs;
            }

            @Override
            public Collection<WriterFileSet> getSearchableFiles(String df) {
                return List.of();
            }

            @Override
            public Set<String> getDataFormats() {
                return Set.of();
            }

            @Override
            public long getLastWriterGeneration() {
                return 0;
            }

            @Override
            public String serializeToString() {
                return "";
            }

            @Override
            public CatalogSnapshot cloneNoAcquire() {
                return this;
            }

            @Override
            public void setUserData(Map<String, String> userData, boolean commitData) {}

            @Override
            public CatalogSnapshot clone() {
                return this;
            }

            @Override
            public int getFormatVersionForFile(String file) {
                return 0;
            }

            @Override
            public byte[] serialize() throws IOException {
                return new byte[0];
            }

            @Override
            public Collection<String> getFiles(boolean includeSegmentsFile) {
                return List.of();
            }
        };
    }

    private void addDoc(String id, long generation) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));
        indexWriter.addDocument(doc);
        indexWriter.commit();
        stampLatestSegmentGeneration(generation);
    }

    /**
     * Stamps the most recently written segment with the {@code writer_generation} attribute
     * that {@link LuceneReaderManager#afterRefresh}'s assertion expects. In production this
     * is done by {@code LuceneWriterCodec}; tests that write directly through a plain
     * {@link IndexWriter} must stamp it themselves.
     */
    @SuppressForbidden(reason = "Need reflection to stamp writer_generation on segments for testing")
    private void stampLatestSegmentGeneration(long generation) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            SegmentInfos segInfos = (SegmentInfos) segInfosField.get(indexWriter);
            if (segInfos.size() == 0) {
                return;
            }
            SegmentCommitInfo last = segInfos.asList().get(segInfos.size() - 1);
            if (last.info.getAttribute(LuceneWriter.WRITER_GENERATION_ATTRIBUTE) == null) {
                last.info.putAttribute(LuceneWriter.WRITER_GENERATION_ATTRIBUTE, String.valueOf(generation));
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to stamp writer_generation via reflection", e);
        }
    }

    public void testAfterRefreshCreatesReader() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
        rm.afterRefresh(true, snap);
        assertNotNull(rm.getReader(snap));
    }

    public void testAfterRefreshNoOpWhenDidRefreshFalse() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        rm.afterRefresh(false, snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
    }

    public void testMultipleRefreshesWithIndexing() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());

        // Empty initial reader — no segments yet.
        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        LuceneReader lr1 = rm.getReader(snap1);
        assertEquals(0, new IndexSearcher(lr1.directoryReader()).count(new MatchAllDocsQuery()));
        assertTrue(lr1.generationToSegmentName().isEmpty());

        // Add doc1 in generation 10, refresh.
        addDoc("doc1", 10L);
        CatalogSnapshot snap2 = stubSnapshot(2, List.of(10L));
        rm.afterRefresh(true, snap2);
        LuceneReader lr2 = rm.getReader(snap2);
        assertEquals(1, new IndexSearcher(lr2.directoryReader()).count(new MatchAllDocsQuery()));
        assertNotNull(lr2.generationToSegmentName().get(10L));

        assertEquals(0, new IndexSearcher(lr1.directoryReader()).count(new MatchAllDocsQuery()));

        // Add doc2 in generation 20.
        addDoc("doc2", 20L);
        CatalogSnapshot snap3 = stubSnapshot(3, List.of(10L, 20L));
        rm.afterRefresh(true, snap3);
        LuceneReader lr3 = rm.getReader(snap3);
        assertEquals(2, new IndexSearcher(lr3.directoryReader()).count(new MatchAllDocsQuery()));
        assertEquals(2, lr3.generationToSegmentName().size());

        assertNotSame(lr1, lr2);
        assertNotSame(lr2, lr3);

        rm.onDeleted(snap1);
        rm.onDeleted(snap2);
        rm.onDeleted(snap3);
    }

    public void testOnDeletedClosesReader() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);
        rm.afterRefresh(true, snap);

        LuceneReader lr = rm.getReader(snap);
        assertTrue(lr.directoryReader().getRefCount() > 0);

        rm.onDeleted(snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
    }

    public void testOnDeletedUnknownSnapshotIsNoOp() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        rm.onDeleted(stubSnapshot(99));
    }

    public void testGetReaderThrowsForUnknownSnapshot() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        expectThrows(IllegalStateException.class, () -> rm.getReader(stubSnapshot(42)));
    }

    public void testDuplicateAfterRefreshIsIdempotent() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        rm.afterRefresh(true, snap);
        LuceneReader first = rm.getReader(snap);

        rm.afterRefresh(true, snap);
        assertSame(first, rm.getReader(snap));

        rm.onDeleted(snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
    }

    // --- LuceneSearchBackEnd.createReaderManager tests ---

    public void testCreateReaderManagerWithLuceneIndexingEngine() throws IOException {
        Path dir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = dir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        java.nio.file.Files.createDirectories(dataPath);
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        Store store = new Store(shardId, idxSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId), (x) -> {}, shardPath);
        store.createEmpty(org.apache.lucene.util.Version.LATEST);
        Path translogPath = dataPath.resolve("translog");
        java.nio.file.Files.createDirectories(translogPath);
        EngineConfig engineConfig = new EngineConfig.Builder().indexSettings(idxSettings)
            .store(store)
            .codecService(new CodecService(null, idxSettings, LogManager.getLogger(getClass()), java.util.List.of()))
            .translogConfig(
                new org.opensearch.index.translog.TranslogConfig(
                    shardId,
                    translogPath,
                    idxSettings,
                    org.opensearch.common.util.BigArrays.NON_RECYCLING_INSTANCE,
                    "",
                    false
                )
            )
            .retentionLeasesSupplier(() -> new RetentionLeases(0, 0, java.util.Collections.emptyList()))
            .build();
        CommitterConfig cs = new CommitterConfig(engineConfig, () -> {});
        LuceneCommitter committer = new LuceneCommitter(cs);

        try {
            LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(
                new LuceneDataFormat(),
                committer,
                mock(MapperService.class),
                store
            );
            ReaderManagerConfig settings = new ReaderManagerConfig(
                Optional.of(engine),
                dataFormat,
                mock(DataFormatRegistry.class),
                shardPath,
                Map.of()
            );

            EngineReaderManager<?> rm = LuceneSearchBackEnd.createReaderManager(settings);
            assertNotNull(rm);
        } finally {
            committer.close();
            store.close();
        }
    }

    public void testCreateReaderManagerWithEmptyProviderThrows() {
        ReaderManagerConfig settings = new ReaderManagerConfig(
            Optional.empty(),
            dataFormat,
            mock(DataFormatRegistry.class),
            null,
            Map.of()
        );

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> LuceneSearchBackEnd.createReaderManager(settings));
        assertTrue(ex.getMessage().contains("IndexStoreProvider is required"));
    }
}
