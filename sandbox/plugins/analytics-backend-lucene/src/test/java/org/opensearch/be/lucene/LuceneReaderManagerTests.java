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
     * This is required by {@link LuceneReaderManager#afterRefresh}'s assertion, which
     * compares the snapshot's segment generations against the writer-generation attribute
     * on each leaf in the refreshed {@link DirectoryReader}.
     */
    private CatalogSnapshot stubSnapshot(long generation, List<Long> segmentGenerations) {
        List<Segment> segs = segmentGenerations.stream().map(g -> Segment.builder(g).build()).toList();
        return new CatalogSnapshot("test", generation, generation) {
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
            public void setUserData(Map<String, String> userData, boolean commitData) {}

            @Override
            public CatalogSnapshot clone() {
                return this;
            }

            @Override
            public long getFormatVersionForFile(String file) {
                return 0L;
            }

            @Override
            public long getMinSegmentFormatVersion() {
                return 0L;
            }

            @Override
            public long getCommitDataFormatVersion() {
                return 0L;
            }

            @Override
            public long getNumDocs() {
                return 0L;
            }

            @Override
            public String getLastCommitFileName() {
                return null;
            }

            public java.util.Set<String> getSegmentNames() {
                return java.util.Set.of();
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
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            openReader(),
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> DirectoryReader.openIfChanged(dr)
        );
        CatalogSnapshot snap = stubSnapshot(1);

        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
        rm.afterRefresh(true, snap);
        assertNotNull(rm.getReader(snap));
    }

    public void testAfterRefreshNoOpWhenDidRefreshFalse() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            openReader(),
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> DirectoryReader.openIfChanged(dr)
        );
        CatalogSnapshot snap = stubSnapshot(1);

        rm.afterRefresh(false, snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
    }

    public void testMultipleRefreshesWithIndexing() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            openReader(),
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> DirectoryReader.openIfChanged(dr)
        );

        // Empty initial reader — no segments yet.
        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        DirectoryReader reader1 = rm.getReader(snap1);
        assertEquals(0, new IndexSearcher(reader1).count(new MatchAllDocsQuery()));

        // Add doc1 in generation 10, refresh. Reader now has one leaf stamped with gen=10.
        addDoc("doc1", 10L);
        CatalogSnapshot snap2 = stubSnapshot(2, List.of(10L));
        rm.afterRefresh(true, snap2);
        DirectoryReader reader2 = rm.getReader(snap2);
        assertEquals(1, new IndexSearcher(reader2).count(new MatchAllDocsQuery()));

        assertEquals(0, new IndexSearcher(reader1).count(new MatchAllDocsQuery()));

        // Add doc2 in generation 20. Reader now has two leaves stamped with gens {10, 20}.
        addDoc("doc2", 20L);
        CatalogSnapshot snap3 = stubSnapshot(3, List.of(10L, 20L));
        rm.afterRefresh(true, snap3);
        DirectoryReader reader3 = rm.getReader(snap3);
        assertEquals(2, new IndexSearcher(reader3).count(new MatchAllDocsQuery()));

        assertNotSame(reader1, reader2);
        assertNotSame(reader2, reader3);

        rm.onDeleted(snap1);
        rm.onDeleted(snap2);
        rm.onDeleted(snap3);
    }

    public void testOnDeletedClosesReader() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            openReader(),
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> DirectoryReader.openIfChanged(dr)
        );
        CatalogSnapshot snap = stubSnapshot(1);
        rm.afterRefresh(true, snap);

        DirectoryReader reader = rm.getReader(snap);
        assertTrue(reader.getRefCount() > 0);

        rm.onDeleted(snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
    }

    public void testOnDeletedUnknownSnapshotIsNoOp() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            openReader(),
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> DirectoryReader.openIfChanged(dr)
        );
        rm.onDeleted(stubSnapshot(99));
    }

    public void testGetReaderThrowsForUnknownSnapshot() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            openReader(),
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> DirectoryReader.openIfChanged(dr)
        );
        expectThrows(IllegalStateException.class, () -> rm.getReader(stubSnapshot(42)));
    }

    public void testDuplicateAfterRefreshIsIdempotent() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            openReader(),
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> DirectoryReader.openIfChanged(dr)
        );
        CatalogSnapshot snap = stubSnapshot(1);

        rm.afterRefresh(true, snap);
        DirectoryReader first = rm.getReader(snap);

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
        Path translogPath = dataPath.resolve("translog");
        java.nio.file.Files.createDirectories(translogPath);
        String translogUUID = org.opensearch.index.translog.Translog.createEmptyTranslog(
            translogPath,
            org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            1L
        );
        store.createEmpty(org.apache.lucene.util.Version.LATEST, translogUUID);
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

    // --- Tests for null-refresher (incRef) path and ref-count correctness ---

    public void testAfterRefreshWithNullRefresherIncRefsCurrentReader() throws IOException {
        DirectoryReader initialReader = openReader();
        long initialRefCount = initialReader.getRefCount();

        // Refresher always returns null — simulates no new segments
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            initialReader,
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> null
        );

        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);

        // Reader should be the same instance with incRef'd count
        assertSame(initialReader, rm.getReader(snap1));
        assertEquals(initialRefCount + 1, initialReader.getRefCount());

        rm.onDeleted(snap1);
    }

    public void testMultipleSnapshotsShareReaderWhenRefresherReturnsNull() throws IOException {
        DirectoryReader initialReader = openReader();
        long initialRefCount = initialReader.getRefCount();

        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            initialReader,
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> null
        );

        CatalogSnapshot snap1 = stubSnapshot(1);
        CatalogSnapshot snap2 = stubSnapshot(2);
        CatalogSnapshot snap3 = stubSnapshot(3);

        rm.afterRefresh(true, snap1);
        rm.afterRefresh(true, snap2);
        rm.afterRefresh(true, snap3);

        // All three snapshots should share the same reader
        assertSame(initialReader, rm.getReader(snap1));
        assertSame(initialReader, rm.getReader(snap2));
        assertSame(initialReader, rm.getReader(snap3));

        // RefCount should be initial + 3 (one incRef per afterRefresh)
        assertEquals(initialRefCount + 3, initialReader.getRefCount());

        // Deleting each snapshot should decRef once
        rm.onDeleted(snap1);
        assertEquals(initialRefCount + 2, initialReader.getRefCount());

        rm.onDeleted(snap2);
        assertEquals(initialRefCount + 1, initialReader.getRefCount());

        rm.onDeleted(snap3);
        assertEquals(initialRefCount, initialReader.getRefCount());
    }

    public void testCloseDecRefsAllAccumulatedReaders() throws IOException {
        DirectoryReader initialReader = openReader();
        long initialRefCount = initialReader.getRefCount();

        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            initialReader,
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> null
        );

        // Accumulate 3 snapshots sharing the same reader
        rm.afterRefresh(true, stubSnapshot(1));
        rm.afterRefresh(true, stubSnapshot(2));
        rm.afterRefresh(true, stubSnapshot(3));
        assertEquals(initialRefCount + 3, initialReader.getRefCount());

        // close() should decRef all 3
        rm.close();
        assertEquals(initialRefCount, initialReader.getRefCount());
    }

    public void testMixedRefreshSomeNullSomeNew() throws IOException {
        // Scenario: snap1 gets null from refresher (no change), snap2 gets a new reader (doc added),
        // snap3 gets null again (same reader as snap2). Verify ref-counts are correct throughout.
        DirectoryReader initialReader = openReader();

        // Use a controllable refresher
        DirectoryReader[] nextReader = { null };
        LuceneReaderManager rm = new LuceneReaderManager(
            dataFormat,
            initialReader,
            new java.util.concurrent.ConcurrentHashMap<>(),
            (dr, sis) -> nextReader[0]
        );

        // snap1: refresher returns null → incRef initialReader
        nextReader[0] = null;
        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        assertSame(initialReader, rm.getReader(snap1));
        long refAfterSnap1 = initialReader.getRefCount();

        // snap2: add a doc, open a new reader, refresher returns it
        addDoc("doc1", 10L);
        DirectoryReader newReader = openReader();
        nextReader[0] = newReader;
        CatalogSnapshot snap2 = stubSnapshot(2, List.of(10L));
        rm.afterRefresh(true, snap2);
        assertSame(newReader, rm.getReader(snap2));
        assertNotSame(initialReader, newReader);

        // snap3: refresher returns null → incRef newReader (currentReader is now newReader)
        nextReader[0] = null;
        CatalogSnapshot snap3 = stubSnapshot(3, List.of(10L));
        rm.afterRefresh(true, snap3);
        assertSame(newReader, rm.getReader(snap3));

        long newReaderRefCount = newReader.getRefCount();

        // Delete snap3 → decRef newReader
        rm.onDeleted(snap3);
        assertEquals(newReaderRefCount - 1, newReader.getRefCount());

        // Delete snap1 → decRef initialReader
        long initialRefBefore = initialReader.getRefCount();
        rm.onDeleted(snap1);
        assertEquals(initialRefBefore - 1, initialReader.getRefCount());

        // Delete snap2 → decRef newReader
        long newRefBefore = newReader.getRefCount();
        rm.onDeleted(snap2);
        assertEquals(newRefBefore - 1, newReader.getRefCount());
    }

}
