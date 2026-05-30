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
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
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
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.index.OpenSearchLeafReader;
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
import org.opensearch.index.shard.ShardUtils;
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
 * All tests use a non-null ShardId to exercise the OpenSearchDirectoryReader wrapping
 * path that production code requires for IndicesQueryCache compatibility.
 */
public class LuceneReaderManagerTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId("test_index", "_na_", 0);

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

    private LuceneReaderManager createManager(
        DirectoryReader reader,
        org.opensearch.common.CheckedBiFunction<DirectoryReader, SegmentInfos, DirectoryReader, IOException> refresher
    ) throws IOException {
        return new LuceneReaderManager(dataFormat, reader, new java.util.concurrent.ConcurrentHashMap<>(), refresher, SHARD_ID);
    }

    private LuceneReaderManager createManager(DirectoryReader reader) throws IOException {
        return createManager(reader, (dr, sis) -> DirectoryReader.openIfChanged(dr));
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
                        sci.info.maxDoc(),
                        0L
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

    // --- Core lifecycle tests ---

    public void testAfterRefreshCreatesReader() throws IOException {
        LuceneReaderManager rm = createManager(openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
        rm.afterRefresh(true, snap);
        assertNotNull(rm.getReader(snap));
        rm.close();
    }

    public void testAfterRefreshNoOpWhenDidRefreshFalse() throws IOException {
        LuceneReaderManager rm = createManager(openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        rm.afterRefresh(false, snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
        rm.close();
    }

    public void testGetReaderThrowsForUnknownSnapshot() throws IOException {
        LuceneReaderManager rm = createManager(openReader());
        expectThrows(IllegalStateException.class, () -> rm.getReader(stubSnapshot(42)));
        rm.close();
    }

    public void testDuplicateAfterRefreshIsIdempotent() throws IOException {
        LuceneReaderManager rm = createManager(openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        rm.afterRefresh(true, snap);
        LuceneReader first = rm.getReader(snap);

        rm.afterRefresh(true, snap);
        assertSame(first, rm.getReader(snap));

        rm.onDeleted(snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
    }

    public void testOnDeletedUnknownSnapshotIsNoOp() throws IOException {
        LuceneReaderManager rm = createManager(openReader());
        rm.onDeleted(stubSnapshot(99));
        rm.close();
    }

    // --- OpenSearchDirectoryReader wrapping tests ---

    public void testReaderIsWrappedInOpenSearchDirectoryReader() throws IOException {
        LuceneReaderManager rm = createManager(openReader());
        CatalogSnapshot snap = stubSnapshot(1);
        rm.afterRefresh(true, snap);

        DirectoryReader reader = rm.getReader(snap).directoryReader();
        assertTrue(
            "Reader must be an OpenSearchDirectoryReader for IndicesQueryCache compatibility",
            reader instanceof OpenSearchDirectoryReader
        );
        assertEquals(SHARD_ID, ((OpenSearchDirectoryReader) reader).shardId());
        rm.close();
    }

    public void testWrappedReaderLeavesExposeShardId() throws IOException {
        addDoc("doc1", 10L);
        LuceneReaderManager rm = createManager(openReader());
        CatalogSnapshot snap = stubSnapshot(1, List.of(10L));
        rm.afterRefresh(true, snap);

        DirectoryReader reader = rm.getReader(snap).directoryReader();
        List<LeafReaderContext> leaves = reader.leaves();
        assertFalse("Should have at least one leaf", leaves.isEmpty());

        for (LeafReaderContext lrc : leaves) {
            assertTrue("Leaf reader must be an OpenSearchLeafReader", lrc.reader() instanceof OpenSearchLeafReader);
            ShardId extracted = ShardUtils.extractShardId(lrc.reader());
            assertNotNull("ShardUtils.extractShardId must succeed (required by IndicesQueryCache)", extracted);
            assertEquals(SHARD_ID, extracted);
        }
        rm.close();
    }

    public void testWrappedReaderLeavesCanBeUnwrappedToSegmentReader() throws IOException {
        addDoc("doc1", 10L);
        LuceneReaderManager rm = createManager(openReader());
        CatalogSnapshot snap = stubSnapshot(1, List.of(10L));
        rm.afterRefresh(true, snap);

        DirectoryReader reader = rm.getReader(snap).directoryReader();
        for (LeafReaderContext lrc : reader.leaves()) {
            // LuceneFilterDelegationHandle needs to unwrap to SegmentReader for segment name lookup
            org.apache.lucene.index.LeafReader current = lrc.reader();
            while (current instanceof FilterLeafReader flr) {
                current = flr.getDelegate();
            }
            assertTrue("Unwrapped leaf must be a SegmentReader", current instanceof org.apache.lucene.index.SegmentReader);
        }
        rm.close();
    }

    // --- Ref counting with wrapped readers ---

    public void testWrappedReaderRefCountOnSingleSnapshot() throws IOException {
        DirectoryReader rawReader = openReader();
        LuceneReaderManager rm = createManager(rawReader, (dr, sis) -> null);

        CatalogSnapshot snap = stubSnapshot(1);
        rm.afterRefresh(true, snap);

        DirectoryReader wrappedReader = rm.getReader(snap).directoryReader();
        assertTrue(wrappedReader instanceof OpenSearchDirectoryReader);
        // Wrapper refCount: 1 (creation/initial) + 1 (snapshot incRef) = 2
        assertEquals(2, wrappedReader.getRefCount());

        rm.onDeleted(snap);
        // After onDeleted: -1 (map decRef) -1 (releaseInitialReader) = 0
        assertEquals(0, wrappedReader.getRefCount());
    }

    public void testWrappedReaderRefCountOnMultipleSnapshots() throws IOException {
        DirectoryReader rawReader = openReader();
        LuceneReaderManager rm = createManager(rawReader, (dr, sis) -> null);

        CatalogSnapshot snap1 = stubSnapshot(1);
        CatalogSnapshot snap2 = stubSnapshot(2);
        CatalogSnapshot snap3 = stubSnapshot(3);

        rm.afterRefresh(true, snap1);
        rm.afterRefresh(true, snap2);
        rm.afterRefresh(true, snap3);

        // All snapshots share the same wrapped reader (initial wrapper)
        DirectoryReader wrapped = rm.getReader(snap1).directoryReader();
        assertSame(wrapped, rm.getReader(snap2).directoryReader());
        assertSame(wrapped, rm.getReader(snap3).directoryReader());

        // RefCount: 1 (creation) + 3 (one incRef per snapshot) = 4
        assertEquals(4, wrapped.getRefCount());

        // First onDeleted: -1 (map) -1 (releaseInitialReader) = 2
        rm.onDeleted(snap1);
        assertEquals(2, wrapped.getRefCount());

        // Second onDeleted: -1 (map) = 1
        rm.onDeleted(snap2);
        assertEquals(1, wrapped.getRefCount());

        // Third onDeleted: -1 (map) = 0 → closed
        rm.onDeleted(snap3);
        assertEquals(0, wrapped.getRefCount());
    }

    public void testCloseReleasesAllWrappedReaderRefs() throws IOException {
        DirectoryReader rawReader = openReader();
        LuceneReaderManager rm = createManager(rawReader, (dr, sis) -> null);

        rm.afterRefresh(true, stubSnapshot(1));
        rm.afterRefresh(true, stubSnapshot(2));
        rm.afterRefresh(true, stubSnapshot(3));

        DirectoryReader wrapped = rm.getReader(stubSnapshot(1)).directoryReader();
        // RefCount: 1 (creation) + 3 (snapshots) = 4
        assertEquals(4, wrapped.getRefCount());

        // close() decRefs 3 map entries + 1 releaseInitialReader = 0
        rm.close();
        assertEquals(0, wrapped.getRefCount());
    }

    // --- Refresh produces new reader: new wrapper lifecycle ---

    public void testRefreshCreatesNewWrapper() throws IOException {
        DirectoryReader rawReader = openReader();
        DirectoryReader[] nextReader = { null };
        LuceneReaderManager rm = createManager(rawReader, (dr, sis) -> nextReader[0]);

        // snap1: no refresh (null) → uses initial wrapper
        nextReader[0] = null;
        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        DirectoryReader initialWrapper = rm.getReader(snap1).directoryReader();
        assertTrue(initialWrapper instanceof OpenSearchDirectoryReader);

        // snap2: refresh with new reader → new wrapper created
        addDoc("doc1", 10L);
        DirectoryReader refreshedRaw = openReader();
        nextReader[0] = refreshedRaw;
        CatalogSnapshot snap2 = stubSnapshot(2, List.of(10L));
        rm.afterRefresh(true, snap2);
        DirectoryReader newWrapper = rm.getReader(snap2).directoryReader();
        assertTrue(newWrapper instanceof OpenSearchDirectoryReader);
        assertNotSame("Refresh must produce a new wrapper", initialWrapper, newWrapper);

        // Initial wrapper: creation(1) + snap1 incRef(1) = 2
        assertEquals(2, initialWrapper.getRefCount());
        // New wrapper: creation ref only (serves as snap2's ref) = 1
        assertEquals(1, newWrapper.getRefCount());

        // Delete snap1 → releases initial wrapper (map -1, releaseInitial -1 = 0)
        rm.onDeleted(snap1);
        assertEquals(0, initialWrapper.getRefCount());

        // Delete snap2 → releases new wrapper (map -1 = 0)
        rm.onDeleted(snap2);
        assertEquals(0, newWrapper.getRefCount());
    }

    public void testMultipleRefreshesEachCreateNewWrapper() throws IOException {
        DirectoryReader rawReader = openReader();
        DirectoryReader[] nextReader = { null };
        LuceneReaderManager rm = createManager(rawReader, (dr, sis) -> nextReader[0]);

        // snap1: initial wrapper
        nextReader[0] = null;
        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        DirectoryReader wrapper1 = rm.getReader(snap1).directoryReader();

        // snap2: first refresh
        addDoc("doc1", 10L);
        nextReader[0] = openReader();
        CatalogSnapshot snap2 = stubSnapshot(2, List.of(10L));
        rm.afterRefresh(true, snap2);
        DirectoryReader wrapper2 = rm.getReader(snap2).directoryReader();

        // snap3: second refresh
        addDoc("doc2", 20L);
        nextReader[0] = openReader();
        CatalogSnapshot snap3 = stubSnapshot(3, List.of(10L, 20L));
        rm.afterRefresh(true, snap3);
        DirectoryReader wrapper3 = rm.getReader(snap3).directoryReader();

        // All different wrappers
        assertNotSame(wrapper1, wrapper2);
        assertNotSame(wrapper2, wrapper3);
        assertNotSame(wrapper1, wrapper3);

        // Each new wrapper has refCount=1 (creation=snapshot ref); initial has 1+1=2
        assertEquals(2, wrapper1.getRefCount());
        assertEquals(1, wrapper2.getRefCount());
        assertEquals(1, wrapper3.getRefCount());

        // close() cleans up all
        rm.close();
        assertEquals(0, wrapper1.getRefCount());
        assertEquals(0, wrapper2.getRefCount());
        assertEquals(0, wrapper3.getRefCount());
    }

    public void testRefreshThenNoRefreshSharesNewWrapper() throws IOException {
        DirectoryReader rawReader = openReader();
        DirectoryReader[] nextReader = { null };
        LuceneReaderManager rm = createManager(rawReader, (dr, sis) -> nextReader[0]);

        // snap1: initial wrapper
        nextReader[0] = null;
        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        DirectoryReader initialWrapper = rm.getReader(snap1).directoryReader();

        // snap2: refresh → new wrapper (creation ref = snap2's ref)
        addDoc("doc1", 10L);
        nextReader[0] = openReader();
        CatalogSnapshot snap2 = stubSnapshot(2, List.of(10L));
        rm.afterRefresh(true, snap2);
        DirectoryReader newWrapper = rm.getReader(snap2).directoryReader();
        assertEquals(1, newWrapper.getRefCount());

        // snap3: no refresh → shares new wrapper (incRef)
        nextReader[0] = null;
        CatalogSnapshot snap3 = stubSnapshot(3, List.of(10L));
        rm.afterRefresh(true, snap3);
        DirectoryReader snap3Reader = rm.getReader(snap3).directoryReader();
        assertSame("snap3 must get the same wrapped reader", newWrapper, snap3Reader);
        // newWrapper: creation(1 for snap2) + incRef(1 for snap3) = 2
        assertEquals(2, newWrapper.getRefCount());

        // onDeleted(snap2): decRefs newWrapper (-1) and releases initialWrapper via releaseInitialReader (-1)
        rm.onDeleted(snap2);
        assertEquals(1, newWrapper.getRefCount());
        // initialWrapper had: creation(1) + snap1 incRef(1) - releaseInitial(1) = 1
        assertEquals(1, initialWrapper.getRefCount());

        // onDeleted(snap3): decRefs newWrapper (-1) → 0
        rm.onDeleted(snap3);
        assertEquals(0, newWrapper.getRefCount());

        // onDeleted(snap1): decRefs initialWrapper (-1) → 0
        rm.onDeleted(snap1);
        assertEquals(0, initialWrapper.getRefCount());
    }

    // --- Functional tests with indexing ---

    public void testMultipleRefreshesWithIndexing() throws IOException {
        LuceneReaderManager rm = createManager(openReader());

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

        // Old snapshot still sees 0 docs
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

    public void testOnDeletedClosesWrappedReader() throws IOException {
        LuceneReaderManager rm = createManager(openReader());
        CatalogSnapshot snap = stubSnapshot(1);
        rm.afterRefresh(true, snap);

        LuceneReader lr = rm.getReader(snap);
        DirectoryReader wrapped = lr.directoryReader();
        assertTrue(wrapped.getRefCount() > 0);

        rm.onDeleted(snap);
        assertEquals(0, wrapped.getRefCount());
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

}
