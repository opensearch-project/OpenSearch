/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
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
                return List.of();
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

    private void addDoc(String id) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));
        indexWriter.addDocument(doc);
        indexWriter.commit();
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

        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        DirectoryReader reader1 = rm.getReader(snap1);
        assertEquals(0, new IndexSearcher(reader1).count(new MatchAllDocsQuery()));

        addDoc("doc1");
        CatalogSnapshot snap2 = stubSnapshot(2);
        rm.afterRefresh(true, snap2);
        DirectoryReader reader2 = rm.getReader(snap2);
        assertEquals(1, new IndexSearcher(reader2).count(new MatchAllDocsQuery()));

        assertEquals(0, new IndexSearcher(reader1).count(new MatchAllDocsQuery()));

        addDoc("doc2");
        CatalogSnapshot snap3 = stubSnapshot(3);
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
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);
        rm.afterRefresh(true, snap);

        DirectoryReader reader = rm.getReader(snap);
        assertTrue(reader.getRefCount() > 0);

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
        Store store = new Store(shardId, idxSettings, new NIOFSDirectory(dataPath), new DummyShardLock(shardId));
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        CommitterConfig cs = new CommitterConfig(idxSettings, null, store, CatalogSnapshotDeletionPolicy.KEEP_LATEST_ONLY);
        LuceneCommitter committer = new LuceneCommitter(cs);

        try {
            LuceneIndexingExecutionEngine engine = new LuceneIndexingExecutionEngine(committer, store);
            ReaderManagerConfig settings = new ReaderManagerConfig(Optional.of(engine), dataFormat, shardPath);

            EngineReaderManager<?> rm = LuceneSearchBackEnd.createReaderManager(settings);
            assertNotNull(rm);
        } finally {
            committer.close();
            store.close();
        }
    }

    public void testCreateReaderManagerWithEmptyProviderThrows() {
        ReaderManagerConfig settings = new ReaderManagerConfig(Optional.empty(), dataFormat, null);

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> LuceneSearchBackEnd.createReaderManager(settings));
        assertTrue(ex.getMessage().contains("IndexStoreProvider is required"));
    }
}
