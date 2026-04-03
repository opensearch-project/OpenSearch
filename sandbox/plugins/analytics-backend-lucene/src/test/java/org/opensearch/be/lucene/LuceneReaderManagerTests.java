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
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link LuceneReaderManager} lifecycle with CatalogSnapshot interactions.
 */
public class LuceneReaderManagerTests extends OpenSearchTestCase {

    private IndexWriter indexWriter;
    private Directory directory;
    private ShardId shardId;
    private DataFormat dataFormat;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path dir = createTempDir();
        directory = new MMapDirectory(dir);
        indexWriter = new IndexWriter(directory, new IndexWriterConfig());
        shardId = new ShardId("test", "_na_", 0);
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

    private OpenSearchDirectoryReader openReader() throws IOException {
        return OpenSearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
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
            public void setUserData(Map<String, String> userData) {}

            @Override
            public CatalogSnapshot clone() {
                return this;
            }
        };
    }

    private void addDoc(String id) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));
        indexWriter.addDocument(doc);
        indexWriter.commit();
    }

    /**
     * afterRefresh with didRefresh=true creates a reader for the snapshot.
     */
    public void testAfterRefreshCreatesReader() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
        rm.afterRefresh(true, snap);
        assertNotNull(rm.getReader(snap));
    }

    /**
     * afterRefresh with didRefresh=false does not create a reader.
     */
    public void testAfterRefreshNoOpWhenDidRefreshFalse() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        rm.afterRefresh(false, snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
    }

    /**
     * Multiple refreshes after indexing produce readers that see the correct doc counts.
     */
    public void testMultipleRefreshesWithIndexing() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());

        // Snapshot 1: 0 docs
        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        OpenSearchDirectoryReader reader1 = rm.getReader(snap1);
        assertNotNull(reader1);
        assertEquals(0, new IndexSearcher(reader1).count(new MatchAllDocsQuery()));

        // Index a doc and refresh
        addDoc("doc1");
        CatalogSnapshot snap2 = stubSnapshot(2);
        rm.afterRefresh(true, snap2);
        OpenSearchDirectoryReader reader2 = rm.getReader(snap2);
        assertNotNull(reader2);
        assertEquals(1, new IndexSearcher(reader2).count(new MatchAllDocsQuery()));

        // Snapshot 1 reader still sees 0 docs (point-in-time)
        assertEquals(0, new IndexSearcher(reader1).count(new MatchAllDocsQuery()));

        // Index another doc and refresh
        addDoc("doc2");
        CatalogSnapshot snap3 = stubSnapshot(3);
        rm.afterRefresh(true, snap3);
        OpenSearchDirectoryReader reader3 = rm.getReader(snap3);
        assertEquals(2, new IndexSearcher(reader3).count(new MatchAllDocsQuery()));

        // Each snapshot has its own reader
        assertNotSame(reader1, reader2);
        assertNotSame(reader2, reader3);

        // Cleanup
        rm.onDeleted(snap1);
        rm.onDeleted(snap2);
        rm.onDeleted(snap3);
    }

    /**
     * onDeleted removes the reader and closes it.
     */
    public void testOnDeletedClosesReader() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);
        rm.afterRefresh(true, snap);

        OpenSearchDirectoryReader reader = rm.getReader(snap);
        assertNotNull(reader);
        // Reader should have positive ref count
        assertTrue(reader.getRefCount() > 0);

        rm.onDeleted(snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
    }

    /**
     * onDeleted with unknown snapshot is a no-op.
     */
    public void testOnDeletedUnknownSnapshotIsNoOp() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot unknown = stubSnapshot(99);
        rm.onDeleted(unknown); // should not throw
    }

    /**
     * getReader throws IllegalStateException for a snapshot that was never refreshed.
     */
    public void testGetReaderThrowsForUnknownSnapshot() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot unknown = stubSnapshot(42);
        expectThrows(IllegalStateException.class, () -> rm.getReader(unknown));
    }

    /**
     * Duplicate afterRefresh for the same snapshot is idempotent.
     */
    public void testDuplicateAfterRefreshIsIdempotent() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);

        rm.afterRefresh(true, snap);
        OpenSearchDirectoryReader first = rm.getReader(snap);

        rm.afterRefresh(true, snap);
        assertSame(first, rm.getReader(snap));

        rm.onDeleted(snap);
    }

    /**
     * Multiple getReader calls on the same snapshot return the same reader with stable ref count.
     * The reader ref count stays at 1 (the reader manager's own reference).
     * onDeleted closes the reader, dropping the ref count to 0.
     */
    public void testMultipleAcquiresRefCountStaysOne() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());
        CatalogSnapshot snap = stubSnapshot(1);
        rm.afterRefresh(true, snap);

        OpenSearchDirectoryReader reader = rm.getReader(snap);
        int initialRefCount = reader.getRefCount();
        assertEquals("Reader ref count should be 1 after afterRefresh", 1, initialRefCount);

        // Multiple getReader calls should return the same instance without incrementing ref count
        for (int i = 0; i < 5; i++) {
            OpenSearchDirectoryReader same = rm.getReader(snap);
            assertSame(reader, same);
            assertEquals("Ref count must not change on getReader", 1, reader.getRefCount());
        }

        // onDeleted closes the reader
        rm.onDeleted(snap);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap));
        assertEquals("Ref count should be 0 after onDeleted", 0, reader.getRefCount());
    }

    /**
     * Two snapshots sharing the same underlying reader (no index changes between refreshes).
     * Deleting one closes the shared reader. The other snapshot still has a map entry
     * but the reader is closed. In production, CatalogSnapshotManager ensures ordered deletion.
     */
    public void testTwoSnapshotsSameReaderDeleteOneClosesSharedReader() throws IOException {
        LuceneReaderManager rm = new LuceneReaderManager(dataFormat, openReader());

        // No indexing between refreshes — both snapshots get the same reader
        CatalogSnapshot snap1 = stubSnapshot(1);
        rm.afterRefresh(true, snap1);
        OpenSearchDirectoryReader reader1 = rm.getReader(snap1);

        CatalogSnapshot snap2 = stubSnapshot(2);
        rm.afterRefresh(true, snap2);
        OpenSearchDirectoryReader reader2 = rm.getReader(snap2);

        // Same reader since no changes happened
        assertSame(reader1, reader2);

        // Delete snap1 — closes the shared reader
        rm.onDeleted(snap1);
        expectThrows(IllegalStateException.class, () -> rm.getReader(snap1));

        // snap2 still has a map entry but the underlying reader is closed
        // This is expected — in production, snapshots are deleted in order
        OpenSearchDirectoryReader snap2Reader = rm.getReader(snap2);
        assertEquals("Shared reader should be closed after snap1 deletion", 0, snap2Reader.getRefCount());

        // Clean up
        rm.onDeleted(snap2);
    }
}
